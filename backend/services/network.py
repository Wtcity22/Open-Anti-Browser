from __future__ import annotations

import base64
import datetime
import hashlib
import select
import shutil
import socket
import ssl
import struct
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import psutil
import pycountry
import pytz
from babel import Locale
from babel.core import get_global
from curl_cffi import requests


DEFAULT_HTTP_TIMEOUT = 8
GEO_PRIMARY_TIMEOUT = 4.5
PROXY_CONNECT_TIMEOUT = 6
PROXY_TEST_TARGET_HOST = "1.1.1.1"
PROXY_TEST_TARGET_PORT = 443
DEFAULT_GEO_PROFILE = {
    "ip": None,
    "language": "en-US",
    "timezone": "Etc/UTC",
    "locale": "UTC+00:00",
    "country_code": None,
    "latitude": None,
    "longitude": None,
    "precision": None,
    "source": None,
    "ip_scan_channel": None,
    "region": None,
    "city": None,
    "isp": None,
    "zipcode": None,
}


class GeoProfileResolveError(RuntimeError):
    pass


BROWSERSCAN_HEADERS = {
    "accept": "*/*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
    "origin": "https://www.browserscan.net",
    "referer": "https://www.browserscan.net/",
    "sec-ch-ua": '"Google Chrome";v="136", "Not.A/Brand";v="8", "Chromium";v="136"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
}


def create_http_session(proxy_url: str | None = None, timeout: float = DEFAULT_HTTP_TIMEOUT) -> requests.Session:
    proxies = None
    if proxy_url:
        proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }
    return requests.Session(
        impersonate="chrome136",
        proxies=proxies,
        verify=False,
        timeout=timeout,
    )


def _normalize_proxy_server(proxy_server: str | None) -> str | None:
    proxy_server = str(proxy_server or "").strip()
    if not proxy_server:
        return None
    if "://" not in proxy_server:
        proxy_server = f"http://{proxy_server}"
    return proxy_server


def normalize_proxy_config(proxy: Any) -> dict[str, Any] | None:
    if not proxy:
        return None

    if isinstance(proxy, dict):
        proxy_server = (
            proxy.get("server")
            or proxy.get("url")
            or proxy.get("proxy")
            or proxy.get("host")
        )
        username = proxy.get("username")
        password = proxy.get("password")
    else:
        proxy_server = proxy
        username = None
        password = None

    proxy_server = _normalize_proxy_server(proxy_server)
    if not proxy_server:
        return None

    parsed_url = urlparse(proxy_server)
    if not parsed_url.hostname or not parsed_url.port:
        raise ValueError(f"Invalid proxy server: {proxy}")

    scheme = parsed_url.scheme or "http"
    username = parsed_url.username or username
    password = parsed_url.password or password
    ip_port = f"{parsed_url.hostname}:{parsed_url.port}"
    browser_proxy = f"{scheme}://{ip_port}"
    request_scheme = "socks5h" if scheme == "socks5" else scheme

    if username is not None and str(username) != "":
        auth = f"{quote(str(username), safe='')}:{quote(str(password or ''), safe='')}@"
        request_proxy = f"{request_scheme}://{auth}{ip_port}"
    else:
        request_proxy = f"{request_scheme}://{ip_port}"
        username = None
        password = None

    return {
        "scheme": scheme,
        "server": browser_proxy,
        "request_proxy": request_proxy,
        "username": username,
        "password": password,
        "ip_port": ip_port,
        "host": parsed_url.hostname,
        "port": parsed_url.port,
    }


def proxy_to_profile_proxy(proxy: dict[str, Any]) -> dict[str, Any] | None:
    if not proxy:
        return None
    proxy_type = str(proxy.get("type") or "none").lower()
    if proxy_type == "none" or not proxy.get("host") or not proxy.get("port"):
        return None
    server = f"{proxy_type}://{proxy['host']}:{proxy['port']}"
    return normalize_proxy_config(
        {
            "server": server,
            "username": proxy.get("username") or None,
            "password": proxy.get("password") or None,
        }
    )


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return sock.getsockname()[1]


class LocalHttpProxyBridge:
    def __init__(self, upstream_proxy: dict[str, Any] | str):
        self.upstream_proxy = normalize_proxy_config(upstream_proxy)
        if not self.upstream_proxy:
            raise ValueError("upstream proxy is required")
        if self.upstream_proxy["scheme"] not in ("http", "https"):
            raise ValueError("proxy bridge only supports http/https upstream proxies")

        self.listen_host = "127.0.0.1"
        self.listen_port: int | None = None
        self._server_socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._running = threading.Event()

    @property
    def local_proxy(self) -> str:
        if self.listen_port is None:
            raise RuntimeError("proxy bridge is not started")
        return f"http://{self.listen_host}:{self.listen_port}"

    @property
    def upstream_auth_header(self) -> str:
        username = self.upstream_proxy["username"] or ""
        password = self.upstream_proxy["password"] or ""
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        return f"Basic {token}"

    def start(self) -> "LocalHttpProxyBridge":
        if self._running.is_set():
            return self

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.listen_host, 0))
        server_socket.listen(50)
        server_socket.settimeout(1)
        self.listen_port = server_socket.getsockname()[1]
        self._server_socket = server_socket
        self._running.set()
        self._thread = threading.Thread(target=self._serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._running.clear()
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass
            self._server_socket = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None
        self.listen_port = None

    def _serve_forever(self) -> None:
        while self._running.is_set():
            try:
                client_socket, _ = self._server_socket.accept()  # type: ignore[union-attr]
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True).start()

    def _handle_client(self, client_socket: socket.socket) -> None:
        upstream_socket: socket.socket | None = None
        try:
            raw_request = self._recv_headers(client_socket)
            if not raw_request:
                return

            header_bytes, remain = raw_request
            header_text = header_bytes.decode("iso-8859-1", errors="replace")
            lines = header_text.split("\r\n")
            request_line = lines[0]
            if not request_line:
                return

            method, _, _ = request_line.split(" ", 2)
            headers = [line for line in lines[1:] if line]
            content_length = self._get_content_length(headers)
            body = remain
            while len(body) < content_length:
                chunk = client_socket.recv(min(65536, content_length - len(body)))
                if not chunk:
                    break
                body += chunk

            upstream_host, upstream_port = self.upstream_proxy["ip_port"].split(":", 1)
            upstream_socket = socket.create_connection((upstream_host, int(upstream_port)), timeout=30)
            if self.upstream_proxy["scheme"] == "https":
                upstream_socket = ssl.create_default_context().wrap_socket(
                    upstream_socket,
                    server_hostname=upstream_host,
                )

            filtered_headers: list[str] = []
            for line in headers:
                lower_line = line.lower()
                if lower_line.startswith("proxy-authorization:"):
                    continue
                if lower_line.startswith("proxy-connection:"):
                    filtered_headers.append("Proxy-Connection: close")
                    continue
                if lower_line.startswith("connection:"):
                    filtered_headers.append("Connection: close")
                    continue
                filtered_headers.append(line)

            filtered_headers.append(f"Proxy-Authorization: {self.upstream_auth_header}")
            upstream_request = (
                request_line + "\r\n" + "\r\n".join(filtered_headers) + "\r\n\r\n"
            ).encode("iso-8859-1")
            upstream_socket.sendall(upstream_request)
            if body:
                upstream_socket.sendall(body)

            if method.upper() == "CONNECT":
                response_header = self._recv_raw_header_block(upstream_socket)
                if not response_header:
                    return
                client_socket.sendall(response_header)
                if b" 200 " not in response_header.split(b"\r\n", 1)[0]:
                    return
                self._bridge_bidirectional(client_socket, upstream_socket)
                return

            self._forward_until_close(upstream_socket, client_socket)
        finally:
            for sock in (client_socket, upstream_socket):
                if not sock:
                    continue
                try:
                    sock.close()
                except Exception:
                    pass

    @staticmethod
    def _get_content_length(headers: list[str]) -> int:
        for line in headers:
            if line.lower().startswith("content-length:"):
                try:
                    return int(line.split(":", 1)[1].strip())
                except Exception:
                    return 0
        return 0

    @staticmethod
    def _recv_headers(sock: socket.socket, max_size: int = 1024 * 1024):
        data = b""
        while b"\r\n\r\n" not in data and len(data) < max_size:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
        if b"\r\n\r\n" not in data:
            return None
        header_bytes, remain = data.split(b"\r\n\r\n", 1)
        return header_bytes + b"\r\n\r\n", remain

    @staticmethod
    def _recv_raw_header_block(sock: socket.socket, max_size: int = 1024 * 1024) -> bytes:
        data = b""
        while b"\r\n\r\n" not in data and len(data) < max_size:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
        return data

    @staticmethod
    def _forward_until_close(source_socket: socket.socket, target_socket: socket.socket) -> None:
        while True:
            data = source_socket.recv(65536)
            if not data:
                break
            target_socket.sendall(data)

    @staticmethod
    def _bridge_bidirectional(client_socket: socket.socket, upstream_socket: socket.socket, idle_timeout: int = 300) -> None:
        sockets = [client_socket, upstream_socket]
        while True:
            readable, _, exceptional = select.select(sockets, [], sockets, idle_timeout)
            if exceptional or not readable:
                break
            for sock in readable:
                other = upstream_socket if sock is client_socket else client_socket
                data = sock.recv(65536)
                if not data:
                    return
                other.sendall(data)


def get_country_language_timezone(country_code: str | None) -> tuple[str, str, str]:
    try:
        if not country_code:
            raise ValueError("empty country code")
        country = pycountry.countries.get(alpha_2=country_code.upper())
        language_code = None
        if country:
            Locale.parse("und_" + country.alpha_2)
            territory_data = get_global("territory_languages")
            if country.alpha_2 in territory_data:
                main_language_code = list(territory_data[country.alpha_2].keys())[0]
                language = pycountry.languages.get(alpha_2=main_language_code)
                if language:
                    language_code = f"{language.alpha_2}-{country.alpha_2}"
        if not language_code:
            language_code = f"en-{country.alpha_2}" if country else "en-US"

        timezones = pytz.country_timezones.get(country_code.upper(), ["Etc/UTC"])
        main_timezone = timezones[0]
        tz = pytz.timezone(main_timezone)
        offset = tz.utcoffset(datetime.datetime.now()).total_seconds() / 3600
        sign = "+" if offset >= 0 else "-"
        hours = int(abs(offset))
        minutes = int((abs(offset) - hours) * 60)
        locale = f"UTC{sign}{hours:02d}:{minutes:02d}"
        return language_code, locale, main_timezone
    except Exception:
        return "en-US", "UTC+00:00", "Etc/UTC"


def build_browserscan_sign(ts: int | None = None) -> dict[str, str]:
    now = str(ts or int(time.time()))
    source = f"{now[-6:]}browserscan"
    return {
        "_t": now,
        "_from": "browserscan",
        "_sign": hashlib.md5(source.encode("utf-8")).hexdigest(),
    }


def resolve_geo_profile(
    proxy: dict[str, Any] | None = None,
    auto_timezone: bool = True,
    strict: bool = False,
) -> dict[str, Any]:
    geo_profile = dict(DEFAULT_GEO_PROFILE)
    if not auto_timezone:
        return geo_profile

    request_proxy = proxy["request_proxy"] if proxy else None
    session = create_http_session(request_proxy)
    error_message: str | None = None
    try:
        try:
            response = session.get(
                "https://ip-scan.browserscan.net/sys/config/ip/get-visitor-ip",
                params=build_browserscan_sign(),
                headers=BROWSERSCAN_HEADERS,
                timeout=GEO_PRIMARY_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or payload.get("code") != 0:
                raise GeoProfileResolveError("browserscan 返回异常")

            data = payload.get("data") or {}
            if not isinstance(data, dict):
                raise GeoProfileResolveError("browserscan 返回数据格式异常")

            ip_data = data.get("ip_data") or {}
            if not isinstance(ip_data, dict):
                ip_data = {}

            _merge_geo_profile(geo_profile, data.get("ip"), ip_data)
            if geo_profile.get("ip"):
                return geo_profile
            raise GeoProfileResolveError("browserscan 没有返回可用的 IP 信息")
        except Exception as exc:
            error_message = str(exc) or "browserscan 请求失败"
    finally:
        try:
            session.close()
        except Exception:
            pass

    if strict:
        message = "IP 解析超时或失败，请检查当前网络或代理后重试。"
        if proxy:
            message = "代理 IP 解析超时或失败，请检查当前网络或代理后重试。"
        if error_message:
            raise GeoProfileResolveError(f"{message}（{error_message}）")
        raise GeoProfileResolveError(message)
    return geo_profile


def test_proxy_connectivity(proxy: dict[str, Any] | None) -> dict[str, Any]:
    if not proxy:
        return {
            "ok": True,
            "message": "未设置代理，当前为直连",
            "latency_ms": 0,
        }

    scheme = str(proxy.get("scheme") or "").lower()
    started_at = time.perf_counter()
    try:
        if scheme == "socks5":
            _test_socks5_proxy_connectivity(proxy)
        else:
            _test_http_proxy_connectivity(proxy)
    except Exception as exc:
        return {
            "ok": False,
            "message": f"代理握手失败：{exc}",
            "latency_ms": int((time.perf_counter() - started_at) * 1000),
        }

    return {
        "ok": True,
        "message": "代理握手成功",
        "latency_ms": int((time.perf_counter() - started_at) * 1000),
    }


def _merge_geo_profile(profile: dict[str, Any], ip_value: str | None, ip_data: dict[str, Any]) -> None:
    if ip_value:
        profile["ip"] = ip_value
    profile["source"] = "browserscan_visitor"

    country_code = ip_data.get("country") or ip_data.get("countryCode")
    timezone_name = ip_data.get("timezone")
    latitude = ip_data.get("latitude", ip_data.get("lat"))
    longitude = ip_data.get("longitude", ip_data.get("lon"))
    profile["ip_scan_channel"] = ip_data.get("ip_scan_channel") or "ip2location"
    profile["region"] = ip_data.get("region")
    profile["city"] = ip_data.get("city")
    profile["isp"] = ip_data.get("isp")
    profile["zipcode"] = ip_data.get("zipcode")

    if latitude is not None:
        profile["latitude"] = latitude
    if longitude is not None:
        profile["longitude"] = longitude
    if latitude is not None or longitude is not None:
        profile["precision"] = 1200

    if country_code:
        language, locale, auto_timezone = get_country_language_timezone(country_code)
        profile["country_code"] = country_code
        profile["language"] = language
        profile["locale"] = locale
        profile["timezone"] = timezone_name or auto_timezone
    elif timezone_name:
        profile["timezone"] = timezone_name


def _test_socks5_proxy_connectivity(proxy: dict[str, Any]) -> None:
    username = str(proxy.get("username") or "")
    password = str(proxy.get("password") or "")
    auth_methods = [0x00, 0x02] if username else [0x00]

    with socket.create_connection((proxy["host"], int(proxy["port"])), timeout=PROXY_CONNECT_TIMEOUT) as sock:
        sock.settimeout(PROXY_CONNECT_TIMEOUT)
        sock.sendall(bytes([0x05, len(auth_methods), *auth_methods]))
        greeting = _recv_exact(sock, 2)
        if greeting[0] != 0x05:
            raise RuntimeError("SOCKS5 服务响应异常")
        if greeting[1] == 0xFF:
            raise RuntimeError("SOCKS5 服务不接受当前认证方式")

        if greeting[1] == 0x02:
            user_bytes = username.encode("utf-8")
            password_bytes = password.encode("utf-8")
            if len(user_bytes) > 255 or len(password_bytes) > 255:
                raise RuntimeError("SOCKS5 账号或密码长度超限")
            sock.sendall(
                bytes([0x01, len(user_bytes)]) + user_bytes + bytes([len(password_bytes)]) + password_bytes
            )
            auth_result = _recv_exact(sock, 2)
            if auth_result[1] != 0x00:
                raise RuntimeError("SOCKS5 账号或密码错误")

        sock.sendall(
            bytes([0x05, 0x01, 0x00, 0x01])
            + socket.inet_aton(PROXY_TEST_TARGET_HOST)
            + struct.pack("!H", PROXY_TEST_TARGET_PORT)
        )
        response = _recv_exact(sock, 10)
        if response[1] != 0x00:
            raise RuntimeError(f"SOCKS5 连接失败，错误码 {response[1]}")


def _test_http_proxy_connectivity(proxy: dict[str, Any]) -> None:
    host = str(proxy.get("host") or "")
    port = int(proxy.get("port") or 0)
    if not host or not port:
        raise RuntimeError("代理地址不完整")

    username = str(proxy.get("username") or "")
    password = str(proxy.get("password") or "")

    raw_socket = socket.create_connection((host, port), timeout=PROXY_CONNECT_TIMEOUT)
    try:
        raw_socket.settimeout(PROXY_CONNECT_TIMEOUT)
        sock: socket.socket = raw_socket
        if str(proxy.get("scheme") or "").lower() == "https":
            context = ssl.create_default_context()
            sock = context.wrap_socket(raw_socket, server_hostname=host)

        headers = [
            f"CONNECT {PROXY_TEST_TARGET_HOST}:{PROXY_TEST_TARGET_PORT} HTTP/1.1",
            f"Host: {PROXY_TEST_TARGET_HOST}:{PROXY_TEST_TARGET_PORT}",
            "Proxy-Connection: keep-alive",
            "Connection: keep-alive",
        ]
        if username:
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            headers.append(f"Proxy-Authorization: Basic {token}")
        headers.append("")
        headers.append("")
        sock.sendall("\r\n".join(headers).encode("iso-8859-1"))
        response = _recv_until(sock, b"\r\n\r\n")
        first_line = response.split(b"\r\n", 1)[0].decode("iso-8859-1", errors="ignore")
        if " 200 " not in first_line:
            raise RuntimeError(first_line or "HTTP 代理握手失败")
    finally:
        try:
            raw_socket.close()
        except Exception:
            pass


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("代理连接被提前关闭")
        data += chunk
    return data


def _recv_until(sock: socket.socket, marker: bytes, max_size: int = 65536) -> bytes:
    data = b""
    while marker not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
        if len(data) >= max_size:
            break
    return data


def slugify(value: str, fallback: str = "profile") -> str:
    value = "".join(ch if ch.isalnum() else "-" for ch in str(value or "").strip().lower())
    value = "-".join(part for part in value.split("-") if part)
    return value or fallback


def kill_process_tree(pid: int) -> None:
    try:
        parent = psutil.Process(pid)
    except psutil.Error:
        return
    children = parent.children(recursive=True)
    for process in reversed(children):
        try:
            process.kill()
        except psutil.Error:
            continue
    try:
        parent.kill()
    except psutil.Error:
        pass
    psutil.wait_procs(children + [parent], timeout=5)


def remove_directory(path: str | Path) -> None:
    try:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass
