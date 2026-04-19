from __future__ import annotations

import argparse
import atexit
import hashlib
import os
import socket
import sys
import threading
import time
from pathlib import Path

from uvicorn import Config, Server

from backend.config import APP_ROOT, ASSETS_DIR, FRONTEND_DIST_DIR
from backend.main import app
from backend.runtime_control import clear_backend_only_state, find_available_port as find_backend_port, write_backend_only_state
from backend.ui_bridge import register_directory_picker_callback, register_exit_callback


APP_TITLE = "Open-Anti-Browser · 开源指纹浏览器"


def find_available_port(preferred: int = 8000, span: int = 20) -> int:
    for port in range(preferred, preferred + span):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"没有找到可用端口，请先关闭占用 {preferred}~{preferred + span - 1} 的程序。")


def wait_for_port(port: int, timeout: float = 20.0) -> None:
    stop_at = time.time() + timeout
    while time.time() < stop_at:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.2)
    raise RuntimeError("本地服务启动超时。")


def resolve_window_icon_path() -> Path | None:
    candidates = [
        ASSETS_DIR / "app.ico",
        ASSETS_DIR / "logo-512.png",
        FRONTEND_DIST_DIR / "logo.png",
        FRONTEND_DIST_DIR / "logo.jpeg",
        Path(__file__).resolve().parent / "frontend" / "public" / "logo.png",
        Path(__file__).resolve().parent / "frontend" / "public" / "logo.jpeg",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def build_server(port: int) -> tuple[Server, threading.Thread]:
    config = Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        access_log=False,
        log_config=None,
    )
    server = Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    return server, thread


def _desktop_instance_server_name() -> str:
    identity = str(Path(sys.argv[0]).resolve()).lower()
    digest = hashlib.sha1(identity.encode("utf-8")).hexdigest()[:16]
    return f"OpenAntiBrowserDesktop_{digest}"


def _configure_desktop_webview_env() -> None:
    desired_flags = [
        "--disable-features=CalculateNativeWinOcclusion,BackForwardCache",
        "--enable-gpu-rasterization",
        "--enable-zero-copy",
    ]
    current_flags = os.environ.get("QTWEBENGINE_CHROMIUM_FLAGS", "").strip()
    merged_flags = [flag for flag in current_flags.split() if flag]
    existing_flags = set(merged_flags)
    for flag in desired_flags:
        if flag not in existing_flags:
            merged_flags.append(flag)
            existing_flags.add(flag)
    os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = " ".join(merged_flags).strip()
    os.environ.setdefault("QT_OPENGL", "desktop")


def run_backend_only(port: int | None = None) -> int:
    target_port = port or find_backend_port(18000, 20)
    write_backend_only_state(os.getpid(), target_port)
    atexit.register(clear_backend_only_state)
    try:
        server = Server(
            Config(
                app=app,
                host="127.0.0.1",
                port=target_port,
                log_level="warning",
                access_log=False,
                log_config=None,
            )
        )
        server.run()
        return 0
    finally:
        clear_backend_only_state()


def run_desktop() -> int:
    _configure_desktop_webview_env()
    from PySide6.QtCore import QObject, Qt, QTimer, QUrl, Signal, Slot
    from PySide6.QtGui import QAction, QCloseEvent, QIcon
    from PySide6.QtNetwork import QLocalServer, QLocalSocket
    from PySide6.QtWebEngineCore import QWebEnginePage, QWebEngineProfile, QWebEngineSettings
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWidgets import QApplication, QFileDialog, QMainWindow, QMenu, QMessageBox, QSystemTrayIcon

    QApplication.setAttribute(Qt.AA_UseDesktopOpenGL, True)
    QApplication.setAttribute(Qt.AA_ShareOpenGLContexts, True)

    class DirectoryPickerBridge(QObject):
        pick_directory_requested = Signal(str, str)

        def __init__(self, owner: "DesktopMainWindow") -> None:
            super().__init__(owner)
            self.owner = owner
            self._result: str | None = None
            self.pick_directory_requested.connect(self._pick_directory, Qt.BlockingQueuedConnection)

        def pick_directory(self, title: str = "", initial_dir: str = "") -> str | None:
            self._result = None
            self.pick_directory_requested.emit(title, initial_dir)
            return self._result

        @Slot(str, str)
        def _pick_directory(self, title: str, initial_dir: str) -> None:
            start_dir = str(initial_dir or APP_ROOT)
            chosen = QFileDialog.getExistingDirectory(
                self.owner,
                title or "选择扩展文件夹",
                start_dir,
            )
            self._result = chosen or None

    class DesktopMainWindow(QMainWindow):
        def __init__(self, url: str, server: Server, thread: threading.Thread) -> None:
            super().__init__()
            self.url = url
            self.server = server
            self.server_thread = thread
            self._closing = False
            self._force_exit = False
            self._tray_notified = False
            self._recovering_renderer = False
            self.tray_icon: QSystemTrayIcon | None = None

            self.setWindowTitle(APP_TITLE)
            self.setWindowIcon(window_icon)
            self.resize(1480, 960)
            self.setMinimumSize(1180, 760)

            self.web_profile = QWebEngineProfile(APP_TITLE, self)
            self.web_profile.setPersistentStoragePath(str(APP_ROOT / "data" / "qt-webview"))
            self.web_profile.setCachePath(str(APP_ROOT / "data" / "qt-webview-cache"))
            self.web_profile.setPersistentCookiesPolicy(QWebEngineProfile.ForcePersistentCookies)

            self.browser = QWebEngineView(self)
            self.browser.setPage(QWebEnginePage(self.web_profile, self.browser))
            self.browser.settings().setAttribute(QWebEngineSettings.LocalStorageEnabled, True)
            self.browser.settings().setAttribute(QWebEngineSettings.JavascriptEnabled, True)
            self.browser.settings().setAttribute(QWebEngineSettings.FullScreenSupportEnabled, True)
            self.browser.loadFinished.connect(self._handle_load_finished)
            self.browser.page().renderProcessTerminated.connect(self._handle_render_process_terminated)
            self.setCentralWidget(self.browser)
            self.browser.setUrl(QUrl(self.url))

            if QSystemTrayIcon.isSystemTrayAvailable():
                self._create_tray_icon()

        def _create_tray_icon(self) -> None:
            tray = QSystemTrayIcon(window_icon, self)
            tray.setToolTip(APP_TITLE)
            tray.activated.connect(self._handle_tray_activated)

            menu = QMenu()
            open_action = QAction("打开主界面", self)
            open_action.triggered.connect(self.restore_from_tray)
            exit_action = QAction("退出程序", self)
            exit_action.triggered.connect(self.force_exit)
            menu.addAction(open_action)
            menu.addSeparator()
            menu.addAction(exit_action)
            tray.setContextMenu(menu)
            tray.show()
            self.tray_icon = tray

        def _handle_load_finished(self, ok: bool) -> None:
            if ok:
                self._recovering_renderer = False
                return
            if self._closing:
                return
            QMessageBox.critical(
                self,
                APP_TITLE,
                f"页面加载失败。\n\n请确认本地服务是否正常启动：{self.url}",
            )

        def _handle_render_process_terminated(self, termination_status, exit_code: int) -> None:
            if self._closing:
                return
            if self._recovering_renderer:
                return
            self._recovering_renderer = True
            if self.tray_icon is not None:
                self.tray_icon.showMessage(
                    APP_TITLE,
                    "界面已自动恢复，请继续使用。",
                    QSystemTrayIcon.Warning,
                    2500,
                )
            QTimer.singleShot(450, lambda: self.browser.setUrl(QUrl(self.url)))

        def _handle_tray_activated(self, reason) -> None:
            if reason in (QSystemTrayIcon.Trigger, QSystemTrayIcon.DoubleClick):
                self.restore_from_tray()

        def restore_from_tray(self) -> None:
            self.showNormal()
            self.setWindowState((self.windowState() & ~Qt.WindowMinimized) | Qt.WindowActive)
            self.raise_()
            self.activateWindow()

        def force_exit(self) -> None:
            self._force_exit = True
            self.showNormal()
            self.close()

        def closeEvent(self, event: QCloseEvent) -> None:
            if not self._force_exit and self.tray_icon is not None:
                self.hide()
                if not self._tray_notified:
                    self.tray_icon.showMessage(
                        APP_TITLE,
                        "程序已最小化到托盘，可在托盘图标中重新打开或退出。",
                        QSystemTrayIcon.Information,
                        2500,
                    )
                    self._tray_notified = True
                event.ignore()
                return
            self.shutdown()
            event.accept()
            QTimer.singleShot(0, QApplication.instance().quit)

        def shutdown(self) -> None:
            if self._closing:
                return
            self._closing = True
            self.server.should_exit = True
            self.server.force_exit = True
            self.browser.stop()
            if self.tray_icon is not None:
                self.tray_icon.hide()
            self.server_thread.join(timeout=8)

    qt_app = QApplication.instance() or QApplication([])
    qt_app.setApplicationDisplayName(APP_TITLE)
    qt_app.setApplicationName(APP_TITLE)
    qt_app.setQuitOnLastWindowClosed(False)
    icon_path = resolve_window_icon_path()
    window_icon = QIcon(str(icon_path)) if icon_path else QIcon()
    if not window_icon.isNull():
        qt_app.setWindowIcon(window_icon)

    instance_server_name = _desktop_instance_server_name()
    activation_socket = QLocalSocket()
    activation_socket.connectToServer(instance_server_name)
    if activation_socket.waitForConnected(500):
        activation_socket.write(b"activate")
        activation_socket.flush()
        activation_socket.waitForBytesWritten(500)
        activation_socket.disconnectFromServer()
        return 0
    activation_socket.abort()

    instance_server = QLocalServer()
    if not instance_server.listen(instance_server_name):
        QLocalServer.removeServer(instance_server_name)
        if not instance_server.listen(instance_server_name):
            QMessageBox.critical(None, APP_TITLE, "程序实例检测失败，请先关闭已有程序后重试")
            return 1

    try:
        port = find_available_port(8000, 20)
        server, thread = build_server(port)
        wait_for_port(port)
    except Exception as exc:
        QMessageBox.critical(None, APP_TITLE, f"启动失败：\n{exc}")
        return 1

    window = DesktopMainWindow(f"http://127.0.0.1:{port}?shell=desktop", server, thread)
    directory_picker_bridge = DirectoryPickerBridge(window)

    def handle_instance_activation() -> None:
        while instance_server.hasPendingConnections():
            connection = instance_server.nextPendingConnection()
            if connection is None:
                break
            connection.waitForReadyRead(200)
            try:
                connection.readAll()
            except Exception:
                pass
            connection.disconnectFromServer()
        QTimer.singleShot(0, window.restore_from_tray)

    instance_server.newConnection.connect(handle_instance_activation)

    def request_exit_from_api() -> None:
        QTimer.singleShot(0, window.force_exit)

    register_exit_callback(request_exit_from_api)
    register_directory_picker_callback(directory_picker_bridge.pick_directory)
    qt_app.aboutToQuit.connect(window.shutdown)
    qt_app.aboutToQuit.connect(lambda: register_exit_callback(None))
    qt_app.aboutToQuit.connect(lambda: register_directory_picker_callback(None))
    qt_app.aboutToQuit.connect(instance_server.close)
    qt_app.aboutToQuit.connect(lambda: QLocalServer.removeServer(instance_server_name))
    window.show()
    QTimer.singleShot(120, window.activateWindow)
    return qt_app.exec()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--backend-only", action="store_true")
    parser.add_argument("--port", type=int, default=None)
    args, _ = parser.parse_known_args(argv)

    if args.backend_only:
        return run_backend_only(args.port)
    return run_desktop()


if __name__ == "__main__":
    raise SystemExit(main())
