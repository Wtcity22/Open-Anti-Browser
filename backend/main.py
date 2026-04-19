from __future__ import annotations

import os
import webbrowser
from pathlib import Path
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Request, Security, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from fastapi.staticfiles import StaticFiles

from .browser_manager import BrowserManager
from .config import FRONTEND_DIST_DIR
from .runtime_control import get_backend_only_status, start_backend_only, stop_backend_only
from .ui_bridge import request_exit_ui, request_pick_directory


manager = BrowserManager()
app = FastAPI(title="Open-Anti-Browser API", version="0.1.2")
open_api = FastAPI(
    title="Open-Anti-Browser Open API",
    version="0.1.2",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _request_open_api_base_url(request: Request) -> str:
    return f"{str(request.base_url).rstrip('/')}/open-api"


def _request_manual_docs_url(request: Request) -> str:
    return f"{str(request.base_url).rstrip('/')}?view=apiDocs"


def verify_open_api_key(
    x_api_key: str | None = Security(api_key_header),
    authorization: str | None = Header(default=None),
) -> None:
    settings = manager.get_settings()
    if not settings.api_access.enabled:
        raise HTTPException(status_code=403, detail="Open API 未启用")

    token = (x_api_key or "").strip()
    if not token and authorization:
        scheme, _, value = authorization.partition(" ")
        if scheme.lower() == "bearer":
            token = value.strip()
    if not token or token != settings.api_access.api_key:
        raise HTTPException(status_code=401, detail="API Key 无效")


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/bootstrap")
def bootstrap() -> dict:
    return manager.bootstrap()


@app.get("/api/settings")
def get_settings() -> dict:
    return manager.get_settings().model_dump(mode="json")


@app.put("/api/settings")
def update_settings(payload: dict) -> dict:
    try:
        return manager.update_settings(payload).model_dump(mode="json")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/profiles")
def list_profiles() -> list[dict]:
    return manager.list_profiles()


@app.post("/api/profiles")
def save_profile(payload: dict) -> dict:
    try:
        return manager.save_profile(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/profiles/{profile_id}")
def delete_profile(profile_id: str) -> dict[str, bool]:
    manager.delete_profile(profile_id)
    return {"ok": True}


@app.post("/api/profiles/{profile_id}/duplicate")
def duplicate_profile(profile_id: str) -> dict:
    try:
        return manager.duplicate_profile(profile_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/profiles/{profile_id}/start")
def start_profile(profile_id: str) -> dict:
    try:
        return manager.start_profile(profile_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/profiles/{profile_id}/stop")
def stop_profile(profile_id: str) -> dict:
    try:
        payload = manager.stop_profile(profile_id)
        return payload or {"ok": True}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/groups/{group_name}/start")
def start_group(group_name: str) -> list[dict]:
    if group_name == "_ungrouped_":
        group_name = ""
    return manager.start_group(group_name)


@app.post("/api/groups/{group_name}/stop")
def stop_group(group_name: str) -> list[dict]:
    if group_name == "_ungrouped_":
        group_name = ""
    return manager.stop_group(group_name)


@app.post("/api/proxy/test")
def test_proxy(payload: dict) -> dict:
    try:
        return manager.test_proxy(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/saved-proxies")
def list_saved_proxies() -> list[dict]:
    return manager.list_saved_proxies()


@app.post("/api/saved-proxies")
def save_saved_proxy(payload: dict) -> dict:
    try:
        return manager.save_saved_proxy(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/saved-proxies/{proxy_id}")
def delete_saved_proxy(proxy_id: str) -> dict[str, bool]:
    manager.delete_saved_proxy(proxy_id)
    return {"ok": True}


@app.post("/api/saved-proxies/{proxy_id}/assign")
def assign_saved_proxy(proxy_id: str, payload: dict) -> list[dict]:
    try:
        profile_ids = payload.get("profile_ids") or []
        return manager.assign_saved_proxy(proxy_id, profile_ids)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/extensions")
def list_extensions(engine: str | None = None) -> list[dict]:
    try:
        return manager.list_managed_extensions(engine)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/extensions/upload")
async def upload_extension(
    engine: str = Form(...),
    name: str = Form(default=""),
    file: UploadFile = File(...),
) -> dict:
    try:
        content = await file.read()
        return manager.save_managed_extension(engine, file.filename or "extension", content, name=name)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/extensions/import-folder")
def import_extension_folder(payload: dict) -> dict:
    try:
        engine = payload.get("engine")
        folder_path = payload.get("folder_path")
        name = payload.get("name")
        return manager.import_managed_extension_folder(engine, folder_path, name=name)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.put("/api/extensions/{extension_id}")
def update_extension(extension_id: str, payload: dict) -> dict:
    try:
        return manager.update_managed_extension(extension_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/extensions/{extension_id}")
def delete_extension(extension_id: str) -> dict[str, bool]:
    try:
        manager.delete_managed_extension(extension_id)
        return {"ok": True}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/engines")
def get_engines() -> dict:
    return manager.get_engine_statuses()


@app.post("/api/engines/{engine}/download")
def start_engine_download(engine: str) -> dict:
    try:
        return manager.start_download(engine)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/downloads")
def get_downloads() -> dict:
    return manager.downloads.get_all()


@app.get("/api/open-api/info")
def open_api_info(request: Request) -> dict:
    settings = manager.get_settings()
    current_base_url = _request_open_api_base_url(request)
    current_docs_url = _request_manual_docs_url(request)
    backend_mode = get_backend_only_status(settings.api_access.backend_only_port)
    return {
        "enabled": settings.api_access.enabled,
        "api_key": settings.api_access.api_key,
        "current_base_url": current_base_url,
        "current_docs_url": current_docs_url,
        "backend_mode": backend_mode,
    }


@app.post("/api/open-api/regenerate-key")
def regenerate_open_api_key(request: Request) -> dict:
    api_access = manager.regenerate_api_key()
    current_base_url = _request_open_api_base_url(request)
    current_docs_url = _request_manual_docs_url(request)
    return {
        "enabled": api_access["enabled"],
        "api_key": api_access["api_key"],
        "current_base_url": current_base_url,
        "current_docs_url": current_docs_url,
        "backend_mode": get_backend_only_status(api_access["backend_only_port"]),
    }


@app.get("/api/backend-mode/status")
def backend_mode_status() -> dict:
    return get_backend_only_status(manager.get_settings().api_access.backend_only_port)


@app.post("/api/backend-mode/start")
def start_backend_mode() -> dict:
    try:
        return start_backend_only(manager.get_settings().api_access.backend_only_port)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/backend-mode/stop")
def stop_backend_mode() -> dict:
    try:
        return stop_backend_only(manager.get_settings().api_access.backend_only_port)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/app/exit")
def exit_desktop_app() -> dict[str, bool]:
    return {"ok": request_exit_ui()}


@app.post("/api/system/open-url")
def open_system_url(payload: dict) -> dict[str, bool]:
    raw_url = str(payload.get("url") or "").strip()
    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(status_code=400, detail="只支持打开 http 或 https 链接")

    try:
        if os.name == "nt":
            os.startfile(raw_url)  # type: ignore[attr-defined]
        else:
            webbrowser.open_new_tab(raw_url)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"打开链接失败：{exc}") from exc

    return {"ok": True}


@app.post("/api/system/pick-directory")
def pick_system_directory(payload: dict) -> dict[str, str]:
    title = str(payload.get("title") or "").strip()
    initial_dir = str(payload.get("initial_dir") or "").strip()
    try:
        path = request_pick_directory(title, initial_dir)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"path": path or ""}


@app.get("/api/export")
def export_profiles() -> list[dict]:
    return manager.export_profiles()


@app.post("/api/import")
async def import_profiles(file: UploadFile = File(...)) -> dict:
    try:
        import json

        payload = json.loads((await file.read()).decode("utf-8"))
        if not isinstance(payload, list):
            raise ValueError("导入文件必须是数组")
        imported = manager.import_profiles(payload)
        return {"count": len(imported), "items": imported}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@open_api.get("/health", dependencies=[Depends(verify_open_api_key)], summary="服务状态")
def open_api_health() -> dict[str, str]:
    return {"status": "ok"}


@open_api.get("/profiles", dependencies=[Depends(verify_open_api_key)], summary="获取浏览器配置列表")
def open_api_list_profiles() -> list[dict]:
    return manager.list_profiles()


@open_api.post("/profiles", dependencies=[Depends(verify_open_api_key)], summary="创建或更新浏览器配置")
def open_api_save_profile(payload: dict) -> dict:
    try:
        return manager.save_profile(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@open_api.delete("/profiles/{profile_id}", dependencies=[Depends(verify_open_api_key)], summary="删除浏览器配置")
def open_api_delete_profile(profile_id: str) -> dict[str, bool]:
    manager.delete_profile(profile_id)
    return {"ok": True}


@open_api.post("/profiles/{profile_id}/start", dependencies=[Depends(verify_open_api_key)], summary="启动浏览器配置")
def open_api_start_profile(profile_id: str) -> dict:
    try:
        return manager.start_profile(profile_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@open_api.post("/profiles/{profile_id}/stop", dependencies=[Depends(verify_open_api_key)], summary="停止浏览器配置")
def open_api_stop_profile(profile_id: str) -> dict:
    try:
        return manager.stop_profile(profile_id) or {"ok": True}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@open_api.get("/settings", dependencies=[Depends(verify_open_api_key)], summary="读取全局设置")
def open_api_get_settings() -> dict:
    return manager.get_settings().model_dump(mode="json")


@open_api.get("/saved-proxies", dependencies=[Depends(verify_open_api_key)], summary="获取已保存代理")
def open_api_list_saved_proxies() -> list[dict]:
    return manager.list_saved_proxies()


@open_api.get("/extensions", dependencies=[Depends(verify_open_api_key)], summary="获取扩展列表")
def open_api_list_extensions(engine: str | None = None) -> list[dict]:
    return manager.list_managed_extensions(engine)


@open_api.put("/extensions/{extension_id}", dependencies=[Depends(verify_open_api_key)], summary="更新扩展状态")
def open_api_update_extension(extension_id: str, payload: dict) -> dict:
    try:
        return manager.update_managed_extension(extension_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@open_api.delete("/extensions/{extension_id}", dependencies=[Depends(verify_open_api_key)], summary="删除扩展")
def open_api_delete_extension(extension_id: str) -> dict[str, bool]:
    try:
        manager.delete_managed_extension(extension_id)
        return {"ok": True}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@open_api.post("/proxy/test", dependencies=[Depends(verify_open_api_key)], summary="测试代理")
def open_api_test_proxy(payload: dict) -> dict:
    try:
        return manager.test_proxy(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


app.mount("/open-api", open_api)


if FRONTEND_DIST_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST_DIR / "assets")), name="assets")

    @app.get("/{full_path:path}")
    def frontend(full_path: str):
        target = FRONTEND_DIST_DIR / full_path
        if full_path and target.exists() and target.is_file():
            return FileResponse(target)
        return FileResponse(FRONTEND_DIST_DIR / "index.html")
else:
    @app.get("/{full_path:path}")
    def frontend_missing(full_path: str):
        raise HTTPException(status_code=404, detail="前端还没构建，请先运行 npm run build")
