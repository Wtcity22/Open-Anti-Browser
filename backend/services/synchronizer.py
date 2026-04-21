from __future__ import annotations

import json
import queue
import random
import threading
import time
import urllib.request
from collections import deque
from typing import Any, Callable

import websocket

try:
    from ruyipage import FirefoxOptions, FirefoxPage
except Exception:  # pragma: no cover - only used when Firefox synchronizer starts
    FirefoxOptions = None
    FirefoxPage = None


SYNC_EVENT_PREFIX = "__OAB_SYNC__"
SYNC_HEARTBEAT_SECONDS = 1.2
SYNC_FIREFOX_POLL_SECONDS = 0.035
SYNC_CLICK_NEW_TAB_DEFER_SECONDS = 1.5
SYNC_DISCOVERY_TIMEOUT = 5
SYNC_COMMAND_TIMEOUT = 6
SYNC_WORKER_QUEUE_LIMIT = 280

MASTER_INJECT_SCRIPT = r"""
(() => {
  if (window.__oabSyncInstalled) {
    return 'installed';
  }

  const prefix = '__OAB_SYNC__';
  window.__oabSyncQueue = Array.isArray(window.__oabSyncQueue) ? window.__oabSyncQueue : [];
  window.__oabSyncDrain = () => window.__oabSyncQueue.splice(0, window.__oabSyncQueue.length);
  const state = {
    lastLocation: location.href,
    inputTimer: null,
    moveFrame: null,
    movePayload: null,
    scrollTimer: null,
    wheelCalibrateTimer: null,
    wheelFrame: null,
    wheelPayload: null,
    lastWheelAt: 0,
    suppressScrollUntil: 0,
  };

  const cssEscape = (value) => {
    try {
      if (window.CSS && typeof window.CSS.escape === 'function') {
        return window.CSS.escape(String(value));
      }
    } catch (error) {
      // ignore
    }
    return String(value).replace(/([^a-zA-Z0-9_-])/g, '\\$1');
  };

  const selectorEscape = (value) => String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"');

  const buildSelector = (node) => {
    if (!node || node.nodeType !== 1) {
      return '';
    }
    if (node.id) {
      return `#${cssEscape(node.id)}`;
    }
    const directAttr = node.getAttribute && (node.getAttribute('data-testid') || node.getAttribute('data-test') || node.getAttribute('name'));
    if (directAttr) {
      const attrName = node.getAttribute('data-testid') ? 'data-testid' : (node.getAttribute('data-test') ? 'data-test' : 'name');
      const selector = `${node.localName.toLowerCase()}[${attrName}="${selectorEscape(directAttr)}"]`;
      try {
        if (document.querySelectorAll(selector).length === 1) {
          return selector;
        }
      } catch (error) {
        // ignore
      }
    }

    const parts = [];
    let current = node;
    while (current && current.nodeType === 1 && parts.length < 7) {
      let part = current.localName.toLowerCase();
      if (current.id) {
        part = `#${cssEscape(current.id)}`;
        parts.unshift(part);
        break;
      }
      const nameValue = current.getAttribute && current.getAttribute('name');
      if (nameValue) {
        part += `[name="${selectorEscape(nameValue)}"]`;
      }
      const parent = current.parentElement;
      if (parent) {
        const siblings = Array.from(parent.children).filter(item => item.localName === current.localName);
        if (siblings.length > 1) {
          part += `:nth-of-type(${siblings.indexOf(current) + 1})`;
        }
      }
      parts.unshift(part);
      try {
        const selector = parts.join(' > ');
        if (document.querySelectorAll(selector).length === 1) {
          return selector;
        }
      } catch (error) {
        // ignore
      }
      current = current.parentElement;
    }
    return parts.join(' > ');
  };

  const emit = (type, payload) => {
    const event = {
      type,
      payload,
      href: location.href,
      ts: Date.now(),
    };
    const body = JSON.stringify(event);
    try {
      window.__oabSyncQueue.push(body);
      if (window.__oabSyncQueue.length > 220) {
        window.__oabSyncQueue.splice(0, window.__oabSyncQueue.length - 220);
      }
    } catch (error) {
      // ignore
    }
    try {
      if (typeof window.__oabSyncBinding === 'function') {
        window.__oabSyncBinding(body);
        return;
      }
    } catch (error) {
      // ignore
    }
    try {
      console.debug(prefix + body);
    } catch (error) {
      // ignore
    }
  };

  const buildPoint = (event) => ({
    x: Number(event.clientX || 0),
    y: Number(event.clientY || 0),
    rx: window.innerWidth ? Number(event.clientX || 0) / window.innerWidth : 0,
    ry: window.innerHeight ? Number(event.clientY || 0) / window.innerHeight : 0,
  });

  const maxScrollTop = () => Math.max(document.documentElement.scrollHeight, document.body.scrollHeight) - window.innerHeight;
  const maxScrollLeft = () => Math.max(document.documentElement.scrollWidth, document.body.scrollWidth) - window.innerWidth;

  document.addEventListener('click', (event) => {
    emit('click', {
      ...buildPoint(event),
      selector: buildSelector(event.target),
      button: Number(event.button || 0),
      ctrlKey: !!event.ctrlKey,
      shiftKey: !!event.shiftKey,
      altKey: !!event.altKey,
      metaKey: !!event.metaKey,
    });
  }, true);

  const emitInput = (type, event) => {
    const target = event.target;
    if (!target || target.nodeType !== 1) return;
    const tag = (target.tagName || '').toLowerCase();
    if (!['input', 'textarea', 'select'].includes(tag) && !target.isContentEditable) {
      return;
    }
    const payload = {
      selector: buildSelector(target),
      tag,
      inputType: target.type || '',
      value: target.isContentEditable ? target.innerText : (typeof target.value === 'string' ? target.value : ''),
      checked: typeof target.checked === 'boolean' ? !!target.checked : null,
    };
    emit(type, payload);
  };

  document.addEventListener('input', (event) => {
    window.clearTimeout(state.inputTimer);
    state.inputTimer = window.setTimeout(() => emitInput('input', event), 40);
  }, true);

  document.addEventListener('change', (event) => {
    emitInput('change', event);
  }, true);

  document.addEventListener('keydown', (event) => {
    const shouldSync = ['Enter', 'Tab', 'Escape'].includes(event.key) || event.ctrlKey || event.metaKey || event.altKey;
    if (!shouldSync) return;
    emit('keydown', {
      selector: buildSelector(event.target),
      key: event.key,
      code: event.code,
      ctrlKey: !!event.ctrlKey,
      shiftKey: !!event.shiftKey,
      altKey: !!event.altKey,
      metaKey: !!event.metaKey,
    });
  }, true);

  const emitScrollState = (target) => {
    if (target && target.nodeType === 1 && target !== document.body && target !== document.documentElement) {
      const selector = buildSelector(target);
      const maxY = Math.max(0, Number(target.scrollHeight || 0) - Number(target.clientHeight || 0));
      const maxX = Math.max(0, Number(target.scrollWidth || 0) - Number(target.clientWidth || 0));
      emit('scroll', {
        mode: 'element',
        selector,
        scrollTop: Number(target.scrollTop || 0),
        scrollLeft: Number(target.scrollLeft || 0),
        ratioX: maxX > 0 ? Number(target.scrollLeft || 0) / maxX : 0,
        ratioY: maxY > 0 ? Number(target.scrollTop || 0) / maxY : 0,
      });
      return;
    }
    emit('scroll', {
      mode: 'window',
      x: Number(window.scrollX || 0),
      y: Number(window.scrollY || 0),
      ratioX: maxScrollLeft() > 0 ? Number(window.scrollX || 0) / maxScrollLeft() : 0,
      ratioY: maxScrollTop() > 0 ? Number(window.scrollY || 0) / maxScrollTop() : 0,
    });
  };

  const scheduleScrollEmit = (target) => {
    window.clearTimeout(state.scrollTimer);
    state.scrollTimer = window.setTimeout(() => {
      emitScrollState(target);
    }, 80);
  };

  const scheduleMoveEmit = (event) => {
    state.movePayload = buildPoint(event);
    if (state.moveFrame) {
      return;
    }
    state.moveFrame = window.requestAnimationFrame(() => {
      state.moveFrame = null;
      if (!state.movePayload) {
        return;
      }
      emit('mouse_move', state.movePayload);
      state.movePayload = null;
    });
  };

  const patchWindowScroll = (name) => {
    const original = window[name];
    if (typeof original !== 'function') {
      return;
    }
    window[name] = function (...args) {
      const result = original.apply(this, args);
      window.setTimeout(() => {
        if (Date.now() >= state.suppressScrollUntil) {
          emitScrollState(null);
        }
      }, 60);
      return result;
    };
  };

  document.addEventListener('mousemove', scheduleMoveEmit, true);
  document.addEventListener('wheel', (event) => {
    state.lastWheelAt = Date.now();
    state.suppressScrollUntil = state.lastWheelAt + 1500;
    const wheelTarget = event.target && event.target.nodeType === 1 ? event.target : null;
    const nextPayload = {
      ...buildPoint(event),
      deltaX: Number(event.deltaX || 0),
      deltaY: Number(event.deltaY || 0),
      deltaMode: Number(event.deltaMode || 0),
      ctrlKey: !!event.ctrlKey,
      shiftKey: !!event.shiftKey,
      altKey: !!event.altKey,
      metaKey: !!event.metaKey,
    };
    if (state.wheelPayload) {
      state.wheelPayload.deltaX += nextPayload.deltaX;
      state.wheelPayload.deltaY += nextPayload.deltaY;
      state.wheelPayload.x = nextPayload.x;
      state.wheelPayload.y = nextPayload.y;
      state.wheelPayload.rx = nextPayload.rx;
      state.wheelPayload.ry = nextPayload.ry;
      state.wheelPayload.ctrlKey = nextPayload.ctrlKey;
      state.wheelPayload.shiftKey = nextPayload.shiftKey;
      state.wheelPayload.altKey = nextPayload.altKey;
      state.wheelPayload.metaKey = nextPayload.metaKey;
    } else {
      state.wheelPayload = nextPayload;
    }
    window.clearTimeout(state.wheelCalibrateTimer);
    state.wheelCalibrateTimer = window.setTimeout(() => {
      const calibrationTarget = wheelTarget && wheelTarget.isConnected ? wheelTarget : null;
      if (calibrationTarget && calibrationTarget !== document.body && calibrationTarget !== document.documentElement) {
        const selector = buildSelector(calibrationTarget);
        const maxY = Math.max(0, Number(calibrationTarget.scrollHeight || 0) - Number(calibrationTarget.clientHeight || 0));
        const maxX = Math.max(0, Number(calibrationTarget.scrollWidth || 0) - Number(calibrationTarget.clientWidth || 0));
        emit('scroll', {
          source: 'wheel_calibrate',
          mode: 'element',
          selector,
          scrollTop: Number(calibrationTarget.scrollTop || 0),
          scrollLeft: Number(calibrationTarget.scrollLeft || 0),
          ratioX: maxX > 0 ? Number(calibrationTarget.scrollLeft || 0) / maxX : 0,
          ratioY: maxY > 0 ? Number(calibrationTarget.scrollTop || 0) / maxY : 0,
        });
        return;
      }
      emit('scroll', {
        source: 'wheel_calibrate',
        mode: 'window',
        x: Number(window.scrollX || 0),
        y: Number(window.scrollY || 0),
        ratioX: maxScrollLeft() > 0 ? Number(window.scrollX || 0) / maxScrollLeft() : 0,
        ratioY: maxScrollTop() > 0 ? Number(window.scrollY || 0) / maxScrollTop() : 0,
      });
    }, 280);
    if (state.wheelFrame) {
      return;
    }
    state.wheelFrame = window.requestAnimationFrame(() => {
      state.wheelFrame = null;
      if (!state.wheelPayload) {
        return;
      }
      emit('wheel', state.wheelPayload);
      state.wheelPayload = null;
    });
  }, { capture: true, passive: true });

  document.addEventListener('scroll', (event) => {
    if (Date.now() < state.suppressScrollUntil) {
      return;
    }
    const rawTarget = event.target && event.target !== document ? event.target : null;
    const target = rawTarget && rawTarget.nodeType === 1 ? rawTarget : null;
    scheduleScrollEmit(target);
  }, true);
  patchWindowScroll('scrollTo');
  patchWindowScroll('scrollBy');

  const emitNavigate = () => {
    if (state.lastLocation === location.href) {
      return;
    }
    state.lastLocation = location.href;
    emit('navigate', { url: location.href });
  };

  const patchHistory = (name) => {
    const original = history[name];
    if (typeof original !== 'function') {
      return;
    }
    history[name] = function (...args) {
      const result = original.apply(this, args);
      window.setTimeout(emitNavigate, 0);
      return result;
    };
  };

  patchHistory('pushState');
  patchHistory('replaceState');
  window.addEventListener('hashchange', emitNavigate, true);
  window.addEventListener('popstate', emitNavigate, true);
  window.addEventListener('load', emitNavigate, true);
  window.setInterval(emitNavigate, 700);

  window.__oabSyncInstalled = true;
  return 'installed';
})();
"""

FOLLOWER_VISUAL_SCRIPT = r"""
(() => {
  if (window.__oabSyncVisual && window.__oabSyncVisual.version === 1) {
    return true;
  }

  const rootId = '__oab-sync-visual-root';
  const oldRoot = document.getElementById(rootId);
  if (oldRoot) {
    oldRoot.remove();
  }

  const root = document.createElement('div');
  root.id = rootId;
  root.style.cssText = [
    'position:fixed',
    'inset:0',
    'z-index:2147483647',
    'pointer-events:none',
    'overflow:hidden',
    'contain:layout style paint',
  ].join(';');

  const cursor = document.createElement('div');
  cursor.style.cssText = [
    'position:absolute',
    'left:0',
    'top:0',
    'width:12px',
    'height:12px',
    'margin-left:-6px',
    'margin-top:-6px',
    'border-radius:999px',
    'background:#0a84ff',
    'border:2px solid rgba(255,255,255,.95)',
    'box-shadow:0 0 0 3px rgba(10,132,255,.18),0 8px 20px rgba(0,0,0,.22)',
    'opacity:0',
    'transform:translate3d(-100px,-100px,0)',
    'transition:opacity .18s ease',
    'will-change:transform,opacity',
  ].join(';');
  root.appendChild(cursor);

  const ensureRoot = () => {
    if (!root.isConnected) {
      (document.documentElement || document.body).appendChild(root);
    }
  };

  const clamp = (value, max) => Math.max(0, Math.min(Math.round(Number(value || 0)), Math.max(0, max - 1)));
  const state = {
    x: -100,
    y: -100,
    visibleUntil: 0,
    frame: 0,
    trailCount: 0,
  };

  const render = () => {
    state.frame = 0;
    ensureRoot();
    const now = Date.now();
    cursor.style.opacity = now < state.visibleUntil ? '1' : '0';
    cursor.style.transform = `translate3d(${state.x}px, ${state.y}px, 0)`;
    if (now < state.visibleUntil) {
      state.frame = requestAnimationFrame(render);
    }
  };

  const show = (x, y) => {
    state.x = clamp(x, window.innerWidth);
    state.y = clamp(y, window.innerHeight);
    state.visibleUntil = Date.now() + 900;
    if (!state.frame) {
      state.frame = requestAnimationFrame(render);
    }
  };

  const trail = (x, y) => {
    ensureRoot();
    const dot = document.createElement('div');
    const size = Math.max(5, 10 - Math.min(4, state.trailCount % 5));
    dot.style.cssText = [
      'position:absolute',
      `left:${clamp(x, window.innerWidth)}px`,
      `top:${clamp(y, window.innerHeight)}px`,
      `width:${size}px`,
      `height:${size}px`,
      `margin-left:${-size / 2}px`,
      `margin-top:${-size / 2}px`,
      'border-radius:999px',
      'background:rgba(10,132,255,.26)',
      'transform:translate3d(0,0,0) scale(1)',
      'opacity:.92',
      'transition:opacity .38s ease, transform .38s ease',
      'will-change:opacity,transform',
    ].join(';');
    root.appendChild(dot);
    state.trailCount += 1;
    requestAnimationFrame(() => {
      dot.style.opacity = '0';
      dot.style.transform = 'translate3d(0,0,0) scale(.3)';
    });
    window.setTimeout(() => dot.remove(), 440);
  };

  const pulse = (x, y, color = '10,132,255') => {
    ensureRoot();
    const ring = document.createElement('div');
    ring.style.cssText = [
      'position:absolute',
      `left:${clamp(x, window.innerWidth)}px`,
      `top:${clamp(y, window.innerHeight)}px`,
      'width:14px',
      'height:14px',
      'margin-left:-7px',
      'margin-top:-7px',
      'border-radius:999px',
      `border:2px solid rgba(${color},.62)`,
      `background:rgba(${color},.08)`,
      'opacity:.95',
      'transform:scale(.55)',
      'transition:opacity .42s ease, transform .42s ease',
      'will-change:opacity,transform',
    ].join(';');
    root.appendChild(ring);
    requestAnimationFrame(() => {
      ring.style.opacity = '0';
      ring.style.transform = 'scale(3.1)';
    });
    window.setTimeout(() => ring.remove(), 500);
  };

  window.__oabSyncVisual = {
    version: 1,
    move(x, y) {
      show(x, y);
      trail(x, y);
      return true;
    },
    click(x, y) {
      show(x, y);
      pulse(x, y);
      return true;
    },
    wheel(x, y) {
      show(x, y);
      pulse(x, y, '142,142,147');
      return true;
    },
  };

  ensureRoot();
  return true;
})();
"""


def _http_json(url: str, timeout: float = SYNC_DISCOVERY_TIMEOUT) -> Any:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Open-Anti-Browser-Syncer"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="ignore"))


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


class CdpPageClient:
    def __init__(
        self,
        profile_id: str,
        port: int,
        event_handler: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.profile_id = profile_id
        self.port = int(port)
        self._event_handler = event_handler
        self._lock = threading.RLock()
        self._pending: dict[int, queue.Queue] = {}
        self._message_id = 0
        self._ws: websocket.WebSocket | None = None
        self._recv_thread: threading.Thread | None = None
        self._connected = False
        self._last_error = ""
        self._target: dict[str, Any] = {}
        self._last_seen_at: str | None = None

    @property
    def is_connected(self) -> bool:
        with self._lock:
            return bool(self._connected and self._ws)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "profile_id": self.profile_id,
                "port": self.port,
                "connected": self._connected,
                "target_id": self._target.get("id") or "",
                "target_url": self._target.get("url") or "",
                "target_title": self._target.get("title") or "",
                "last_seen_at": self._last_seen_at,
                "last_error": self._last_error,
            }

    def current_target_id(self) -> str:
        with self._lock:
            return str(self._target.get("id") or "")

    def connect(self, target_id: str | None = None) -> None:
        target = self._discover_target(target_id=target_id)
        ws_url = target.get("webSocketDebuggerUrl")
        if not ws_url:
            raise RuntimeError(f"{self.profile_id} 未找到可用调试页面")

        self.close()
        with self._lock:
            self._target = target
            self._last_error = ""
            self._message_id = 0
            self._pending = {}
            self._ws = websocket.create_connection(
                ws_url,
                timeout=SYNC_DISCOVERY_TIMEOUT,
                enable_multithread=True,
                suppress_origin=True,
            )
            self._ws.settimeout(1)
            self._connected = True
            self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
            self._recv_thread.start()

        self.send("Runtime.enable")
        self.send("Page.enable")
        self._mark_seen()

    def close(self) -> None:
        with self._lock:
            ws = self._ws
            recv_thread = self._recv_thread
            pending = list(self._pending.values())
            self._pending = {}
            self._ws = None
            self._recv_thread = None
            self._connected = False
        for item in pending:
            try:
                item.put_nowait({"error": {"message": "connection_closed"}})
            except Exception:
                pass
        if ws:
            try:
                ws.close()
            except Exception:
                pass
        if recv_thread and recv_thread.is_alive() and recv_thread is not threading.current_thread():
            recv_thread.join(timeout=1)

    def refresh_target(self) -> None:
        try:
            target = self._discover_target()
        except Exception:
            return
        with self._lock:
            self._target.update({
                "url": target.get("url") or self._target.get("url") or "",
                "title": target.get("title") or self._target.get("title") or "",
            })

    def sync_to_current_target(self) -> str:
        target = self._discover_target()
        target_id = str(target.get("id") or "")
        if target_id and target_id != self.current_target_id():
            self.switch_target(target_id)
        return target_id

    def ensure_ready(self) -> None:
        if self.is_connected:
            return
        self.connect()

    def send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        timeout: float = SYNC_COMMAND_TIMEOUT,
        wait: bool = True,
    ) -> dict[str, Any]:
        self.ensure_ready()
        response_queue: queue.Queue | None = queue.Queue(maxsize=1) if wait else None
        with self._lock:
            if not self._ws or not self._connected:
                raise RuntimeError(f"{self.profile_id} 调试连接不可用")
            self._message_id += 1
            message_id = self._message_id
            if response_queue:
                self._pending[message_id] = response_queue
            payload = json.dumps({
                "id": message_id,
                "method": method,
                "params": params or {},
            })
            self._ws.send(payload)
        if not response_queue:
            return {}
        try:
            response = response_queue.get(timeout=timeout)
        except queue.Empty as exc:
            with self._lock:
                self._pending.pop(message_id, None)
            raise RuntimeError(f"{self.profile_id} 调试命令超时：{method}") from exc

        if response.get("error"):
            error = response["error"]
            message = error.get("message") if isinstance(error, dict) else str(error)
            raise RuntimeError(f"{self.profile_id} 调试命令失败：{message}")
        return response.get("result") or {}

    def evaluate(self, expression: str) -> Any:
        result = self.send(
            "Runtime.evaluate",
            {
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": True,
                "userGesture": True,
            },
        )
        if result.get("exceptionDetails"):
            raise RuntimeError(f"{self.profile_id} 页面脚本执行失败")
        remote_object = result.get("result") or {}
        if "value" in remote_object:
            return remote_object.get("value")
        return remote_object.get("description")

    def dispatch_mouse_event(self, payload: dict[str, Any], wait: bool = True) -> None:
        self.send("Input.dispatchMouseEvent", payload, wait=wait)

    def dispatch_key_event(self, payload: dict[str, Any]) -> None:
        self.send("Input.dispatchKeyEvent", payload)

    def insert_text(self, text: str) -> None:
        self.send("Input.insertText", {"text": text})

    def create_target(self, url: str, background: bool = False) -> str:
        result = self.send("Target.createTarget", {"url": url, "background": bool(background)})
        return str(result.get("targetId") or "")

    def close_target(self, target_id: str) -> None:
        self.send("Target.closeTarget", {"targetId": target_id})

    def activate_target(self, target_id: str) -> None:
        if target_id:
            self.send("Target.activateTarget", {"targetId": target_id})

    def switch_target(self, target_id: str) -> bool:
        target_id = str(target_id or "").strip()
        if not target_id:
            return False
        if self.current_target_id() == target_id and self.is_connected:
            return False
        self.connect(target_id=target_id)
        return True

    def list_targets(self) -> list[dict[str, Any]]:
        payload = _http_json(f"http://127.0.0.1:{self.port}/json/list")
        return payload if isinstance(payload, list) else []

    def navigate(self, url: str) -> None:
        self.send("Page.navigate", {"url": url})
        with self._lock:
            self._target["url"] = url
            self._last_seen_at = _now_iso()

    def get_location(self) -> str:
        value = self.evaluate("location.href")
        if isinstance(value, str):
            with self._lock:
                self._target["url"] = value
        return str(value or "")

    def _mark_seen(self) -> None:
        with self._lock:
            self._last_seen_at = _now_iso()

    def _discover_target(self, target_id: str | None = None) -> dict[str, Any]:
        wanted_id = str(target_id or "").strip()
        for path in ("/json/list", "/json"):
            try:
                payload = _http_json(f"http://127.0.0.1:{self.port}{path}")
            except Exception:
                continue

            targets = payload if isinstance(payload, list) else payload.get("targets") if isinstance(payload, dict) else []
            if not isinstance(targets, list):
                continue
            candidates = []
            for item in targets:
                if not isinstance(item, dict):
                    continue
                if not item.get("webSocketDebuggerUrl"):
                    continue
                target_type = str(item.get("type") or "page").lower()
                if target_type not in {"page", "tab"}:
                    continue
                if str(item.get("url") or "").startswith("devtools://"):
                    continue
                candidates.append(item)
            if wanted_id:
                for item in candidates:
                    if str(item.get("id") or "") == wanted_id:
                        return item
                continue
            if candidates:
                return candidates[0]
        raise RuntimeError(f"{self.profile_id} 未找到调试页面")

    def _recv_loop(self) -> None:
        while True:
            with self._lock:
                ws = self._ws
                connected = self._connected
            if not ws or not connected:
                break
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            except Exception as exc:
                with self._lock:
                    self._last_error = str(exc)
                break
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue

            if isinstance(payload, dict) and "id" in payload:
                item = None
                with self._lock:
                    item = self._pending.pop(int(payload["id"]), None)
                if item:
                    try:
                        item.put_nowait(payload)
                    except Exception:
                        pass
                continue

            self._mark_seen()
            if self._event_handler and isinstance(payload, dict):
                try:
                    self._event_handler(payload)
                except Exception:
                    continue

        self.close()


class RuyiFirefoxPageClient:
    def __init__(
        self,
        profile_id: str,
        port: int,
        event_handler: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.profile_id = profile_id
        self.port = int(port)
        self._event_handler = event_handler
        self._lock = threading.RLock()
        self._command_lock = threading.RLock()
        self._page: Any = None
        self._browser: Any = None
        self._connected = False
        self._last_error = ""
        self._target: dict[str, Any] = {}
        self._last_seen_at: str | None = None
        self._pressed_button: int | None = None
        self._visual_target_id = ""
        self._visual_ready_at = 0.0

    @property
    def is_connected(self) -> bool:
        with self._lock:
            driver = getattr(self._browser, "driver", None)
            return bool(self._connected and self._page and driver and getattr(driver, "_is_running", True))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "profile_id": self.profile_id,
                "port": self.port,
                "connected": self.is_connected,
                "target_id": self._target.get("id") or "",
                "target_url": self._target.get("url") or "",
                "target_title": self._target.get("title") or "",
                "last_seen_at": self._last_seen_at,
                "last_error": self._last_error,
            }

    def current_target_id(self) -> str:
        with self._lock:
            if self._page is not None:
                return str(getattr(self._page, "tab_id", "") or self._target.get("id") or "")
            return str(self._target.get("id") or "")

    def connect(self, target_id: str | None = None) -> None:
        if FirefoxOptions is None or FirefoxPage is None:
            raise RuntimeError("Firefox 同步需要安装 ruyipage")
        self.close()
        opts = FirefoxOptions()
        opts.set_address(f"127.0.0.1:{self.port}")
        opts.existing_only(True)
        try:
            opts.set_retry(8, 0.35)
        except Exception:
            pass
        page = FirefoxPage(opts)
        browser = getattr(page, "_firefox", None)
        if browser is None:
            raise RuntimeError(f"{self.profile_id} Firefox BiDi 连接不可用")
        with self._lock:
            self._browser = browser
            self._page = page
            self._connected = True
            self._last_error = ""
        wanted = str(target_id or "").strip()
        if wanted:
            self.switch_target(wanted)
        else:
            self.sync_to_current_target()
        self._mark_seen()

    def close(self) -> None:
        with self._lock:
            browser = self._browser
            self._browser = None
            self._page = None
            self._connected = False
            self._visual_target_id = ""
            self._visual_ready_at = 0.0
        if not browser:
            return
        try:
            driver = getattr(browser, "driver", None)
            if driver:
                driver.stop()
        except Exception:
            pass
        try:
            from ruyipage._base.browser import Firefox
            from ruyipage._pages.firefox_page import FirefoxPage as RuyiFirefoxPage

            with Firefox._lock:
                Firefox._BROWSERS.pop(getattr(browser, "address", ""), None)
            RuyiFirefoxPage._PAGES.pop(getattr(browser, "address", ""), None)
            browser._initialized = False
        except Exception:
            pass

    def refresh_target(self) -> None:
        try:
            targets = self.list_targets()
        except Exception:
            return
        current = self.current_target_id()
        target = next((item for item in targets if item.get("id") == current), None)
        if not target and targets:
            target = targets[0]
        if target:
            with self._lock:
                previous_url = str(self._target.get("url") or "")
                next_url = str(target.get("url") or previous_url or "")
                self._target.update({
                    "id": target.get("id") or current,
                    "url": next_url,
                    "title": target.get("title") or self._target.get("title") or "",
                })
                if next_url != previous_url:
                    self._visual_target_id = ""
                    self._visual_ready_at = 0.0

    def sync_to_current_target(self) -> str:
        targets = self.list_targets()
        target_id = _active_target_id_from_targets(targets)
        if not target_id and targets:
            target_id = str(targets[0].get("id") or "")
        if target_id and target_id != self.current_target_id():
            self.switch_target(target_id)
        return target_id or self.current_target_id()

    def ensure_ready(self) -> None:
        if self.is_connected:
            return
        self.connect()

    def evaluate(self, expression: str) -> Any:
        last_error: Exception | None = None
        for attempt in range(2):
            self.ensure_ready()
            with self._lock:
                page = self._page
            if page is None:
                raise RuntimeError(f"{self.profile_id} Firefox 页面不可用")
            try:
                with self._command_lock:
                    result = page.run_js(expression, as_expr=True, timeout=SYNC_COMMAND_TIMEOUT)
                self._mark_seen()
                return result
            except Exception as exc:
                last_error = exc
                with self._lock:
                    self._last_error = str(exc)
                if attempt == 0 and _is_missing_browsing_context_error(exc):
                    self._recover_current_target()
                    continue
                raise
        if last_error:
            raise last_error
        raise RuntimeError(f"{self.profile_id} Firefox 页面脚本执行失败")

    def dispatch_mouse_event(self, payload: dict[str, Any], wait: bool = True) -> None:
        self.ensure_ready()
        event_type = str(payload.get("type") or "")
        x = int(round(float(payload.get("x") or 0)))
        y = int(round(float(payload.get("y") or 0)))
        with self._lock:
            page = self._page
        if page is None:
            return
        actions = page.actions
        if event_type == "mouseWheel":
            delta_x = int(round(float(payload.get("deltaX") or 0)))
            delta_y = int(round(float(payload.get("deltaY") or 0)))
            self._draw_visual("wheel", {"x": x, "y": y})
            self.evaluate(_build_smooth_wheel_expression({**payload, "deltaX": delta_x, "deltaY": delta_y, "x": x, "y": y}))
            return
        if event_type == "mouseMoved":
            self._draw_visual("move", {"x": x, "y": y})
            with self._command_lock:
                actions.move_to((x, y), duration=0).perform()
            return
        if event_type == "mousePressed":
            self._pressed_button = _button_index(str(payload.get("button") or "left"))
            return
        if event_type == "mouseReleased":
            button = self._pressed_button
            self._pressed_button = None
            self._draw_visual("click", {"x": x, "y": y})
            with self._command_lock:
                action = actions.move_to((x, y), duration=0)
                if button == 2:
                    action.right_click().perform()
                elif button == 1:
                    action.middle_click().perform()
                else:
                    action.click().perform()
            return

    def dispatch_key_event(self, payload: dict[str, Any]) -> None:
        key = str(payload.get("key") or payload.get("text") or payload.get("unmodifiedText") or "")
        if key:
            self.ensure_ready()
            with self._lock:
                page = self._page
            if page is not None:
                with self._command_lock:
                    page.actions.press(key).perform()

    def insert_text(self, text: str) -> None:
        self.ensure_ready()
        with self._lock:
            page = self._page
        if page is not None and text:
            with self._command_lock:
                page.actions.type(str(text)).perform()

    def create_target(self, url: str, background: bool = False) -> str:
        self.ensure_ready()
        with self._lock:
            browser = self._browser
        if browser is None:
            raise RuntimeError(f"{self.profile_id} Firefox 浏览器不可用")
        tab = browser.new_tab(url or None, background=bool(background))
        target_id = str(getattr(tab, "tab_id", "") or "")
        if not background and target_id:
            self.switch_target(target_id)
        return target_id

    def close_target(self, target_id: str) -> None:
        self.ensure_ready()
        target_id = str(target_id or "").strip()
        if not target_id:
            return
        with self._lock:
            browser = self._browser
        if browser is None:
            return
        browser.close_tabs(target_id)
        if self.current_target_id() == target_id:
            targets = self.list_targets()
            next_id = _active_target_id_from_targets(targets) or (str(targets[0].get("id") or "") if targets else "")
            if next_id:
                self.switch_target(next_id)

    def activate_target(self, target_id: str) -> None:
        self.ensure_ready()
        target_id = str(target_id or "").strip()
        if not target_id:
            return
        with self._lock:
            browser = self._browser
        if browser is None:
            return
        browser.activate_tab(target_id)
        self.switch_target(target_id)

    def switch_target(self, target_id: str) -> bool:
        target_id = str(target_id or "").strip()
        if not target_id:
            return False
        self.ensure_ready()
        if self.current_target_id() == target_id and self._page is not None:
            return False
        with self._lock:
            browser = self._browser
        if browser is None:
            return False
        tab = browser.get_tab(target_id)
        if not tab:
            return False
        with self._lock:
            self._page = tab
            self._target["id"] = target_id
            self._visual_target_id = ""
            self._visual_ready_at = 0.0
        self.refresh_target()
        return True

    def list_targets(self) -> list[dict[str, Any]]:
        self.ensure_ready()
        with self._lock:
            browser = self._browser
        if browser is None:
            return []
        result: list[dict[str, Any]] = []
        try:
            tab_ids = list(browser.tab_ids)
        except Exception:
            tab_ids = []
        for index, tab_id in enumerate(tab_ids):
            tab = None
            try:
                tab = browser.get_tab(tab_id)
            except Exception:
                tab = None
            url = ""
            title = ""
            active = False
            if tab is not None:
                try:
                    url = str(tab.url or "")
                except Exception:
                    url = ""
                try:
                    title = str(tab.title or "")
                except Exception:
                    title = ""
                try:
                    active = bool(tab.run_js("document.hasFocus()", as_expr=True, timeout=1))
                except Exception:
                    active = False
            result.append({
                "id": str(tab_id),
                "type": "page",
                "url": url,
                "title": title,
                "active": active,
                "index": index,
            })
        result.sort(key=lambda item: (0 if item.get("active") else 1, int(item.get("index") or 0)))
        return result

    def navigate(self, url: str) -> None:
        self.ensure_ready()
        with self._lock:
            page = self._page
        if page is None:
            raise RuntimeError(f"{self.profile_id} Firefox 页面不可用")
        with self._command_lock:
            page.get(url, wait="interactive", timeout=SYNC_COMMAND_TIMEOUT)
        with self._lock:
            self._target["url"] = url
            self._last_seen_at = _now_iso()
            self._visual_target_id = ""
            self._visual_ready_at = 0.0

    def get_location(self) -> str:
        value = self.evaluate("location.href")
        if isinstance(value, str):
            with self._lock:
                self._target["url"] = value
        return str(value or "")

    def drain_events(self) -> list[dict[str, Any]]:
        try:
            values = self.evaluate("""
(() => {
  if (typeof window.__oabSyncDrain !== 'function') return [];
  return window.__oabSyncDrain();
})()
""")
        except Exception:
            return []
        if not isinstance(values, list):
            return []
        result: list[dict[str, Any]] = []
        for item in values:
            event = _decode_sync_event(item)
            if event:
                result.append(event)
        return result

    def _mark_seen(self) -> None:
        with self._lock:
            self._last_seen_at = _now_iso()

    def _recover_current_target(self) -> None:
        with self._lock:
            browser = self._browser
            current_id = str(self._target.get("id") or "")
        if browser is None:
            self.connect()
            return

        targets: list[dict[str, Any]] = []
        try:
            targets = self.list_targets()
        except Exception:
            targets = []

        target_id = _active_target_id_from_targets(targets)
        if not target_id and current_id:
            if any(str(item.get("id") or "") == current_id for item in targets):
                target_id = current_id
        if not target_id and targets:
            target_id = str(targets[0].get("id") or "")

        if target_id:
            try:
                tab = browser.get_tab(target_id)
            except Exception:
                tab = None
            if tab is not None:
                with self._lock:
                    self._page = tab
                    self._target["id"] = target_id
                    self._connected = True
                    self._visual_target_id = ""
                    self._visual_ready_at = 0.0
                self.refresh_target()
                return

        with self._lock:
            self._connected = False
            self._page = None
            self._visual_target_id = ""
            self._visual_ready_at = 0.0
        self.connect()

    def _ensure_visual_overlay(self, force: bool = False) -> bool:
        target_id = self.current_target_id()
        now = time.monotonic()
        with self._lock:
            ready = self._visual_target_id == target_id and (now - self._visual_ready_at) < 2.0
        if ready and not force:
            return True
        self.evaluate(FOLLOWER_VISUAL_SCRIPT)
        with self._lock:
            self._visual_target_id = target_id
            self._visual_ready_at = now
        return True

    def _draw_visual(self, action: str, payload: dict[str, Any]) -> None:
        try:
            self._ensure_visual_overlay()
            data = json.dumps(payload, ensure_ascii=False)
            method = "move" if action == "move" else "click" if action == "click" else "wheel"
            self.evaluate(f"""
(() => {{
  const payload = {data};
  const api = window.__oabSyncVisual;
  if (!api || typeof api.{method} !== 'function') return false;
  return api.{method}(payload.x, payload.y);
}})()
""")
        except Exception:
            return


class _FollowerWorker:
    def __init__(
        self,
        follower_id: str,
        client_getter: Callable[[], Any | None],
        apply_handler: Callable[[Any, str, dict[str, Any]], None],
        error_handler: Callable[[str, Exception], None],
    ) -> None:
        self.follower_id = follower_id
        self._client_getter = client_getter
        self._apply_handler = apply_handler
        self._error_handler = error_handler
        self._items: deque[tuple[str, dict[str, Any]]] = deque()
        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        if not self._thread.is_alive():
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._condition:
            self._condition.notify_all()
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout=1.5)

    def submit(self, event_type: str, payload: dict[str, Any]) -> None:
        if self._stop_event.is_set():
            return
        item = (event_type, dict(payload))
        with self._condition:
            items = list(self._items)
            if event_type == "mouse_move":
                items = [existing for existing in items if existing[0] != "mouse_move"]
                items.append(item)
            elif event_type == "scroll":
                if payload.get("source") == "wheel_calibrate":
                    items = [existing for existing in items if existing[0] != "scroll"]
                else:
                    items = [existing for existing in items if existing[0] not in {"scroll", "wheel"}]
                items.append(item)
            elif event_type == "wheel":
                items = [existing for existing in items if existing[0] not in {"scroll", "mouse_move"}]
                for index in range(len(items) - 1, -1, -1):
                    if items[index][0] == "wheel":
                        items[index] = ("wheel", _merge_wheel_payload(items[index][1], payload))
                        break
                else:
                    items.append(item)
            else:
                items.append(item)
            while len(items) >= SYNC_WORKER_QUEUE_LIMIT:
                items.pop(0)
            self._items = deque(items)
            self._condition.notify()

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            with self._condition:
                if not self._items:
                    self._condition.wait(timeout=0.3)
                if not self._items:
                    continue
                event_type, payload = self._items.popleft()
            client = self._client_getter()
            if not client:
                continue
            try:
                self._apply_handler(client, event_type, payload)
            except Exception as exc:
                self._error_handler(self.follower_id, exc)


def _coerce_sync_options(options: dict[str, Any] | None = None) -> dict[str, bool]:
    payload = dict(options or {})
    defaults = {
        "sync_navigation": True,
        "sync_click": True,
        "sync_input": True,
        "sync_scroll": True,
        "sync_keyboard": True,
        "sync_mouse_move": False,
        "sync_current_url_on_start": True,
        "sync_browser_ui": True,
    }
    for key, value in defaults.items():
        payload[key] = bool(payload.get(key, value))
    return payload


class BrowserSynchronizer:
    def __init__(
        self,
        runtime_resolver: Callable[[str], dict[str, Any] | None],
        profile_resolver: Callable[[str], dict[str, Any] | None],
    ) -> None:
        self._runtime_resolver = runtime_resolver
        self._profile_resolver = profile_resolver
        self._lock = threading.RLock()
        self._session: _SyncSession | None = None

    def start(self, master_profile_id: str, follower_profile_ids: list[str], options: dict[str, Any] | None = None) -> dict[str, Any]:
        master_profile_id = str(master_profile_id or "").strip()
        if not master_profile_id:
            raise ValueError("请选择主浏览器")
        follower_ids = [str(item).strip() for item in follower_profile_ids if str(item).strip()]
        follower_ids = list(dict.fromkeys(item for item in follower_ids if item != master_profile_id))
        if not follower_ids:
            raise ValueError("请至少选择一个跟随浏览器")

        session = _SyncSession(
            runtime_resolver=self._runtime_resolver,
            profile_resolver=self._profile_resolver,
            master_profile_id=master_profile_id,
            follower_profile_ids=follower_ids,
            options=_coerce_sync_options(options),
        )
        session.start()
        with self._lock:
            if self._session:
                self._session.stop()
            self._session = session
        return session.snapshot()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            session = self._session
            self._session = None
        if session:
            session.stop()
        return self.status()

    def status(self) -> dict[str, Any]:
        with self._lock:
            session = self._session
        if not session:
            return {
                "running": False,
                "master_profile_id": None,
                "follower_profile_ids": [],
                "followers": [],
                "options": _coerce_sync_options(),
                "last_event": None,
                "last_error": "",
                "started_at": None,
            }
        if not session.is_running:
            with self._lock:
                if self._session is session:
                    self._session = None
            session.stop()
            return {
                "running": False,
                "master_profile_id": None,
                "follower_profile_ids": [],
                "followers": [],
                "options": _coerce_sync_options(),
                "last_event": session.last_event,
                "last_error": session.last_error,
                "started_at": None,
            }
        return session.snapshot()

    def navigate(self, url: str, include_master: bool = True) -> dict[str, Any]:
        url = str(url or "").strip()
        if not url:
            raise ValueError("请输入网址")
        with self._lock:
            session = self._session
        if not session or not session.is_running:
            raise RuntimeError("同步器还没有启动")
        session.navigate(url, include_master=include_master)
        return session.snapshot()

    def sync_master_url(self) -> dict[str, Any]:
        with self._lock:
            session = self._session
        if not session or not session.is_running:
            raise RuntimeError("同步器还没有启动")
        session.sync_master_url_to_followers()
        return session.snapshot()


class _SyncSession:
    def __init__(
        self,
        runtime_resolver: Callable[[str], dict[str, Any] | None],
        profile_resolver: Callable[[str], dict[str, Any] | None],
        master_profile_id: str,
        follower_profile_ids: list[str],
        options: dict[str, bool],
    ) -> None:
        self._runtime_resolver = runtime_resolver
        self._profile_resolver = profile_resolver
        self.master_profile_id = master_profile_id
        self.follower_profile_ids = follower_profile_ids
        self.options = options
        self.started_at = _now_iso()
        self.last_event: dict[str, Any] | None = None
        self.last_error = ""
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._master_poll_thread: threading.Thread | None = None
        self._master_client: Any | None = None
        self._follower_clients: dict[str, Any] = {}
        self._follower_workers: dict[str, _FollowerWorker] = {}
        self._master_target_ids: list[str] = []
        self._master_target_urls: dict[str, str] = {}
        self._master_active_target_id = ""
        self._recent_navigate_urls: deque[str] = deque(maxlen=6)
        self._last_click_event_at = 0.0
        self._deferred_new_target_ids: dict[str, bool] = {}

    @property
    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def start(self) -> None:
        self._ensure_clients(initial=True)
        self._install_master_script()
        self._refresh_master_target_snapshot()
        if self.options.get("sync_current_url_on_start"):
            self.sync_master_url_to_followers()
        self._start_master_poll_thread()
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._master_poll_thread and self._master_poll_thread.is_alive() and self._master_poll_thread is not threading.current_thread():
            self._master_poll_thread.join(timeout=1)
        if self._thread and self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)
        self._close_all_clients()

    def snapshot(self) -> dict[str, Any]:
        master_profile = self._profile_resolver(self.master_profile_id) or {}
        master_state = self._master_client.snapshot() if self._master_client else {
            "profile_id": self.master_profile_id,
            "port": None,
            "connected": False,
            "target_url": "",
            "target_title": "",
            "last_seen_at": None,
            "last_error": self.last_error,
        }
        followers = []
        for follower_id in self.follower_profile_ids:
            profile = self._profile_resolver(follower_id) or {}
            client = self._follower_clients.get(follower_id)
            state = client.snapshot() if client else {
                "profile_id": follower_id,
                "port": None,
                "connected": False,
                "target_url": "",
                "target_title": "",
                "last_seen_at": None,
                "last_error": "",
            }
            state.update({
                "name": profile.get("name") or follower_id[:8],
                "engine": profile.get("engine") or "",
                "status": profile.get("status") or "stopped",
            })
            followers.append(state)
        master_state.update({
            "name": master_profile.get("name") or self.master_profile_id[:8],
            "engine": master_profile.get("engine") or "",
            "status": master_profile.get("status") or "stopped",
        })
        return {
            "running": self.is_running,
            "started_at": self.started_at,
            "master_profile_id": self.master_profile_id,
            "master": master_state,
            "follower_profile_ids": list(self.follower_profile_ids),
            "followers": followers,
            "follower_count": len(self.follower_profile_ids),
            "connected_followers": sum(1 for item in followers if item.get("connected")),
            "options": dict(self.options),
            "last_event": self.last_event,
            "last_error": self.last_error,
        }

    def navigate(self, url: str, include_master: bool = True) -> None:
        self._ensure_clients(initial=False)
        if include_master and self._master_client:
            self._master_client.navigate(url)
        for follower_id in self.follower_profile_ids:
            client = self._follower_clients.get(follower_id)
            if not client:
                continue
            try:
                client.navigate(url)
            except Exception as exc:
                self._set_error(f"{self._profile_label(follower_id)} 打开网址失败：{exc}")
        self._record_event("manual_navigate", {"url": url})

    def sync_master_url_to_followers(self) -> None:
        self._ensure_clients(initial=False)
        if not self._master_client:
            raise RuntimeError("主浏览器不可用")
        url = self._master_client.get_location()
        if not url:
            raise RuntimeError("主浏览器当前标签页没有可同步的网址")
        for follower_id in self.follower_profile_ids:
            client = self._follower_clients.get(follower_id)
            if not client:
                continue
            try:
                client.navigate(url)
            except Exception as exc:
                self._set_error(f"{self._profile_label(follower_id)} 同步网址失败：{exc}")
        self._record_event("sync_current_url", {"url": url})

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.wait(SYNC_HEARTBEAT_SECONDS):
            try:
                self._ensure_clients(initial=False)
                if self._master_client:
                    self._sync_master_current_target()
                    self._master_client.refresh_target()
                self._install_master_script()
                self._drain_master_poll_events()
                self._sync_browser_ui_changes()
                for client in self._follower_clients.values():
                    client.refresh_target()
            except Exception as exc:
                self._set_error(str(exc))
                if not self._runtime_resolver(self.master_profile_id):
                    self._stop_event.set()
                    break
        self._close_all_clients()

    def _ensure_clients(self, initial: bool) -> None:
        master_runtime = self._runtime_resolver(self.master_profile_id)
        if not master_runtime or not master_runtime.get("remote_debugging_port"):
            raise RuntimeError("主浏览器还没有启动，无法开启同步器")
        self._master_client = self._ensure_client(
            existing=self._master_client,
            profile_id=self.master_profile_id,
            runtime=master_runtime,
            is_master=True,
        )

        for follower_id in list(self.follower_profile_ids):
            runtime = self._runtime_resolver(follower_id)
            if not runtime or not runtime.get("remote_debugging_port"):
                if initial:
                    raise RuntimeError(f"跟随浏览器未启动：{self._profile_label(follower_id)}")
                self._close_follower_client(follower_id)
                continue
            self._follower_clients[follower_id] = self._ensure_client(
                existing=self._follower_clients.get(follower_id),
                profile_id=follower_id,
                runtime=runtime,
                is_master=False,
            )
            self._ensure_follower_worker(follower_id)

    def _ensure_client(
        self,
        existing: Any | None,
        profile_id: str,
        runtime: dict[str, Any],
        is_master: bool,
    ) -> Any:
        port = int(runtime.get("remote_debugging_port") or 0)
        if not port:
            raise RuntimeError(f"{self._profile_label(profile_id)} 没有可用的调试端口")
        engine = str(runtime.get("engine") or (self._profile_resolver(profile_id) or {}).get("engine") or "chrome").strip().lower()
        client_cls = RuyiFirefoxPageClient if engine == "firefox" else CdpPageClient
        if existing and isinstance(existing, client_cls) and existing.port == port and existing.is_connected:
            return existing
        if existing:
            existing.close()
        client = client_cls(
            profile_id=profile_id,
            port=port,
            event_handler=self._handle_master_event if is_master else None,
        )
        client.connect()
        return client

    def _ensure_follower_worker(self, follower_id: str) -> None:
        worker = self._follower_workers.get(follower_id)
        if worker:
            return
        worker = _FollowerWorker(
            follower_id=follower_id,
            client_getter=lambda profile_id=follower_id: self._follower_clients.get(profile_id),
            apply_handler=self._apply_event_to_follower,
            error_handler=self._handle_worker_error,
        )
        self._follower_workers[follower_id] = worker
        worker.start()

    def _start_master_poll_thread(self) -> None:
        if not self._master_client or not hasattr(self._master_client, "drain_events"):
            return
        if self._master_poll_thread and self._master_poll_thread.is_alive():
            return
        self._master_poll_thread = threading.Thread(target=self._drain_master_poll_loop, daemon=True)
        self._master_poll_thread.start()

    def _drain_master_poll_loop(self) -> None:
        while not self._stop_event.wait(SYNC_FIREFOX_POLL_SECONDS):
            self._drain_master_poll_events()

    def _sync_master_current_target(self) -> None:
        if not self._master_client or not isinstance(self._master_client, RuyiFirefoxPageClient):
            return
        try:
            self._master_client.sync_to_current_target()
        except Exception as exc:
            if not _is_missing_browsing_context_error(exc):
                self._set_error(f"同步主窗口标签页失败：{exc}")

    def _install_master_script(self) -> None:
        if not self._master_client:
            return
        self._sync_master_current_target()
        try:
            self._master_client.send("Runtime.addBinding", {"name": "__oabSyncBinding"})
        except Exception:
            pass
        for attempt in range(2):
            try:
                self._master_client.evaluate(MASTER_INJECT_SCRIPT)
                if self.last_error.startswith("同步脚本注入失败"):
                    self._set_error("")
                return
            except Exception as exc:
                if attempt == 0 and _is_missing_browsing_context_error(exc):
                    self._sync_master_current_target()
                    continue
                self._set_error(f"同步脚本注入失败：{exc}")
                return

    def _drain_master_poll_events(self) -> None:
        if not self._master_client or not hasattr(self._master_client, "drain_events"):
            return
        try:
            events = self._master_client.drain_events()
        except Exception as exc:
            self._set_error(f"读取同步事件失败：{exc}")
            return
        for event in events:
            self._dispatch_master_event(event)

    def _refresh_master_target_snapshot(self) -> None:
        targets = self._list_master_targets()
        self._master_target_ids = [str(item.get("id") or "") for item in targets if item.get("id")]
        self._master_target_urls = {
            str(item.get("id") or ""): str(item.get("url") or "")
            for item in targets
            if item.get("id")
        }
        self._master_active_target_id = self._master_target_ids[0] if self._master_target_ids else ""

    def _list_master_targets(self) -> list[dict[str, Any]]:
        if not self._master_client:
            return []
        try:
            targets = self._master_client.list_targets()
        except Exception as exc:
            self._set_error(f"读取主窗口标签页失败：{exc}")
            return []
        result: list[dict[str, Any]] = []
        for item in targets:
            if not isinstance(item, dict):
                continue
            target_type = str(item.get("type") or "").lower()
            if target_type not in {"page", "tab"}:
                continue
            target_id = str(item.get("id") or "")
            if not target_id:
                continue
            result.append(item)
        return result

    def _sync_browser_ui_changes(self) -> None:
        targets = self._list_master_targets()
        if not targets:
            return
        previous_ids = list(self._master_target_ids)
        previous_urls = dict(self._master_target_urls)
        previous_active_target_id = str(self._master_active_target_id or "")
        current_ids = [str(item.get("id") or "") for item in targets if item.get("id")]
        current_urls = {
            str(item.get("id") or ""): str(item.get("url") or "")
            for item in targets
            if item.get("id")
        }
        new_ids = [target_id for target_id in current_ids if target_id and target_id not in previous_ids]
        active_target_id = _active_target_id_from_targets(targets)
        if not active_target_id and new_ids:
            active_target_id = new_ids[-1]
        if not active_target_id:
            active_target_id = current_ids[0] if current_ids else ""

        resolved_deferred_target_ids: set[str] = set()
        if self.options.get("sync_browser_ui"):
            for target_id in new_ids:
                target_url = current_urls.get(target_id, "")
                if self._should_defer_new_tab(target_url):
                    self._deferred_new_target_ids[target_id] = target_id == active_target_id
                    continue
                self._broadcast_browser_ui_event("browser_new_tab", {
                    "url": target_url,
                    "activate": target_id == active_target_id,
                })
            if new_ids:
                first_new_id = new_ids[0]
                self._record_event("browser_new_tab", {"url": current_urls.get(first_new_id, ""), "count": len(new_ids)})

            closed_ids = [target_id for target_id in previous_ids if target_id and target_id not in current_ids]
            for _ in closed_ids:
                self._broadcast_browser_ui_event("browser_close_current", {})
            if closed_ids:
                self._record_event("browser_close_current", {"count": len(closed_ids)})

            if previous_active_target_id and active_target_id and active_target_id != previous_active_target_id and active_target_id not in new_ids:
                active_url = current_urls.get(active_target_id, "")
                self._broadcast_browser_ui_event("browser_activate_tab", {"url": active_url})
                self._record_event("browser_activate_tab", {"url": active_url})

            for target_id in list(self._deferred_new_target_ids.keys()):
                if target_id not in current_ids:
                    self._deferred_new_target_ids.pop(target_id, None)
                    continue
                target_url = current_urls.get(target_id, "")
                if not _should_sync_browser_url(target_url):
                    continue
                activate = bool(self._deferred_new_target_ids.pop(target_id, False)) or target_id == active_target_id
                self._broadcast_browser_ui_event("browser_new_tab", {
                    "url": target_url,
                    "activate": activate,
                })
                self._record_event("browser_new_tab", {"url": target_url, "count": 1})
                resolved_deferred_target_ids.add(target_id)

        for target_id, url in current_urls.items():
            previous_url = previous_urls.get(target_id, "")
            if url == previous_url:
                continue
            if target_id in new_ids:
                continue
            if target_id in self._deferred_new_target_ids:
                continue
            if target_id in resolved_deferred_target_ids:
                continue
            if _should_sync_browser_url(url):
                self._broadcast_navigation(url)

        self._master_target_ids = current_ids
        self._master_target_urls = current_urls
        if active_target_id and active_target_id != previous_active_target_id:
            if self._switch_master_target(active_target_id):
                self._install_master_script()
        self._master_active_target_id = active_target_id

    def _switch_master_target(self, target_id: str) -> bool:
        if not self._master_client:
            return False
        try:
            return self._master_client.switch_target(target_id)
        except Exception as exc:
            self._set_error(f"切换主控标签页失败：{exc}")
            return False

    def _should_defer_new_tab(self, url: str) -> bool:
        if _should_sync_browser_url(url):
            return False
        if not self._last_click_event_at:
            return False
        return (time.monotonic() - self._last_click_event_at) <= SYNC_CLICK_NEW_TAB_DEFER_SECONDS

    def _broadcast_browser_ui_event(self, event_type: str, payload: dict[str, Any]) -> None:
        for follower_id in self.follower_profile_ids:
            client = self._follower_clients.get(follower_id)
            if not client:
                continue
            worker = self._follower_workers.get(follower_id)
            if worker:
                worker.submit(event_type, payload)
                continue
            try:
                self._apply_event_to_follower(client, event_type, payload)
            except Exception as exc:
                self._handle_worker_error(follower_id, exc)

    def _broadcast_navigation(self, url: str) -> None:
        normalized = str(url or "").strip()
        if not normalized:
            return
        if normalized in self._recent_navigate_urls:
            return
        self._recent_navigate_urls.append(normalized)
        self._dispatch_master_event({
            "type": "navigate",
            "payload": {"url": normalized},
        })

    def _handle_master_event(self, payload: dict[str, Any]) -> None:
        method = str(payload.get("method") or "")
        if method == "Runtime.bindingCalled":
            self._handle_binding_event(payload.get("params") or {})
            return
        if method == "Runtime.consoleAPICalled":
            self._handle_console_event(payload.get("params") or {})
            return
        if method == "Page.frameNavigated":
            params = payload.get("params") or {}
            frame = params.get("frame") or {}
            if isinstance(frame, dict) and not frame.get("parentId"):
                url = str(frame.get("url") or "").strip()
                if url and _should_sync_browser_url(url):
                    self._broadcast_navigation(url)
            return
        if method == "Page.navigatedWithinDocument":
            params = payload.get("params") or {}
            url = str(params.get("url") or "").strip()
            if url and _should_sync_browser_url(url):
                self._broadcast_navigation(url)
            return
        if method == "Page.loadEventFired":
            return

    def _handle_binding_event(self, params: dict[str, Any]) -> None:
        if str(params.get("name") or "") != "__oabSyncBinding":
            return
        event = _decode_sync_event(params.get("payload"))
        if event:
            self._dispatch_master_event(event)

    def _handle_console_event(self, params: dict[str, Any]) -> None:
        args = params.get("args") or []
        if not args:
            return
        first = args[0]
        if not isinstance(first, dict):
            return
        event = _decode_sync_event(first.get("value"))
        if event:
            self._dispatch_master_event(event)

    def _dispatch_master_event(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("type") or "").strip().lower()
        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        page_url = str(event.get("href") or "").strip()
        if page_url:
            payload = {**payload, "page_url": page_url}

        option_map = {
            "navigate": "sync_navigation",
            "click": "sync_click",
            "input": "sync_input",
            "change": "sync_input",
            "wheel": "sync_scroll",
            "scroll": "sync_scroll",
            "keydown": "sync_keyboard",
            "mouse_move": "sync_mouse_move",
        }
        option_key = option_map.get(event_type)
        if option_key and not self.options.get(option_key):
            return

        if event_type == "click":
            self._last_click_event_at = time.monotonic()

        for follower_id in self.follower_profile_ids:
            client = self._follower_clients.get(follower_id)
            if not client:
                continue
            worker = self._follower_workers.get(follower_id)
            if worker:
                worker.submit(event_type, payload)
            else:
                try:
                    self._apply_event_to_follower(client, event_type, payload)
                except Exception as exc:
                    self._handle_worker_error(follower_id, exc)
        self._record_event(event_type, payload)

    def _apply_event_to_follower(self, client: Any, event_type: str, payload: dict[str, Any]) -> None:
        if event_type == "browser_new_tab":
            self._open_follower_tab(client, payload)
            return
        if event_type == "browser_activate_tab":
            self._activate_follower_tab(client, payload)
            return
        if event_type == "browser_close_current":
            current_id = ""
            try:
                current_id = client.sync_to_current_target()
            except Exception:
                current_id = client.current_target_id()
            if not current_id:
                targets = client.list_targets()
                for item in targets:
                    if not isinstance(item, dict):
                        continue
                    target_id = str(item.get("id") or "")
                    target_type = str(item.get("type") or "").lower()
                    if target_id and target_type in {"page", "tab"}:
                        current_id = target_id
                        break
            if current_id:
                client.close_target(current_id)
            return

        self._align_follower_target_for_payload(client, payload)
        if event_type == "navigate":
            url = str(payload.get("url") or "").strip()
            if url:
                client.navigate(url)
            return
        if event_type == "wheel":
            client.dispatch_mouse_event(_build_wheel_payload(payload), wait=False)
            return
        if event_type == "mouse_move":
            client.dispatch_mouse_event(_build_mouse_move_payload(payload), wait=False)
            return
        if event_type == "click":
            self._sleep_for_sync_delay("click")
            point = client.evaluate(_resolve_click_point_expression(payload))
            if not isinstance(point, dict) or not point.get("ok"):
                return
            for event_payload in _build_click_mouse_events({**payload, **point}):
                wait = event_payload.get("type") == "mouseReleased"
                client.dispatch_mouse_event(event_payload, wait=wait)
            return
        if event_type in {"input", "change"}:
            self._sleep_for_sync_delay("input")
            client.evaluate(_build_input_expression(payload))
            return
        if event_type == "scroll":
            client.evaluate(_build_scroll_expression(payload))
            return
        if event_type == "keydown":
            client.evaluate(_build_key_expression(payload))
            return

    def _align_follower_target_for_payload(self, client: Any, payload: dict[str, Any]) -> None:
        if not self.options.get("sync_browser_ui"):
            return
        page_url = str(payload.get("page_url") or "").strip()
        if not page_url:
            return
        target_id = self._find_matching_target_id(client, page_url)
        if not target_id:
            return
        if target_id == client.current_target_id():
            return
        try:
            client.activate_target(target_id)
        except Exception:
            pass
        client.switch_target(target_id)

    def _open_follower_tab(self, client: Any, payload: dict[str, Any]) -> None:
        requested_url = str(payload.get("url") or "").strip()
        if _should_sync_browser_url(requested_url):
            existing_target_id = self._find_matching_target_id(client, requested_url)
            if existing_target_id:
                if bool(payload.get("activate", True)):
                    try:
                        client.activate_target(existing_target_id)
                    except Exception:
                        pass
                    client.switch_target(existing_target_id)
                return
        create_url = self._new_tab_url_for_profile(client.profile_id, requested_url)
        activate = bool(payload.get("activate", True))
        try:
            target_id = client.create_target(create_url, background=not activate)
        except Exception:
            if create_url == "about:blank":
                raise
            target_id = client.create_target("about:blank", background=not activate)
        if activate and target_id:
            try:
                client.activate_target(target_id)
            except Exception:
                pass
            client.switch_target(target_id)

    def _activate_follower_tab(self, client: Any, payload: dict[str, Any]) -> None:
        target_id = self._find_matching_target_id(client, str(payload.get("url") or "").strip())
        if not target_id:
            return
        try:
            client.activate_target(target_id)
        except Exception:
            pass
        client.switch_target(target_id)

    def _new_tab_url_for_profile(self, profile_id: str, raw_url: str) -> str:
        url = str(raw_url or "").strip()
        if _should_sync_browser_url(url):
            return url
        profile = self._profile_resolver(profile_id) or {}
        engine = str(profile.get("engine") or "").strip().lower()
        if engine == "firefox":
            return "about:newtab"
        if engine == "chrome":
            return "chrome://newtab/"
        return url or "about:blank"

    def _find_matching_target_id(self, client: Any, target_url: str) -> str:
        normalized_url = str(target_url or "").strip()
        if not normalized_url:
            return ""
        candidates = []
        try:
            candidates = client.list_targets()
        except Exception:
            return ""
        target_is_blank = _is_browser_blank_url(normalized_url)
        for item in candidates:
            if not isinstance(item, dict):
                continue
            target_id = str(item.get("id") or "")
            target_type = str(item.get("type") or "").lower()
            if not target_id or target_type not in {"page", "tab"}:
                continue
            candidate_url = str(item.get("url") or "").strip()
            if target_is_blank:
                if _is_browser_blank_url(candidate_url):
                    return target_id
                continue
            if candidate_url == normalized_url:
                return target_id
        return ""

    def _sleep_for_sync_delay(self, delay_type: str) -> None:
        if delay_type == "click":
            enabled = bool(self.options.get("delay_click_enabled"))
            minimum = int(self.options.get("delay_click_min_ms") or 0)
            maximum = int(self.options.get("delay_click_max_ms") or minimum)
        else:
            enabled = bool(self.options.get("delay_input_enabled"))
            minimum = int(self.options.get("delay_input_min_ms") or 0)
            maximum = int(self.options.get("delay_input_max_ms") or minimum)
        if not enabled:
            return
        minimum, maximum = sorted((max(0, minimum), max(0, maximum)))
        delay_ms = random.randint(minimum, maximum) if maximum > minimum else minimum
        if delay_ms > 0:
            time.sleep(delay_ms / 1000)

    def _handle_worker_error(self, follower_id: str, exc: Exception) -> None:
        self._set_error(f"{self._profile_label(follower_id)} 同步失败：{exc}")
        client = self._follower_clients.get(follower_id)
        if client:
            client.close()

    def _record_event(self, event_type: str, payload: dict[str, Any]) -> None:
        summary = ""
        if event_type in {"navigate", "manual_navigate", "sync_current_url"}:
            summary = str(payload.get("url") or "")
        elif event_type in {"click", "input", "change"}:
            summary = str(payload.get("selector") or "") or event_type
        elif event_type == "wheel":
            summary = f"ΔY {int(round(float(payload.get('deltaY') or 0)))}"
        elif event_type == "scroll":
            if payload.get("mode") == "element":
                summary = f"{int(payload.get('scrollLeft') or 0)}, {int(payload.get('scrollTop') or 0)}"
            else:
                summary = f"{int(payload.get('x') or 0)}, {int(payload.get('y') or 0)}"
        elif event_type == "keydown":
            summary = str(payload.get("key") or "")
        elif event_type == "browser_close_current":
            summary = f"已关闭 {int(payload.get('count') or 1)} 个标签页"
        self.last_event = {
            "type": event_type,
            "summary": summary,
            "at": _now_iso(),
        }

    def _set_error(self, message: str) -> None:
        self.last_error = str(message or "").strip()

    def _close_follower_client(self, follower_id: str) -> None:
        worker = self._follower_workers.pop(follower_id, None)
        if worker:
            worker.stop()
        client = self._follower_clients.pop(follower_id, None)
        if client:
            client.close()

    def _close_all_clients(self) -> None:
        if self._master_client:
            self._master_client.close()
            self._master_client = None
        for worker in list(self._follower_workers.values()):
            worker.stop()
        self._follower_workers = {}
        for client in list(self._follower_clients.values()):
            client.close()
        self._follower_clients = {}

    def _profile_label(self, profile_id: str) -> str:
        payload = self._profile_resolver(profile_id) or {}
        return str(payload.get("name") or profile_id[:8])


def _decode_sync_event(raw_payload: Any) -> dict[str, Any] | None:
    if isinstance(raw_payload, dict):
        return raw_payload
    if not isinstance(raw_payload, str):
        return None
    value = raw_payload.strip()
    if not value:
        return None
    if value.startswith(SYNC_EVENT_PREFIX):
        value = value[len(SYNC_EVENT_PREFIX):]
    try:
        event = json.loads(value)
    except Exception:
        return None
    if not isinstance(event, dict):
        return None
    return event


def _should_sync_browser_url(url: str) -> bool:
    value = str(url or "").strip()
    if not value:
        return False
    if value.startswith("devtools://"):
        return False
    if _is_browser_blank_url(value):
        return False
    return True


def _is_browser_blank_url(url: str) -> bool:
    value = str(url or "").strip().lower()
    return value in {"about:blank", "about:newtab", "chrome://newtab/", "chrome://new-tab-page/"}


def _is_missing_browsing_context_error(exc: Exception | str) -> bool:
    value = str(exc or "").strip().lower()
    if not value:
        return False
    return (
        "no such frame" in value
        or ("browsing context" in value and "not found" in value)
        or ("browsingcontext" in value and "not found" in value)
        or ("context" in value and "discarded" in value)
    )


def _active_target_id_from_targets(targets: list[dict[str, Any]]) -> str:
    for item in targets:
        if isinstance(item, dict) and bool(item.get("active")):
            return str(item.get("id") or "")
    return ""


def _mouse_modifiers(payload: dict[str, Any]) -> int:
    modifiers = 0
    if payload.get("altKey"):
        modifiers |= 1
    if payload.get("ctrlKey"):
        modifiers |= 2
    if payload.get("metaKey"):
        modifiers |= 4
    if payload.get("shiftKey"):
        modifiers |= 8
    return modifiers


def _normalize_wheel_delta(delta: float, delta_mode: int) -> float:
    if delta_mode == 1:
        return delta * 40
    if delta_mode == 2:
        return delta * 700
    return delta


def _build_wheel_payload(payload: dict[str, Any]) -> dict[str, Any]:
    x = int(round(float(payload.get("x") or 0)))
    y = int(round(float(payload.get("y") or 0)))
    return {
        "type": "mouseWheel",
        "x": x,
        "y": y,
        "deltaX": float(_normalize_wheel_delta(float(payload.get("deltaX") or 0), int(payload.get("deltaMode") or 0))),
        "deltaY": float(_normalize_wheel_delta(float(payload.get("deltaY") or 0), int(payload.get("deltaMode") or 0))),
        "modifiers": _mouse_modifiers(payload),
        "pointerType": "mouse",
    }


def _build_smooth_wheel_expression(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return f"""
(() => {{
  const payload = {data};
  const clamp = (value, max) => Math.max(0, Math.min(Math.round(Number(value || 0)), Math.max(0, max - 1)));
  const x = clamp(payload.x, window.innerWidth);
  const y = clamp(payload.y, window.innerHeight);
  const deltaX = Number(payload.deltaX || 0);
  const deltaY = Number(payload.deltaY || 0);
  const canScroll = (node) => {{
    if (!node || node.nodeType !== 1) return false;
    const style = window.getComputedStyle(node);
    const overflowY = style.overflowY || '';
    const overflowX = style.overflowX || '';
    const scrollY = /(auto|scroll|overlay)/.test(overflowY) && node.scrollHeight > node.clientHeight + 1;
    const scrollX = /(auto|scroll|overlay)/.test(overflowX) && node.scrollWidth > node.clientWidth + 1;
    return scrollY || scrollX;
  }};
  const findTarget = () => {{
    let node = document.elementFromPoint(x, y);
    while (node && node !== document.body && node !== document.documentElement) {{
      if (canScroll(node)) return node;
      node = node.parentElement;
    }}
    return window;
  }};
  const scrollTarget = findTarget();
  const state = window.__oabSmoothWheel || (window.__oabSmoothWheel = {{
    dx: 0,
    dy: 0,
    target: window,
    running: false,
    frame: 0,
  }});
  state.dx += deltaX;
  state.dy += deltaY;
  state.target = scrollTarget;

  const applyScroll = (target, dx, dy) => {{
    if (target === window) {{
      window.scrollBy({{ left: dx, top: dy, behavior: 'auto' }});
      return;
    }}
    target.scrollLeft += dx;
    target.scrollTop += dy;
  }};

  const step = () => {{
    const absX = Math.abs(state.dx);
    const absY = Math.abs(state.dy);
    if (absX < 0.5 && absY < 0.5) {{
      state.dx = 0;
      state.dy = 0;
      state.running = false;
      state.frame = 0;
      return;
    }}
    const partX = absX < 1 ? state.dx : state.dx * 0.42;
    const partY = absY < 1 ? state.dy : state.dy * 0.42;
    state.dx -= partX;
    state.dy -= partY;
    applyScroll(state.target || window, partX, partY);
    state.frame = requestAnimationFrame(step);
  }};

  try {{
    const wheelTarget = scrollTarget === window ? (document.elementFromPoint(x, y) || document.body) : scrollTarget;
    wheelTarget.dispatchEvent(new WheelEvent('wheel', {{
      bubbles: true,
      cancelable: true,
      ruyi: true,
      clientX: x,
      clientY: y,
      deltaX,
      deltaY,
      deltaMode: 0,
      ctrlKey: !!payload.ctrlKey,
      shiftKey: !!payload.shiftKey,
      altKey: !!payload.altKey,
      metaKey: !!payload.metaKey,
    }}));
  }} catch (error) {{
    // ignore
  }}

  if (!state.running) {{
    state.running = true;
    state.frame = requestAnimationFrame(step);
  }}
  return true;
}})()
"""


def _build_mouse_move_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "mouseMoved",
        "x": int(round(float(payload.get("x") or 0))),
        "y": int(round(float(payload.get("y") or 0))),
        "modifiers": _mouse_modifiers(payload),
        "button": "none",
        "buttons": 0,
        "pointerType": "mouse",
    }


def _button_name(button: int) -> str:
    mapping = {
        1: "middle",
        2: "right",
    }
    return mapping.get(int(button or 0), "left")


def _button_index(button: str) -> int:
    mapping = {
        "middle": 1,
        "right": 2,
    }
    return mapping.get(str(button or "left").strip().lower(), 0)


def _button_mask(button: int) -> int:
    mapping = {
        1: 4,
        2: 2,
    }
    return mapping.get(int(button or 0), 1)


def _build_click_mouse_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    x = int(round(float(payload.get("x") or 0)))
    y = int(round(float(payload.get("y") or 0)))
    button = int(payload.get("button") or 0)
    button_name = _button_name(button)
    button_mask = _button_mask(button)
    base = {
        "x": x,
        "y": y,
        "modifiers": _mouse_modifiers(payload),
        "pointerType": "mouse",
    }
    return [
        {
            **base,
            "type": "mouseMoved",
            "button": "none",
            "buttons": 0,
        },
        {
            **base,
            "type": "mousePressed",
            "button": button_name,
            "buttons": button_mask,
            "clickCount": 1,
        },
        {
            **base,
            "type": "mouseReleased",
            "button": button_name,
            "buttons": 0,
            "clickCount": 1,
        },
    ]


def _merge_wheel_payload(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    merged["deltaX"] = float(base.get("deltaX") or 0) + float(incoming.get("deltaX") or 0)
    merged["deltaY"] = float(base.get("deltaY") or 0) + float(incoming.get("deltaY") or 0)
    for key in ("x", "y", "rx", "ry", "deltaMode", "ctrlKey", "shiftKey", "altKey", "metaKey"):
        if key in incoming:
            merged[key] = incoming.get(key)
    return merged


def _resolve_click_point_expression(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return f"""
(() => {{
  const payload = {data};
  const clampPoint = (point) => ({{
    x: Math.max(1, Math.min(window.innerWidth - 1, Math.round(Number(point.x || 0)))),
    y: Math.max(1, Math.min(window.innerHeight - 1, Math.round(Number(point.y || 0)))),
  }});
  const pickTarget = () => {{
    let target = null;
    if (payload.selector) {{
      try {{
        target = document.querySelector(payload.selector);
      }} catch (error) {{
        target = null;
      }}
    }}
    if (target) return target;
    const fallback = clampPoint({{
      x: Number.isFinite(Number(payload.x)) ? Number(payload.x) : Number(payload.rx || 0) * window.innerWidth,
      y: Number.isFinite(Number(payload.y)) ? Number(payload.y) : Number(payload.ry || 0) * window.innerHeight,
    }});
    return document.elementFromPoint(fallback.x, fallback.y);
  }};
  const target = pickTarget();
  if (!target) return {{ ok: false }};
  target.focus?.();
  const rect = target.getBoundingClientRect();
  if (!rect || rect.width < 1 || rect.height < 1) {{
    const fallback = clampPoint({{
      x: Number.isFinite(Number(payload.x)) ? Number(payload.x) : Number(payload.rx || 0) * window.innerWidth,
      y: Number.isFinite(Number(payload.y)) ? Number(payload.y) : Number(payload.ry || 0) * window.innerHeight,
    }});
    return {{ ok: true, x: fallback.x, y: fallback.y }};
  }}
  const x = rect.left + Math.min(Math.max(1, rect.width / 2), Math.max(1, rect.width - 1));
  const y = rect.top + Math.min(Math.max(1, rect.height / 2), Math.max(1, rect.height - 1));
  const point = clampPoint({{ x, y }});
  return {{ ok: true, x: point.x, y: point.y }};
}})()
"""


def _build_click_expression(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return f"""
(() => {{
  const payload = {data};
  const pickBySelector = () => {{
    if (!payload.selector) return null;
    try {{
      return document.querySelector(payload.selector);
    }} catch (error) {{
      return null;
    }}
  }};
  let target = pickBySelector();
  if (!target) {{
    const x = Math.max(0, Math.min(window.innerWidth - 1, Math.round(Number(payload.rx || 0) * window.innerWidth)));
    const y = Math.max(0, Math.min(window.innerHeight - 1, Math.round(Number(payload.ry || 0) * window.innerHeight)));
    target = document.elementFromPoint(x, y);
  }}
  if (!target) return false;
  target.focus?.();
  const rect = target.getBoundingClientRect();
  const clientX = rect.left + Math.max(1, rect.width / 2);
  const clientY = rect.top + Math.max(1, rect.height / 2);
  const init = {{
    bubbles: true,
    cancelable: true,
    composed: true,
    ruyi: true,
    view: window,
    clientX,
    clientY,
    button: Number(payload.button || 0),
    ctrlKey: !!payload.ctrlKey,
    shiftKey: !!payload.shiftKey,
    altKey: !!payload.altKey,
    metaKey: !!payload.metaKey,
  }};
  for (const eventName of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {{
    target.dispatchEvent(new MouseEvent(eventName, init));
  }}
  if (typeof target.click === 'function') {{
    target.click();
  }}
  return true;
}})()
"""


def _build_input_expression(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return f"""
(() => {{
  const payload = {data};
  if (!payload.selector) return false;
  let target = null;
  try {{
    target = document.querySelector(payload.selector);
  }} catch (error) {{
    target = null;
  }}
  if (!target) return false;
  target.focus?.();
  if (payload.tag === 'select') {{
    target.value = payload.value ?? '';
    target.dispatchEvent(new Event('change', {{ bubbles: true, ruyi: true }}));
    return true;
  }}
  if (payload.inputType === 'checkbox' || payload.inputType === 'radio') {{
    target.checked = !!payload.checked;
    try {{
      target.dispatchEvent(new InputEvent('input', {{ bubbles: true, data: null, inputType: 'insertReplacementText', ruyi: true }}));
    }} catch (error) {{
      target.dispatchEvent(new Event('input', {{ bubbles: true, ruyi: true }}));
    }}
    target.dispatchEvent(new Event('change', {{ bubbles: true, ruyi: true }}));
    return true;
  }}
  if (target.isContentEditable) {{
    target.innerText = payload.value ?? '';
  }} else if ('value' in target) {{
    target.value = payload.value ?? '';
  }} else {{
    return false;
  }}
  try {{
    target.dispatchEvent(new InputEvent('input', {{ bubbles: true, data: payload.value ?? '', inputType: 'insertReplacementText', ruyi: true }}));
  }} catch (error) {{
    target.dispatchEvent(new Event('input', {{ bubbles: true, ruyi: true }}));
  }}
  target.dispatchEvent(new Event('change', {{ bubbles: true, ruyi: true }}));
  return true;
}})()
"""


def _build_scroll_expression(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return f"""
(() => {{
  const payload = {data};
  const pickTarget = () => {{
    if (payload.mode === 'element' && payload.selector) {{
      try {{
        const target = document.querySelector(payload.selector);
        if (target && typeof target.scrollTop === 'number') {{
          return target;
        }}
      }} catch (error) {{
        // ignore
      }}
    }}
    return null;
  }};
  const target = pickTarget();
  if (target) {{
    const maxY = Math.max(0, Number(target.scrollHeight || 0) - Number(target.clientHeight || 0));
    const maxX = Math.max(0, Number(target.scrollWidth || 0) - Number(target.clientWidth || 0));
    const top = Number.isFinite(Number(payload.ratioY)) && maxY > 0 ? Number(payload.ratioY) * maxY : Number(payload.scrollTop || 0);
    const left = Number.isFinite(Number(payload.ratioX)) && maxX > 0 ? Number(payload.ratioX) * maxX : Number(payload.scrollLeft || 0);
    if (typeof target.scrollTo === 'function') {{
      target.scrollTo({{ left, top, behavior: 'auto' }});
    }} else {{
      target.scrollTop = top;
      target.scrollLeft = left;
    }}
    return true;
  }}
  const maxY = Math.max(document.documentElement.scrollHeight, document.body.scrollHeight) - window.innerHeight;
  const maxX = Math.max(document.documentElement.scrollWidth, document.body.scrollWidth) - window.innerWidth;
  const top = Number.isFinite(Number(payload.ratioY)) && maxY > 0 ? Number(payload.ratioY) * maxY : Number(payload.y || 0);
  const left = Number.isFinite(Number(payload.ratioX)) && maxX > 0 ? Number(payload.ratioX) * maxX : Number(payload.x || 0);
  window.scrollTo({{ left, top, behavior: 'auto' }});
  return true;
}})()
"""


def _build_key_expression(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return f"""
(() => {{
  const payload = {data};
  let target = document.activeElement || document.body;
  if (payload.selector) {{
    try {{
      target = document.querySelector(payload.selector) || target;
    }} catch (error) {{
      // ignore
    }}
  }}
  target.focus?.();
  const init = {{
    key: payload.key || '',
    code: payload.code || '',
    ctrlKey: !!payload.ctrlKey,
    shiftKey: !!payload.shiftKey,
    altKey: !!payload.altKey,
    metaKey: !!payload.metaKey,
    bubbles: true,
    cancelable: true,
    ruyi: true,
  }};
  target.dispatchEvent(new KeyboardEvent('keydown', init));
  target.dispatchEvent(new KeyboardEvent('keyup', init));
  return true;
}})()
"""
