import os
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.services import synchronizer
import launch_app


class FakeSyncClient:
    def __init__(self, profile_id="follower", targets=None, current_target_id="tab-a"):
        self.profile_id = profile_id
        self._targets = list(targets or [])
        self._current_target_id = current_target_id
        self.created = []
        self.closed = []
        self.activated = []
        self.switched = []
        self.evaluated = []
        self.mouse_events = []

    def current_target_id(self):
        return self._current_target_id

    def create_target(self, url: str, background: bool = False):
        created_id = f"created-{len(self.created) + 1}"
        self.created.append((url, background))
        self._targets.insert(0, {"id": created_id, "type": "page", "url": url})
        self._current_target_id = created_id
        return created_id

    def close_target(self, target_id: str):
        self.closed.append(target_id)

    def list_targets(self):
        return list(self._targets)

    def activate_target(self, target_id: str):
        self.activated.append(target_id)
        self._current_target_id = target_id

    def switch_target(self, target_id: str):
        self.switched.append(target_id)
        self._current_target_id = target_id

    def sync_to_current_target(self):
        return self._current_target_id

    def evaluate(self, expression: str):
        self.evaluated.append(expression)
        return {"ok": False}

    def dispatch_mouse_event(self, payload: dict, wait: bool = True):
        self.mouse_events.append((payload, wait))


class SynchronizerRegressionTests(unittest.TestCase):
    def test_browser_ui_sync_is_enabled_by_default(self):
        options = synchronizer._coerce_sync_options({})
        self.assertTrue(options["sync_browser_ui"])

    def test_click_event_uses_trusted_cdp_mouse_events(self):
        events = synchronizer._build_click_mouse_events({"x": 42, "y": 24, "button": 0})
        self.assertEqual([item["type"] for item in events], ["mouseMoved", "mousePressed", "mouseReleased"])
        self.assertTrue(all(item["x"] == 42 and item["y"] == 24 for item in events))

    def test_firefox_runtime_uses_ruyipage_client(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda _: {"engine": "firefox"},
            "master",
            ["follower"],
            {"sync_browser_ui": True},
        )

        with patch.object(synchronizer.RuyiFirefoxPageClient, "connect") as connect:
            client = session._ensure_client(
                existing=None,
                profile_id="firefox-profile",
                runtime={"engine": "firefox", "remote_debugging_port": 9333},
                is_master=False,
            )

        self.assertIsInstance(client, synchronizer.RuyiFirefoxPageClient)
        self.assertEqual(client.port, 9333)
        connect.assert_called_once()

    def test_master_script_exposes_poll_queue_for_firefox_bidi(self):
        self.assertIn("__oabSyncQueue", synchronizer.MASTER_INJECT_SCRIPT)
        self.assertIn("__oabSyncDrain", synchronizer.MASTER_INJECT_SCRIPT)

    def test_firefox_js_fallback_events_use_ruyi_trusted_flag(self):
        combined = "\n".join([
            synchronizer._build_click_expression({"selector": "#a"}),
            synchronizer._build_input_expression({"selector": "#a", "tag": "input"}),
            synchronizer._build_key_expression({"key": "Enter", "code": "Enter"}),
        ])

        self.assertIn("ruyi: true", combined)

    def test_missing_firefox_context_error_is_detected(self):
        self.assertTrue(synchronizer._is_missing_browsing_context_error("no such frame: Browsing Context with id abc not found"))
        self.assertTrue(synchronizer._is_missing_browsing_context_error(RuntimeError("Browsing Context with id abc not found")))
        self.assertFalse(synchronizer._is_missing_browsing_context_error("普通脚本错误"))

    def test_firefox_evaluate_recovers_stale_context_once(self):
        class FakePage:
            def __init__(self):
                self.calls = 0

            def run_js(self, expression, as_expr=True, timeout=None):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("no such frame: Browsing Context with id abc not found")
                return "ok"

        page = FakePage()
        recovered = []
        client = synchronizer.RuyiFirefoxPageClient("firefox-profile", 9333)
        client.ensure_ready = lambda: None
        client._page = page
        client._recover_current_target = lambda: recovered.append(True)

        self.assertEqual(client.evaluate("1 + 1"), "ok")
        self.assertEqual(page.calls, 2)
        self.assertEqual(recovered, [True])

    def test_firefox_sync_uses_low_latency_poll_loop(self):
        self.assertLessEqual(synchronizer.SYNC_FIREFOX_POLL_SECONDS, 0.05)
        self.assertTrue(hasattr(synchronizer._SyncSession, "_start_master_poll_thread"))
        self.assertTrue(hasattr(synchronizer._SyncSession, "_drain_master_poll_loop"))

    def test_firefox_follower_visual_overlay_script_exists(self):
        self.assertIn("__oabSyncVisual", synchronizer.FOLLOWER_VISUAL_SCRIPT)
        self.assertIn("requestAnimationFrame", synchronizer.FOLLOWER_VISUAL_SCRIPT)

    def test_firefox_smooth_wheel_expression_uses_animation_frame(self):
        expression = synchronizer._build_smooth_wheel_expression({"x": 10, "y": 20, "deltaY": 120})
        self.assertIn("__oabSmoothWheel", expression)
        self.assertIn("requestAnimationFrame", expression)

    def test_master_wheel_script_emits_delayed_scroll_calibration(self):
        self.assertIn("wheelCalibrateTimer", synchronizer.MASTER_INJECT_SCRIPT)
        self.assertIn("source: 'wheel_calibrate'", synchronizer.MASTER_INJECT_SCRIPT)

    def test_wheel_calibration_keeps_pending_wheel_before_scroll(self):
        worker = synchronizer._FollowerWorker(
            follower_id="follower",
            client_getter=lambda: object(),
            apply_handler=lambda client, event_type, payload: None,
            error_handler=lambda follower_id, exc: None,
        )
        worker.submit("wheel", {"deltaY": 100})
        worker.submit("scroll", {"source": "wheel_calibrate", "y": 100})

        self.assertEqual([item[0] for item in list(worker._items)], ["wheel", "scroll"])

    def test_address_bar_navigation_event_is_forwarded(self):
        session = synchronizer._SyncSession(lambda _: {}, lambda _: {}, "master", ["follower"], {"sync_navigation": True})
        forwarded = []
        session._broadcast_navigation = forwarded.append

        session._handle_master_event({
            "method": "Page.frameNavigated",
            "params": {"frame": {"url": "https://example.com/", "parentId": None}},
        })

        self.assertEqual(forwarded, ["https://example.com/"])

    def test_master_tab_close_is_forwarded_from_target_diff(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda _: {},
            "master",
            ["follower"],
            {"sync_browser_ui": True},
        )
        session._master_target_ids = ["tab-a", "tab-b"]
        session._master_target_urls = {"tab-a": "https://a.test/", "tab-b": "https://b.test/"}
        session._list_master_targets = lambda: [{"id": "tab-a", "type": "page", "url": "https://a.test/"}]
        forwarded = []
        session._broadcast_browser_ui_event = lambda event_type, payload: forwarded.append((event_type, payload))

        session._sync_browser_ui_changes()

        self.assertEqual(forwarded, [("browser_close_current", {})])

    def test_master_new_tab_is_forwarded_from_target_diff(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda _: {"engine": "chrome"},
            "master",
            ["follower"],
            {"sync_browser_ui": True},
        )
        session._master_target_ids = ["tab-a"]
        session._master_target_urls = {"tab-a": "https://a.test/"}
        session._master_active_target_id = "tab-a"
        session._list_master_targets = lambda: [
            {"id": "tab-b", "type": "page", "url": "chrome://newtab/"},
            {"id": "tab-a", "type": "page", "url": "https://a.test/"},
        ]
        forwarded = []
        session._broadcast_browser_ui_event = lambda event_type, payload: forwarded.append((event_type, payload))

        session._sync_browser_ui_changes()

        self.assertIn(("browser_new_tab", {"url": "chrome://newtab/", "activate": True}), forwarded)

    def test_blank_new_tab_created_by_recent_click_is_deferred(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda _: {"engine": "chrome"},
            "master",
            ["follower"],
            {"sync_browser_ui": True},
        )
        session._master_target_ids = ["tab-a"]
        session._master_target_urls = {"tab-a": "https://a.test/"}
        session._master_active_target_id = "tab-a"
        session._last_click_event_at = synchronizer.time.monotonic()
        session._list_master_targets = lambda: [
            {"id": "tab-b", "type": "page", "url": "about:blank"},
            {"id": "tab-a", "type": "page", "url": "https://a.test/"},
        ]
        forwarded = []
        session._broadcast_browser_ui_event = lambda event_type, payload: forwarded.append((event_type, payload))

        session._sync_browser_ui_changes()

        self.assertEqual(forwarded, [])
        self.assertIn("tab-b", session._deferred_new_target_ids)

    def test_deferred_new_tab_opens_when_real_url_is_available(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda _: {"engine": "chrome"},
            "master",
            ["follower"],
            {"sync_browser_ui": True},
        )
        session._master_target_ids = ["tab-b", "tab-a"]
        session._master_target_urls = {"tab-b": "about:blank", "tab-a": "https://a.test/"}
        session._master_active_target_id = "tab-b"
        session._deferred_new_target_ids["tab-b"] = True
        session._list_master_targets = lambda: [
            {"id": "tab-b", "type": "page", "url": "https://target.test/"},
            {"id": "tab-a", "type": "page", "url": "https://a.test/"},
        ]
        forwarded = []
        navigations = []
        session._broadcast_browser_ui_event = lambda event_type, payload: forwarded.append((event_type, payload))
        session._broadcast_navigation = navigations.append
        session._switch_master_target = lambda _: False

        session._sync_browser_ui_changes()

        self.assertEqual(forwarded, [("browser_new_tab", {"url": "https://target.test/", "activate": True})])
        self.assertEqual(navigations, [])
        self.assertNotIn("tab-b", session._deferred_new_target_ids)

    def test_master_active_tab_change_is_forwarded(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda _: {"engine": "chrome"},
            "master",
            ["follower"],
            {"sync_browser_ui": True},
        )
        session._master_target_ids = ["tab-a", "tab-b"]
        session._master_target_urls = {"tab-a": "https://a.test/", "tab-b": "https://b.test/"}
        session._master_active_target_id = "tab-a"
        session._list_master_targets = lambda: [
            {"id": "tab-b", "type": "page", "url": "https://b.test/"},
            {"id": "tab-a", "type": "page", "url": "https://a.test/"},
        ]
        forwarded = []
        session._broadcast_browser_ui_event = lambda event_type, payload: forwarded.append((event_type, payload))

        session._sync_browser_ui_changes()

        self.assertIn(("browser_activate_tab", {"url": "https://b.test/"}), forwarded)

    def test_master_event_carries_page_url_to_follower(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda _: {"engine": "chrome"},
            "master",
            ["follower"],
            {"sync_click": True, "sync_browser_ui": True},
        )
        captured = []
        session._follower_clients = {"follower": object()}
        session._apply_event_to_follower = lambda client, event_type, payload: captured.append((event_type, payload))

        session._dispatch_master_event({
            "type": "click",
            "href": "https://b.test/",
            "payload": {"selector": "#submit"},
        })

        self.assertEqual(captured[0][1]["page_url"], "https://b.test/")

    def test_browser_new_tab_event_creates_engine_tab_for_follower(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda profile_id: {"engine": "firefox" if profile_id == "follower" else "chrome"},
            "master",
            ["follower"],
            {"sync_browser_ui": True},
        )
        client = FakeSyncClient(profile_id="follower")

        session._apply_event_to_follower(client, "browser_new_tab", {"url": "about:newtab", "activate": True})

        self.assertEqual(client.created, [("about:newtab", False)])
        self.assertEqual(client.switched, ["created-1"])

    def test_open_follower_tab_activates_existing_matching_url_before_creating(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda _: {"engine": "chrome"},
            "master",
            ["follower"],
            {"sync_browser_ui": True},
        )
        client = FakeSyncClient(
            profile_id="follower",
            current_target_id="tab-a",
            targets=[
                {"id": "tab-a", "type": "page", "url": "https://a.test/"},
                {"id": "tab-b", "type": "page", "url": "https://target.test/"},
            ],
        )

        session._open_follower_tab(client, {"url": "https://target.test/", "activate": True})

        self.assertEqual(client.created, [])
        self.assertEqual(client.activated, ["tab-b"])
        self.assertEqual(client.switched, ["tab-b"])

    def test_browser_activate_tab_switches_matching_follower_target(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda _: {"engine": "chrome"},
            "master",
            ["follower"],
            {"sync_browser_ui": True},
        )
        client = FakeSyncClient(
            profile_id="follower",
            current_target_id="tab-a",
            targets=[
                {"id": "tab-a", "type": "page", "url": "https://a.test/"},
                {"id": "tab-b", "type": "page", "url": "https://b.test/"},
            ],
        )

        session._apply_event_to_follower(client, "browser_activate_tab", {"url": "https://b.test/"})

        self.assertEqual(client.activated, ["tab-b"])
        self.assertEqual(client.switched, ["tab-b"])

    def test_click_event_aligns_follower_to_matching_page_url(self):
        session = synchronizer._SyncSession(
            lambda _: {},
            lambda _: {"engine": "chrome"},
            "master",
            ["follower"],
            {"sync_click": True, "sync_browser_ui": True},
        )
        client = FakeSyncClient(
            profile_id="follower",
            current_target_id="tab-a",
            targets=[
                {"id": "tab-a", "type": "page", "url": "https://a.test/"},
                {"id": "tab-b", "type": "page", "url": "https://b.test/"},
            ],
        )

        session._apply_event_to_follower(client, "click", {"selector": "#submit", "page_url": "https://b.test/"})

        self.assertEqual(client.switched, ["tab-b"])

    def test_safe_rendering_is_default_for_packaged_desktop(self):
        with patch.dict(os.environ, {}, clear=True):
            flags = launch_app._desktop_chromium_flags()
            self.assertNotIn("--disable-gpu", flags)
            self.assertEqual(launch_app._desktop_qt_opengl_backend(), "angle")

    def test_desktop_shell_has_sync_page_lightweight_overrides(self):
        css_path = Path(__file__).resolve().parents[1] / "frontend" / "src" / "assets" / "global.css"
        content = css_path.read_text(encoding="utf-8")
        self.assertIn("html.desktop-shell .sync-page .metric-card", content)
        self.assertIn("html.desktop-shell .sync-page .console-card", content)
        self.assertIn("html.desktop-shell .sync-page .status-card-line", content)

    def test_sync_manager_styles_do_not_use_glass_blur(self):
        sync_manager_path = Path(__file__).resolve().parents[1] / "frontend" / "src" / "components" / "SyncManager.vue"
        content = sync_manager_path.read_text(encoding="utf-8")
        self.assertNotIn("backdrop-filter: blur", content)
        self.assertNotIn("-webkit-backdrop-filter: blur", content)

    def test_sync_manager_overrides_page_panel_glass_effects(self):
        sync_manager_path = Path(__file__).resolve().parents[1] / "frontend" / "src" / "components" / "SyncManager.vue"
        content = sync_manager_path.read_text(encoding="utf-8")
        self.assertIn(".sync-page :deep(.page-panel)", content)
        self.assertIn("backdrop-filter: none !important;", content)
        self.assertIn("box-shadow: none !important;", content)

    def test_installer_closes_existing_desktop_app_before_install(self):
        installer_script = Path(__file__).resolve().parents[1] / "installer" / "Open-Anti-Browser.iss"
        content = installer_script.read_text(encoding="utf-8")
        self.assertIn("CloseApplications=force", content)
        self.assertIn("CloseApplicationsFilter=Open-Anti-Browser.exe", content)


if __name__ == "__main__":
    unittest.main()
