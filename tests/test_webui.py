from __future__ import annotations

import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import astrbot_plugin_xhhrobot.main as main_module
from astrbot_plugin_xhhrobot.main import PLUGIN_ID, XhhRobotPlugin


class FakeArchive:
    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self.search_calls: list[dict] = []

    async def statistics(self) -> dict:
        return {
            "received": {"unique_comments": 2, "status_counts": {"replied": 1}},
            "bot": {"comment_records": 1, "status_counts": {"sent": 1}},
        }

    async def search(self, **kwargs) -> dict:
        self.search_calls.append(kwargs)
        return {
            "matched_count": 1,
            "returned_count": 1,
            "records": [
                {
                    "direction": "received",
                    "content": "不应显示的评论正文",
                    "status": "replied",
                }
            ],
        }


class FakeDmStore:
    def __init__(self) -> None:
        self.search_calls: list[dict] = []

    async def statistics(self) -> dict:
        return {
            "total": 3,
            "unique_users": 2,
            "with_images": 1,
            "status_counts": {"sent": 2},
        }

    async def search(self, **kwargs) -> dict:
        self.search_calls.append(kwargs)
        return {"total": 0, "records": []}


class FakeReviewStore:
    def __init__(self) -> None:
        self.search_calls: list[dict] = []

    async def statistics(self) -> dict:
        return {
            "total": 2,
            "pending": 1,
            "status_counts": {"pending": 1, "sent": 1},
            "kind_counts": {"comment": 1, "direct_message": 1},
        }

    async def search(self, **kwargs) -> dict:
        self.search_calls.append(kwargs)
        return {
            "total": 1,
            "records": [
                {
                    "id": 1,
                    "status": "pending",
                    "reply_text": "待审核草稿",
                    "content_visible": kwargs["include_content"],
                }
            ],
        }


class WebUiTests(unittest.IsolatedAsyncioTestCase):
    def plugin(self, config: dict | None = None) -> XhhRobotPlugin:
        plugin = object.__new__(XhhRobotPlugin)
        plugin.config = config or {"webui": {"enabled": True}}
        plugin.comment_archive = FakeArchive()
        plugin.dm_store = FakeDmStore()
        plugin.review_store = FakeReviewStore()
        return plugin

    async def test_summary_combines_comment_and_direct_message_databases(self) -> None:
        plugin = self.plugin()
        with patch.object(main_module, "jsonify", side_effect=lambda value: value):
            result = await plugin.web_analytics_summary()

        self.assertTrue(result["ok"])
        self.assertTrue(result["comments"]["enabled"])
        self.assertEqual(result["comments"]["received"]["unique_comments"], 2)
        self.assertEqual(result["direct_messages"]["total"], 3)

    async def test_comment_query_forwards_filters_pagination_and_hides_content(
        self,
    ) -> None:
        plugin = self.plugin(
            {
                "webui": {
                    "enabled": True,
                    "show_message_content": False,
                    "max_page_size": 100,
                }
            }
        )
        args = {
            "dataset": "comments",
            "keyword": "测试",
            "direction": "received",
            "source": "mention",
            "status": "replied",
            "link_id": "186",
            "user_id": "270",
            "limit": "30",
            "offset": "60",
        }
        with (
            patch.object(main_module, "request", SimpleNamespace(args=args)),
            patch.object(main_module, "jsonify", side_effect=lambda value: value),
        ):
            result = await plugin.web_analytics_messages()

        call = plugin.comment_archive.search_calls[0]
        self.assertEqual(call["link_id"], 186)
        self.assertEqual(call["user_id"], 270)
        self.assertEqual(call["limit"], 30)
        self.assertEqual(call["offset"], 60)
        self.assertEqual(result["records"][0]["content"], "[内容已在 WebUI 配置中隐藏]")
        self.assertEqual(result["records"][0]["dataset"], "comments")

    async def test_direct_message_query_uses_store_redaction(self) -> None:
        plugin = self.plugin(
            {"webui": {"enabled": True, "show_message_content": False}}
        )
        args = {"dataset": "direct_messages", "user_id": "99"}
        with (
            patch.object(main_module, "request", SimpleNamespace(args=args)),
            patch.object(main_module, "jsonify", side_effect=lambda value: value),
        ):
            result = await plugin.web_analytics_messages()

        self.assertTrue(result["ok"])
        self.assertFalse(plugin.dm_store.search_calls[0]["include_content"])
        self.assertEqual(plugin.dm_store.search_calls[0]["user_id"], "99")

    async def test_status_reports_real_own_post_reply_setting(self) -> None:
        plugin = self.plugin(
            {
                "webui": {"enabled": True},
                "filters": {"reply_to_own_post_comments": False},
            }
        )
        plugin.store = SimpleNamespace(
            snapshot=AsyncMock(
                return_value={
                    "queue": {},
                    "dead": {},
                    "paused": False,
                    "last_message_id": 0,
                    "last_comment_message_id": 0,
                    "stats": {},
                }
            )
        )
        plugin._archive_overview = AsyncMock(
            return_value={
                "enabled": True,
                "received_comments": 0,
                "received_observations": 0,
                "bot_comments": 0,
            }
        )
        plugin._event_tasks = {}
        plugin._worker_task = None
        plugin._started_at = time.time()
        plugin._last_poll_at = 0
        plugin._last_success_at = 0
        plugin._last_error = ""
        plugin._consecutive_errors = 0
        plugin._suspended_until = 0
        plugin._last_dm_poll_at = 0
        plugin._last_dm_error = ""
        plugin._dm_sending_blocked_reason = "小黑盒已禁止当前账号发送私信"
        plugin._dm_sending_blocked_at = 123.0
        plugin._dm_sending_blocked_until = time.time() + 60
        plugin.auth = None
        plugin._auth_invalid = False
        plugin._auth_source = "none"

        result = await plugin._web_status_payload()

        self.assertFalse(result["features"]["reply_to_own_post_comments"])
        self.assertTrue(result["direct_messages"]["sending_blocked"])
        self.assertEqual(
            result["direct_messages"]["sending_blocked_reason"],
            "小黑盒已禁止当前账号发送私信",
        )
        self.assertGreater(
            result["direct_messages"]["sending_blocked_until"], time.time()
        )

    def test_registers_page_routes_with_plugin_prefix(self) -> None:
        routes: list[tuple] = []
        plugin = self.plugin()
        plugin.context = SimpleNamespace(
            register_web_api=lambda *args: routes.append(args)
        )

        plugin._register_web_apis()

        self.assertEqual(len(routes), 10)
        self.assertTrue(all(route[0].startswith(f"/{PLUGIN_ID}/") for route in routes))

    async def test_review_query_honors_content_visibility(self) -> None:
        plugin = self.plugin(
            {"webui": {"enabled": True, "show_message_content": False}}
        )
        args = {"status": "pending", "kind": "comment"}
        with (
            patch.object(main_module, "request", SimpleNamespace(args=args)),
            patch.object(main_module, "jsonify", side_effect=lambda value: value),
        ):
            result = await plugin.web_review_items()

        self.assertTrue(result["ok"])
        self.assertFalse(result["content_visible"])
        self.assertFalse(plugin.review_store.search_calls[0]["include_content"])

    def test_dashboard_loads_bridge_before_inline_application(self) -> None:
        page = (
            Path(__file__).parents[1] / "pages" / "dashboard" / "index.html"
        ).read_text(encoding="utf-8")
        bridge_tag = '<script src="/api/plugin/page/bridge-sdk.js"></script>'
        application_marker = "<script>\n      let bridge = null;"

        self.assertIn(bridge_tag, page)
        self.assertIn(application_marker, page)
        self.assertLess(page.index(bridge_tag), page.index(application_marker))
        self.assertIn("const pageBridge = await getBridge();", page)
        self.assertNotIn("const bridge = window.AstrBotPluginPage;", page)

    def test_qr_code_uses_valid_canvas_matrix(self) -> None:
        payload = XhhRobotPlugin._qr_matrix_payload(
            "https://api.xiaoheihe.cn/account/qr_login/?app=web&qr=state"
        )

        size = payload["size"]
        rows = payload["rows"]
        self.assertGreaterEqual(size, 21)
        self.assertEqual(len(rows), size)
        self.assertTrue(all(len(row) == size for row in rows))
        self.assertTrue(all(set(row) <= {"0", "1"} for row in rows))
        self.assertEqual(set(rows[0]), {"0"})

    def test_dashboard_renders_qr_as_canvas_instead_of_data_image(self) -> None:
        page = (
            Path(__file__).parents[1] / "pages" / "dashboard" / "index.html"
        ).read_text(encoding="utf-8")

        self.assertIn("function renderQrMatrix(qrPayload)", page)
        self.assertIn("payload.qr_matrix", page)
        self.assertIn('document.createElement("canvas")', page)
        self.assertNotIn("payload.qr_image", page)

    async def test_login_payload_can_restore_qr_and_keeps_expiry_on_poll(
        self,
    ) -> None:
        plugin = self.plugin()
        plugin._login_task = SimpleNamespace(done=lambda: False)
        plugin._web_login_challenge = SimpleNamespace(
            qr_url="https://api.xiaoheihe.cn/account/qr_login/?app=web&qr=state",
            expires_in=120,
        )
        plugin._web_login_started_at = 1000.0
        plugin.auth = None
        plugin._auth_source = "none"

        initial = await plugin._web_login_payload(include_qr=True)
        polled = await plugin._web_login_payload(include_qr=False)

        self.assertIn("qr_matrix", initial)
        self.assertNotIn("qr_matrix", polled)
        self.assertEqual(initial["expires_at"], 1120.0)
        self.assertEqual(polled["expires_at"], 1120.0)

    async def test_login_session_requests_qr_for_page_refresh(self) -> None:
        plugin = self.plugin()
        plugin._worker_task = None
        plugin._web_login_payload = AsyncMock(return_value={"ok": True})

        with patch.object(main_module, "jsonify", side_effect=lambda value: value):
            result = await plugin.web_login_session()

        plugin._web_login_payload.assert_awaited_once_with(include_qr=True)
        self.assertFalse(result["worker_running"])

    def test_clear_login_uses_in_page_confirmation_dialog(self) -> None:
        page = (
            Path(__file__).parents[1] / "pages" / "dashboard" / "index.html"
        ).read_text(encoding="utf-8")

        self.assertIn('id="clearLoginDialog"', page)
        self.assertIn('id="confirmClearLoginButton"', page)
        self.assertNotIn("window.confirm(", page)
        self.assertIn(
            'byId("clearLoginButton").addEventListener("click", openClearLoginDialog);',
            page,
        )
        self.assertIn(
            'byId("confirmClearLoginButton").addEventListener("click", clearLogin);',
            page,
        )

    def test_dashboard_contains_review_workflow(self) -> None:
        page = (
            Path(__file__).parents[1] / "pages" / "dashboard" / "index.html"
        ).read_text(encoding="utf-8")

        self.assertIn('data-tab="review"', page)
        self.assertIn('id="reviewDialog"', page)
        self.assertIn('postApi("review/approve"', page)
        self.assertIn('postApi("review/reject"', page)
        self.assertIn("批准并生成回复", page)
        self.assertIn('record.phase === "incoming_message"', page)
        self.assertIn("先审核后生成", page)

    async def test_web_login_clear_returns_updated_state_and_cookie_warning(
        self,
    ) -> None:
        plugin = self.plugin(
            {
                "webui": {"enabled": True},
                "account": {"cookie": "user_pkey=manual"},
            }
        )
        plugin._clear_login_credentials = AsyncMock()

        with patch.object(main_module, "jsonify", side_effect=lambda value: value):
            result = await plugin.web_login_clear()

        plugin._clear_login_credentials.assert_awaited_once()
        self.assertTrue(result["ok"])
        self.assertEqual(result["state"], "logged_out")
        self.assertIn("手动 Cookie", result["message"])


if __name__ == "__main__":
    unittest.main()
