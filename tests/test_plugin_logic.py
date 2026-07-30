from __future__ import annotations

import asyncio
import copy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from astrbot_plugin_xhhrobot.comment_archive import CommentArchive
from astrbot_plugin_xhhrobot.dm_store import DirectMessageStore
from astrbot_plugin_xhhrobot.main import CycleResult, XhhRobotPlugin
from astrbot_plugin_xhhrobot.models import (
    AuthInfo,
    DirectMessage,
    Mention,
    NotificationPage,
    PostContext,
)
from astrbot_plugin_xhhrobot.review_store import ReviewStore
from astrbot_plugin_xhhrobot.state_store import StateStore
from astrbot_plugin_xhhrobot.xhh_client import XhhError


class MemoryBackend:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    async def load(self, key: str, default: object) -> object:
        return copy.deepcopy(self.values.get(key, default))

    async def save(self, key: str, value: object) -> None:
        self.values[key] = copy.deepcopy(value)


class FakeClient:
    def __init__(
        self,
        pages: list[list[Mention]],
        comment_pages: list[NotificationPage] | None = None,
    ) -> None:
        self.pages = pages
        self.comment_pages = comment_pages or []

    async def fetch_mentions(self, *, offset: int, limit: int) -> list[Mention]:
        page = offset // limit
        return self.pages[page] if page < len(self.pages) else []

    async def fetch_mentions_page(self, *, offset: int, limit: int) -> NotificationPage:
        items = await self.fetch_mentions(offset=offset, limit=limit)
        return NotificationPage(
            items=tuple(items),
            message_ids=tuple(item.message_id for item in items),
            raw_count=len(items),
        )

    async def fetch_comment_messages_page(
        self,
        *,
        offset: int,
        limit: int,
    ) -> NotificationPage:
        page = offset // limit
        if page < len(self.comment_pages):
            return self.comment_pages[page]
        return NotificationPage()


class BlockingReplyClient:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def send_reply(self, **kwargs: object) -> None:
        self.started.set()
        await asyncio.Event().wait()


class RecordingReplyClient:
    def __init__(self, post: PostContext) -> None:
        self.post = post
        self.sent: list[dict[str, object]] = []

    async def fetch_post_context(self, link_id: int) -> PostContext:
        return self.post

    async def send_reply(self, **kwargs: object) -> None:
        self.sent.append(dict(kwargs))


class RecordingNotificationContext:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def send_message(self, umo: str, chain: object) -> None:
        self.sent.append((umo, chain.get_plain_text()))


class TimedOutStandardEvent:
    def __init__(self, *, retry_safe: bool) -> None:
        self.delivery_future: asyncio.Future[object] = (
            asyncio.get_running_loop().create_future()
        )
        self.retry_safe = retry_safe
        self.expire_calls = 0
        self.outbound_started = not retry_safe

    def expire_if_not_started(self) -> bool:
        self.expire_calls += 1
        return self.retry_safe


class RecordingGenerationContext:
    def __init__(self) -> None:
        self.request: dict[str, object] = {}

    async def llm_generate(self, **kwargs: object) -> SimpleNamespace:
        self.request = dict(kwargs)
        return SimpleNamespace(completion_text="视觉回复")


class PluginPollingTests(unittest.IsolatedAsyncioTestCase):
    async def make_plugin(
        self, config: dict, pages: list[list[Mention]]
    ) -> XhhRobotPlugin:
        backend = MemoryBackend()
        store = StateStore(load_value=backend.load, save_value=backend.save)
        await store.initialize()
        plugin = object.__new__(XhhRobotPlugin)
        plugin.config = config
        plugin.store = store
        plugin.client = FakeClient(pages)
        plugin.auth = AuthInfo(cookie="cookie=value", heybox_id="999")
        return plugin

    @staticmethod
    def mention(message_id: int, user_id: int) -> Mention:
        return Mention(
            message_id=message_id,
            comment_id=message_id + 100,
            root_comment_id=message_id + 90,
            link_id=500,
            user_id=user_id,
            comment_text="hello",
        )

    async def test_first_poll_only_sets_baseline_by_default(self) -> None:
        plugin = await self.make_plugin(
            {
                "polling": {"page_size": 20, "max_pages_per_poll": 10},
                "filters": {"allow_all_users": True},
            },
            [[self.mention(12, 1), self.mention(11, 1)]],
        )
        result = await plugin._poll_mentions()
        snapshot = await plugin.store.snapshot()
        self.assertEqual(result.queued, 0)
        self.assertEqual(snapshot["last_message_id"], 12)
        self.assertEqual(snapshot["queue"], {})

    async def test_later_poll_applies_allow_and_block_lists(self) -> None:
        plugin = await self.make_plugin(
            {
                "polling": {"page_size": 20, "max_pages_per_poll": 10},
                "filters": {
                    "allow_all_users": False,
                    "allowed_user_ids": ["1", "2"],
                    "blocked_user_ids": ["2"],
                },
            },
            [[self.mention(12, 1), self.mention(11, 2), self.mention(10, 1)]],
        )
        await plugin.store.set_initial_cursor(10)
        result = await plugin._poll_mentions()
        due = await plugin.store.due_items(limit=10)
        self.assertEqual(result.queued, 1)
        self.assertEqual(result.ignored, 1)
        self.assertEqual([item.message_id for item in due], [12])

    async def test_poll_archives_duplicate_platform_comments_once(self) -> None:
        first = Mention(
            message_id=11,
            comment_id=777,
            root_comment_id=777,
            link_id=500,
            user_id=1,
            comment_text="重复通知",
        )
        second = Mention(
            message_id=12,
            comment_id=777,
            root_comment_id=777,
            link_id=500,
            user_id=1,
            comment_text="重复通知",
        )
        plugin = await self.make_plugin(
            {
                "polling": {"page_size": 20, "max_pages_per_poll": 10},
                "filters": {"allow_all_users": True},
            },
            [[second, first]],
        )
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        plugin.comment_archive = CommentArchive(
            Path(temp_dir.name) / "comments.sqlite3",
            retention_days=0,
        )
        plugin._archive_error = ""
        await plugin.comment_archive.initialize()
        await plugin.store.set_initial_cursor(10)

        result = await plugin._poll_mentions()
        stats = await plugin.comment_archive.statistics(keyword="重复通知")

        self.assertEqual(result.queued, 1)
        self.assertEqual(stats["received"]["raw_observations"], 2)
        self.assertEqual(stats["received"]["unique_comments"], 1)
        self.assertEqual(stats["received"]["duplicate_observations"], 1)

    async def test_page_limit_does_not_advance_cursor_or_drop_backlog(self) -> None:
        plugin = await self.make_plugin(
            {
                "polling": {"page_size": 2, "max_pages_per_poll": 1},
                "filters": {"allow_all_users": True},
            },
            [[self.mention(14, 1), self.mention(13, 1)]],
        )
        await plugin.store.set_initial_cursor(10)
        with self.assertRaises(XhhError):
            await plugin._poll_mentions()
        snapshot = await plugin.store.snapshot()
        self.assertEqual(snapshot["last_message_id"], 10)
        self.assertEqual(snapshot["queue"], {})

    async def test_page_limit_is_safe_when_initial_cursor_is_zero(self) -> None:
        plugin = await self.make_plugin(
            {
                "polling": {"page_size": 2, "max_pages_per_poll": 1},
                "filters": {"allow_all_users": True},
            },
            [[self.mention(2, 1), self.mention(1, 1)]],
        )
        await plugin.store.set_initial_cursor(0)
        with self.assertRaises(XhhError):
            await plugin._poll_mentions()
        snapshot = await plugin.store.snapshot()
        self.assertEqual(snapshot["last_message_id"], 0)
        self.assertEqual(snapshot["queue"], {})

    async def test_cancellation_during_send_moves_item_to_uncertain(self) -> None:
        backend = MemoryBackend()
        store = StateStore(load_value=backend.load, save_value=backend.save)
        await store.initialize()
        mention = self.mention(12, 1)
        await store.ingest(newest_message_id=12, queued=[mention], ignored=[])

        client = BlockingReplyClient()
        plugin = object.__new__(XhhRobotPlugin)
        plugin.config = {
            "ai": {"include_post_context": False, "history_turns": 0},
            "filters": {"allow_all_users": True},
            "notifications": {"notify_on_reply": False},
        }
        plugin.store = store
        plugin.client = client
        plugin.auth = AuthInfo(cookie="cookie=value", heybox_id="999")

        async def generate_reply(*args: object) -> str:
            return "reply"

        plugin._generate_reply = generate_reply  # type: ignore[method-assign]
        task = asyncio.create_task(plugin._process_mention(mention))
        await client.started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        snapshot = await store.snapshot()
        self.assertEqual(snapshot["queue"], {})
        self.assertEqual(snapshot["dead"]["12"]["reason"], "uncertain_delivery")

    async def test_standard_event_timeout_reports_whether_retry_is_safe(self) -> None:
        plugin = object.__new__(XhhRobotPlugin)
        plugin._event_tasks = {}
        plugin._int_cfg = lambda *args, **kwargs: 0  # type: ignore[method-assign]

        for retry_safe in (True, False):
            event = TimedOutStandardEvent(retry_safe=retry_safe)
            received: list[bool] = []

            async def on_timeout(
                value: bool,
                captured: list[bool] = received,
            ) -> None:
                captured.append(value)

            await plugin._monitor_standard_event(
                f"event-{retry_safe}", event, on_timeout
            )

            self.assertEqual(event.expire_calls, 1)
            self.assertEqual(received, [retry_safe])

    async def test_ordinary_comment_on_own_post_replies_without_mention(self) -> None:
        backend = MemoryBackend()
        store = StateStore(load_value=backend.load, save_value=backend.save)
        await store.initialize()
        comment = Mention(
            message_id=21,
            comment_id=121,
            root_comment_id=121,
            link_id=500,
            user_id=1,
            comment_text="普通评论",
            source="own_post_comment",
        )
        await store.ingest(
            newest_message_id=21,
            queued=[comment],
            ignored=[],
            source="own_post_comment",
        )
        client = RecordingReplyClient(PostContext(title="帖子", author_id="999"))
        plugin = object.__new__(XhhRobotPlugin)
        plugin.config = {
            "ai": {"include_post_context": False, "history_turns": 0},
            "filters": {
                "allow_all_users": True,
                "reply_to_own_post_comments": True,
            },
            "notifications": {
                "umo": "test:FriendMessage:notify",
                "notify_on_reply": True,
            },
        }
        plugin.store = store
        plugin.client = client
        plugin.auth = AuthInfo(cookie="cookie=value", heybox_id="999")
        plugin.context = RecordingNotificationContext()
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        plugin.comment_archive = CommentArchive(
            Path(temp_dir.name) / "comments.sqlite3",
            retention_days=0,
        )
        plugin._archive_error = ""
        await plugin.comment_archive.initialize()

        async def generate_reply(*args: object) -> str:
            return "自动回复"

        plugin._generate_reply = generate_reply  # type: ignore[method-assign]
        outcome = await plugin._process_mention(comment)

        self.assertEqual(outcome, "replied")
        self.assertEqual(len(client.sent), 1)
        self.assertEqual(client.sent[0]["reply_id"], 121)
        self.assertEqual(len(plugin.context.sent), 1)
        umo, notification = plugin.context.sent[0]
        self.assertEqual(umo, "test:FriendMessage:notify")
        self.assertTrue(notification.startswith("小黑盒自动回复成功"))
        self.assertIn("类型：自己帖子下的普通评论", notification)
        self.assertIn("对方评论：\n普通评论", notification)
        self.assertIn("Bot 回复：\n自动回复", notification)
        self.assertIn("消息 ID：21", notification)
        self.assertIn("帖子 ID：500", notification)
        self.assertIn("评论 ID：121", notification)
        self.assertIn("根评论 ID：121", notification)
        self.assertIn("用户 ID：1", notification)
        self.assertNotIn("[小黑盒机器人]", notification)
        archive_result = await plugin.comment_archive.search(direction="all")
        self.assertEqual(archive_result["matched_count"], 2)
        directions = {record["direction"] for record in archive_result["records"]}
        self.assertEqual(directions, {"received", "bot"})

    async def test_ordinary_comment_on_someone_elses_post_is_skipped(self) -> None:
        backend = MemoryBackend()
        store = StateStore(load_value=backend.load, save_value=backend.save)
        await store.initialize()
        comment = Mention(
            message_id=22,
            comment_id=122,
            root_comment_id=122,
            link_id=500,
            user_id=1,
            comment_text="普通评论",
            source="own_post_comment",
        )
        await store.ingest(
            newest_message_id=22,
            queued=[comment],
            ignored=[],
            source="own_post_comment",
        )
        client = RecordingReplyClient(PostContext(title="帖子", author_id="888"))
        plugin = object.__new__(XhhRobotPlugin)
        plugin.config = {
            "ai": {"include_post_context": False, "history_turns": 0},
            "filters": {
                "allow_all_users": True,
                "reply_to_own_post_comments": True,
            },
        }
        plugin.store = store
        plugin.client = client
        plugin.auth = AuthInfo(cookie="cookie=value", heybox_id="999")

        outcome = await plugin._process_mention(comment)

        self.assertEqual(outcome, "skipped")
        self.assertEqual(client.sent, [])
        snapshot = await store.snapshot()
        self.assertEqual(
            snapshot["recent"][-1]["reason"],
            "普通评论不在机器人自己的帖子下",
        )

    async def test_comment_stream_uses_independent_cursor(self) -> None:
        comment = Mention(
            message_id=12,
            comment_id=112,
            root_comment_id=112,
            link_id=500,
            user_id=1,
            comment_text="没有 @",
            source="own_post_comment",
        )
        page = NotificationPage(items=(comment,), message_ids=(12, 11), raw_count=2)
        plugin = await self.make_plugin(
            {
                "polling": {"page_size": 20, "max_pages_per_poll": 10},
                "filters": {
                    "allow_all_users": True,
                    "reply_to_own_post_comments": True,
                },
            },
            [],
        )
        plugin.client = FakeClient([], [page])
        await plugin.store.set_initial_cursor(10, source="own_post_comment")

        result = await plugin._poll_own_post_comments()
        snapshot = await plugin.store.snapshot()

        self.assertEqual(result.queued, 1)
        self.assertEqual(snapshot["last_message_id"], 0)
        self.assertEqual(snapshot["last_comment_message_id"], 12)


class CommentImageGenerationTests(unittest.IsolatedAsyncioTestCase):
    async def test_compatibility_generation_receives_labeled_comment_images(self) -> None:
        plugin = object.__new__(XhhRobotPlugin)
        plugin.config = {
            "ai": {
                "provider_id": "vision-model",
                "include_post_images": True,
                "max_post_images": 2,
                "max_context_images": 4,
            }
        }
        context = RecordingGenerationContext()
        plugin.context = context

        async def build_system_prompt() -> str:
            return "system"

        plugin._build_system_prompt = build_system_prompt  # type: ignore[method-assign]
        mention = Mention(
            message_id=1,
            comment_id=2,
            root_comment_id=2,
            link_id=3,
            user_id=4,
            comment_text="带图评论",
            image_urls=(
                "https://cdn.example/current.jpg",
                "https://cdn.example/shared.jpg",
            ),
            replied_image_urls=(
                "https://cdn.example/quoted.jpg",
                "https://cdn.example/shared.jpg",
            ),
        )
        post = PostContext(
            title="帖子",
            image_urls=(
                "https://cdn.example/post-1.jpg",
                "https://cdn.example/post-2.jpg",
            ),
        )

        reply = await plugin._generate_reply(mention, post, [])

        self.assertEqual(reply, "视觉回复")
        self.assertEqual(
            context.request["image_urls"],
            [
                "https://cdn.example/current.jpg",
                "https://cdn.example/shared.jpg",
                "https://cdn.example/quoted.jpg",
                "https://cdn.example/post-1.jpg",
            ],
        )
        self.assertIn("本评论图片 2 张", str(context.request["prompt"]))
        self.assertIn("被回复评论图片 1 张", str(context.request["prompt"]))
        self.assertIn("帖子图片 1 张", str(context.request["prompt"]))


class ManualReviewPolicyTests(unittest.TestCase):
    def plugin(self, config: dict) -> XhhRobotPlugin:
        plugin = object.__new__(XhhRobotPlugin)
        plugin.config = config
        return plugin

    def test_enabled_review_covers_all_inbound_message_types(self) -> None:
        plugin = self.plugin({"manual_review": {"enabled": True}})

        self.assertTrue(
            plugin._requires_human_review(
                kind="comment", source="mention", user_id="1"
            )
        )
        self.assertTrue(
            plugin._requires_human_review(
                kind="comment", source="own_post_comment", user_id="1"
            )
        )
        self.assertTrue(
            plugin._requires_human_review(
                kind="comment", source="comment_reply", user_id="1"
            )
        )
        self.assertTrue(
            plugin._requires_human_review(
                kind="direct_message", source="direct_message", user_id="1"
            )
        )
        self.assertTrue(
            plugin._requires_human_review(
                kind="auto_browse", source="auto_browse", user_id=""
            )
        )

    def test_direct_message_auto_approve_list_bypasses_only_review(self) -> None:
        plugin = self.plugin(
            {
                "manual_review": {
                    "enabled": True,
                    "dm_auto_approve_user_ids": ["42"],
                }
            }
        )

        self.assertFalse(
            plugin._requires_human_review(
                kind="direct_message",
                source="direct_message",
                user_id="42",
            )
        )
        self.assertTrue(
            plugin._requires_human_review(
                kind="direct_message",
                source="direct_message",
                user_id="43",
            )
        )
        self.assertTrue(
            plugin._requires_human_review(
                kind="comment",
                source="mention",
                user_id="42",
            )
        )

    def test_review_workflow_switch_controls_pre_generation_hold(self) -> None:
        before = self.plugin(
            {
                "event_bridge": {"enabled": True},
                "manual_review": {
                    "enabled": True,
                    "workflow": "review_then_generate",
                },
            }
        )
        after = self.plugin(
            {
                "event_bridge": {"enabled": True},
                "manual_review": {
                    "enabled": True,
                    "workflow": "generate_then_review",
                },
            }
        )

        self.assertTrue(
            before._review_before_generation(
                kind="comment",
                source="mention",
                user_id="1",
            )
        )
        self.assertFalse(
            after._review_before_generation(
                kind="comment",
                source="mention",
                user_id="1",
            )
        )


class ManualReviewFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        backend = MemoryBackend()
        self.plugin = object.__new__(XhhRobotPlugin)
        self.plugin.config = {}
        self.plugin.store = StateStore(
            load_value=backend.load,
            save_value=backend.save,
        )
        await self.plugin.store.initialize()
        self.plugin.dm_store = DirectMessageStore(
            Path(self.temp_dir.name) / "direct_messages.sqlite3"
        )
        self.plugin.review_store = ReviewStore(
            Path(self.temp_dir.name) / "reviews.sqlite3"
        )
        self.plugin.comment_archive = None

    async def test_comment_draft_moves_source_into_review_hold(self) -> None:
        mention = Mention(
            message_id=10,
            comment_id=20,
            root_comment_id=20,
            link_id=30,
            user_id=40,
            user_name="Alice",
            comment_text="@bot 你好",
        )
        await self.plugin.store.ingest(
            newest_message_id=10,
            queued=[mention],
            ignored=[],
        )
        await self.plugin.store.mark_dispatched(10)

        await self.plugin._hold_comment_for_review(
            mention,
            "你好呀",
            [],
        )

        self.assertEqual(
            await self.plugin.store.item_status(10),
            "pending_review",
        )
        self.assertEqual(await self.plugin.store.due_items(limit=10), [])
        reviews = await self.plugin.review_store.search(status="pending")
        self.assertEqual(reviews["total"], 1)
        self.assertEqual(reviews["records"][0]["reply_text"], "你好呀")

    async def test_direct_message_draft_moves_source_into_review_hold(self) -> None:
        message = DirectMessage(
            event_key="direct_message:40:50",
            message_id="50",
            user_id="40",
            user_name="Alice",
            text="在吗",
            image_urls=(),
            timestamp=100,
        )
        await self.plugin.dm_store.enqueue([message])
        await self.plugin.dm_store.mark_dispatched(message.event_key)

        await self.plugin._hold_direct_message_for_review(
            message,
            "在呢",
            [],
        )

        self.assertEqual(
            await self.plugin.dm_store.status(message.event_key),
            "pending_review",
        )
        self.assertEqual(await self.plugin.dm_store.due(limit=10), [])
        reviews = await self.plugin.review_store.search(status="pending")
        self.assertEqual(reviews["total"], 1)
        self.assertEqual(reviews["records"][0]["kind"], "direct_message")

    async def test_pre_generation_comment_can_be_released_once(self) -> None:
        mention = Mention(
            message_id=11,
            comment_id=21,
            root_comment_id=21,
            link_id=31,
            user_id=41,
            user_name="Bob",
            comment_text="@bot 帮我看看",
        )
        await self.plugin.store.ingest(
            newest_message_id=11,
            queued=[mention],
            ignored=[],
        )
        await self.plugin._hold_comment_for_review(
            mention,
            "",
            [],
            phase="incoming_message",
        )
        result = await self.plugin.review_store.search(status="pending")
        item = await self.plugin.review_store.approve_for_generation(
            result["records"][0]["id"],
            expected_revision=result["records"][0]["revision"],
        )
        await self.plugin._release_review_for_generation(item)

        self.assertTrue(await self.plugin.store.is_review_approved(11))
        self.assertEqual(await self.plugin.store.due_items(limit=10), [mention])


class DirectMessageRestrictionTests(unittest.IsolatedAsyncioTestCase):
    async def test_zero_restriction_pause_does_not_block_later_messages(
        self,
    ) -> None:
        plugin = object.__new__(XhhRobotPlugin)
        plugin.config = {"direct_messages": {"restriction_pause_sec": 0}}
        plugin._last_dm_error = ""
        plugin._dm_sending_blocked_reason = ""
        plugin._dm_sending_blocked_at = 0.0
        plugin._dm_sending_blocked_until = 0.0
        message = DirectMessage(
            event_key="dm:99:1",
            message_id="1",
            user_id="99",
            user_name="Alice",
            text="你好",
            image_urls=(),
            timestamp=1,
        )

        await plugin._block_automatic_direct_messages("平台拒绝发送", message)

        self.assertEqual(plugin._dm_sending_block_reason(), "")
        self.assertEqual(plugin._last_dm_error, "平台拒绝发送")

    async def test_restriction_skips_triggering_message_and_pauses_temporarily(
        self,
    ) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        dm_store = DirectMessageStore(Path(temp_dir.name) / "direct_messages.sqlite3")
        first = DirectMessage(
            event_key="dm:99:1",
            message_id="1",
            user_id="99",
            user_name="Alice",
            text="你好",
            image_urls=(),
            timestamp=1,
        )
        second = DirectMessage(
            event_key="dm:100:2",
            message_id="2",
            user_id="100",
            user_name="Bob",
            text="还在吗",
            image_urls=(),
            timestamp=2,
        )
        await dm_store.enqueue((first, second))

        plugin = object.__new__(XhhRobotPlugin)
        plugin.config = {
            "event_bridge": {"enabled": True},
            "direct_messages": {"enabled": True},
            "notifications": {"umo": "test:FriendMessage:notify"},
        }
        plugin.dm_store = dm_store
        plugin.context = RecordingNotificationContext()
        plugin._last_dm_error = ""
        plugin._dm_sending_blocked_reason = ""
        plugin._dm_sending_blocked_at = 0.0
        plugin._dm_sending_blocked_until = 0.0

        await plugin._handle_dm_event_error(
            first,
            XhhError(
                "您已被禁止发送消息行为",
                retryable=False,
                terminal=True,
                action_restricted=True,
            ),
            "自动回复",
            [],
        )

        self.assertEqual(await dm_store.status(first.event_key), "skipped")
        self.assertEqual(await dm_store.status(second.event_key), "pending")
        self.assertIn("禁止发送消息", plugin._dm_sending_blocked_reason)
        self.assertGreater(
            plugin._dm_sending_blocked_until, plugin._dm_sending_blocked_at
        )
        self.assertEqual(len(plugin.context.sent), 1)
        self.assertIn("自动私信回复已临时暂停", plugin.context.sent[0][1])
        self.assertIn("暂停 1800 秒后会自动恢复", plugin.context.sent[0][1])

        await plugin._process_pending_direct_messages(CycleResult())
        self.assertEqual(await dm_store.status(second.event_key), "pending")

        plugin._dm_sending_blocked_until = 0.0
        self.assertEqual(plugin._dm_sending_block_reason(), "")


if __name__ == "__main__":
    unittest.main()
