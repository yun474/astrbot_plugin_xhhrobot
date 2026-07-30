from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import At, Image, Plain
from astrbot.api.platform import MessageType

from astrbot_plugin_xhhrobot.event_bridge import (
    DeliveryPreparationError,
    EventTarget,
    XhhMessageEvent,
    build_comment_message,
    build_direct_message,
)
from astrbot_plugin_xhhrobot.xhh_client import XhhError


class EventBridgeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.image_path = self.root / "reply.png"
        self.image_path.write_bytes(b"test")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_inbound_messages_use_namespaced_users_and_image_chains(self) -> None:
        comment = build_comment_message(
            self_user_id="42",
            session_id="post!100",
            message_id="7",
            sender_id="99",
            sender_name="Alice",
            message_text="评论正文",
            image_urls=("https://example.com/a.png",),
            link_id=100,
            link_title="帖子标题",
            timestamp=123,
            raw_message={},
        )
        direct = build_direct_message(
            self_user_id="42",
            session_id="dm!99",
            message_id="8",
            sender_id="99",
            sender_name="Alice",
            message_text="私信正文",
            image_urls=("https://example.com/b.png",),
            timestamp=124,
            raw_message={},
        )

        self.assertEqual(comment.type, MessageType.GROUP_MESSAGE)
        self.assertEqual(comment.sender.user_id, "xhh:99")
        self.assertEqual(comment.self_id, "xhh:42")
        self.assertEqual(comment.session_id, "post!100")
        self.assertEqual(comment.group.group_id, "100")
        self.assertEqual(int(comment.group.group_id), 100)
        self.assertEqual(sum(isinstance(item, Image) for item in comment.message), 1)
        self.assertEqual(direct.type, MessageType.FRIEND_MESSAGE)
        self.assertEqual(direct.session_id, "dm!99")
        self.assertIsNone(direct.group)

    def test_comment_image_groups_are_labeled_and_deduplicated(self) -> None:
        comment = build_comment_message(
            self_user_id="42",
            session_id="post!100",
            message_id="7",
            sender_id="99",
            sender_name="Alice",
            message_text="评论正文",
            image_urls=(),
            image_groups=(
                (
                    "本评论图片",
                    ("https://example.com/current.png", "https://example.com/shared.png"),
                ),
                (
                    "被回复评论图片",
                    ("https://example.com/shared.png", "https://example.com/quoted.png"),
                ),
                ("帖子图片", ("https://example.com/post.png",)),
            ),
            link_id=100,
            link_title="帖子标题",
            timestamp=123,
            raw_message={},
        )

        labels = [
            item.text
            for item in comment.message
            if isinstance(item, Plain) and item.text.startswith("\n[")
        ]
        urls = [
            str(item.url or item.file or item.path or "")
            for item in comment.message
            if isinstance(item, Image)
        ]
        self.assertEqual(
            labels,
            ["\n[本评论图片：2 张]", "\n[被回复评论图片：1 张]", "\n[帖子图片：1 张]"],
        )
        self.assertEqual(
            urls,
            [
                "https://example.com/current.png",
                "https://example.com/shared.png",
                "https://example.com/quoted.png",
                "https://example.com/post.png",
            ],
        )

    async def test_outbound_comment_preserves_text_and_full_image_chain(self) -> None:
        message_obj = build_comment_message(
            self_user_id="42",
            session_id="post!100",
            message_id="7",
            sender_id="99",
            sender_name="Alice",
            message_text="评论正文",
            image_urls=(),
            link_id=100,
            link_title="帖子标题",
            timestamp=123,
            raw_message={},
        )
        client = AsyncMock()
        callbacks = {
            "start": AsyncMock(),
            "sent": AsyncMock(),
            "error": AsyncMock(),
            "empty": AsyncMock(),
        }
        event = XhhMessageEvent(
            message_obj=message_obj,
            target=EventTarget(
                kind="comment",
                source="mention",
                event_key="comment:100:7",
                raw_user_id="99",
                link_id=100,
                comment_id=7,
                root_comment_id=7,
            ),
            client=client,
            max_reply_chars=5,
            max_outgoing_images=2,
            max_local_image_bytes=1024,
            allowed_local_roots=(self.root,),
            direct_message_cooldown_seconds=0,
            clean_text=lambda value: value.strip(),
            on_send_start=callbacks["start"],
            on_sent=callbacks["sent"],
            on_send_error=callbacks["error"],
            on_empty=callbacks["empty"],
        )
        chain = MessageChain(
            [
                Plain(" 123456 "),
                Image.fromURL("https://example.com/reply.png"),
                Image(file=str(self.image_path)),
            ]
        )

        with patch.object(AstrMessageEvent, "send", new=AsyncMock()):
            await event.send(chain)

        client.send_reply.assert_awaited_once_with(
            text="12345",
            link_id=100,
            reply_id=7,
            root_id=7,
            image_sources=["https://example.com/reply.png", str(self.image_path)],
            allowed_local_roots=(self.root,),
            max_local_image_bytes=1024,
        )
        callbacks["start"].assert_awaited_once()
        callbacks["sent"].assert_awaited_once()
        callbacks["error"].assert_not_awaited()
        self.assertEqual(event.delivery_future.result().status, "sent")

    async def test_outbound_reply_removes_internal_xhh_mentions(self) -> None:
        event, client, _ = self._make_comment_event()

        with patch.object(AstrMessageEvent, "send", new=AsyncMock()):
            await event.send(
                MessageChain(
                    [
                        At(qq="xhh:99"),
                        Plain("@xhh:99没绷住，2027 年也太远了。"),
                    ]
                )
            )

        self.assertEqual(
            client.send_reply.await_args.kwargs["text"],
            "没绷住，2027 年也太远了。",
        )
        self.assertNotIn("xhh:", event.delivery_future.result().text)

    async def test_second_send_call_is_ignored(self) -> None:
        event, client, callbacks = self._make_comment_event()

        with patch.object(AstrMessageEvent, "send", new=AsyncMock()) as super_send:
            await event.send(MessageChain([Plain("第一条回复")]))
            await event.send(MessageChain([Plain("不应再次发送")]))

        client.send_reply.assert_awaited_once()
        self.assertEqual(client.send_reply.await_args.kwargs["text"], "第一条回复")
        callbacks["sent"].assert_awaited_once()
        super_send.assert_awaited_once()

    async def test_delivery_claim_rejection_does_not_send(self) -> None:
        event, client, callbacks = self._make_comment_event()
        callbacks["start"].return_value = False

        with patch.object(AstrMessageEvent, "send", new=AsyncMock()) as super_send:
            await event.send(MessageChain([Plain("被拦截的重复回复")]))

        client.send_reply.assert_not_awaited()
        callbacks["sent"].assert_not_awaited()
        super_send.assert_not_awaited()
        self.assertEqual(event.delivery_future.result().status, "suppressed")

    async def test_human_review_hold_captures_draft_without_sending(self) -> None:
        event, client, callbacks = self._make_comment_event()
        callbacks["start"].return_value = "review"

        with patch.object(AstrMessageEvent, "send", new=AsyncMock()) as super_send:
            await event.send(MessageChain([Plain("等待管理员批准")]))

        callbacks["start"].assert_awaited_once_with("等待管理员批准", [])
        client.send_reply.assert_not_awaited()
        callbacks["sent"].assert_not_awaited()
        super_send.assert_not_awaited()
        result = event.delivery_future.result()
        self.assertEqual(result.status, "pending_review")
        self.assertEqual(result.text, "等待管理员批准")

    async def test_pre_delivery_failure_finishes_monitor_and_reports_error(
        self,
    ) -> None:
        event, client, callbacks = self._make_comment_event()
        callbacks["start"].side_effect = RuntimeError("数据库不可用")

        with (
            patch.object(AstrMessageEvent, "send", new=AsyncMock()),
            self.assertRaises(DeliveryPreparationError),
        ):
            await event.send(MessageChain([Plain("无法保存的草稿")]))

        client.send_reply.assert_not_awaited()
        callbacks["error"].assert_awaited_once()
        self.assertEqual(event.delivery_future.result().status, "error")

    async def test_expired_event_does_not_send_a_late_reply(self) -> None:
        event, client, callbacks = self._make_comment_event()

        self.assertTrue(event.expire_if_not_started())
        self.assertTrue(event.is_stopped())
        with patch.object(AstrMessageEvent, "send", new=AsyncMock()):
            await event.send(MessageChain([Plain("这条迟到回复不能发送")]))

        client.send_reply.assert_not_awaited()
        callbacks["start"].assert_not_awaited()
        callbacks["sent"].assert_not_awaited()
        self.assertEqual(event.delivery_future.result().status, "expired")

    async def test_outbound_direct_message_uses_chain_sender(self) -> None:
        message_obj = build_direct_message(
            self_user_id="42",
            session_id="dm!99",
            message_id="8",
            sender_id="99",
            sender_name="Alice",
            message_text="私信正文",
            image_urls=(),
            timestamp=124,
            raw_message={},
        )
        client = AsyncMock()
        event = XhhMessageEvent(
            message_obj=message_obj,
            target=EventTarget(
                kind="direct_message",
                source="direct_message",
                event_key="dm:99:8",
                raw_user_id="99",
            ),
            client=client,
            max_reply_chars=100,
            max_outgoing_images=1,
            max_local_image_bytes=1024,
            allowed_local_roots=(self.root,),
            direct_message_cooldown_seconds=3,
            clean_text=lambda value: value,
            on_send_start=AsyncMock(),
            on_sent=AsyncMock(),
            on_send_error=AsyncMock(),
            on_empty=AsyncMock(),
        )

        with patch.object(AstrMessageEvent, "send", new=AsyncMock()):
            await event.send(
                MessageChain(
                    [Plain("收到"), Image.fromURL("https://example.com/a.png")]
                )
            )

        client.send_direct_message_chain.assert_awaited_once_with(
            user_id="99",
            text="收到",
            image_sources=["https://example.com/a.png"],
            allowed_local_roots=(self.root,),
            max_local_image_bytes=1024,
            cooldown_seconds=3,
        )

    async def test_direct_message_restriction_is_not_rethrown_to_astrbot(self) -> None:
        message_obj = build_direct_message(
            self_user_id="42",
            session_id="dm!99",
            message_id="8",
            sender_id="99",
            sender_name="Alice",
            message_text="私信正文",
            image_urls=(),
            timestamp=124,
            raw_message={},
        )
        client = AsyncMock()
        restriction = XhhError(
            "您已被禁止发送消息行为",
            retryable=False,
            terminal=True,
            action_restricted=True,
        )
        client.send_direct_message_chain.side_effect = restriction
        callbacks = {
            "start": AsyncMock(),
            "sent": AsyncMock(),
            "error": AsyncMock(),
            "empty": AsyncMock(),
        }
        event = XhhMessageEvent(
            message_obj=message_obj,
            target=EventTarget(
                kind="direct_message",
                source="direct_message",
                event_key="dm:99:8",
                raw_user_id="99",
            ),
            client=client,
            max_reply_chars=100,
            max_outgoing_images=1,
            max_local_image_bytes=1024,
            allowed_local_roots=(self.root,),
            direct_message_cooldown_seconds=3,
            clean_text=lambda value: value,
            on_send_start=callbacks["start"],
            on_sent=callbacks["sent"],
            on_send_error=callbacks["error"],
            on_empty=callbacks["empty"],
        )

        with patch.object(AstrMessageEvent, "send", new=AsyncMock()) as super_send:
            await event.send(MessageChain([Plain("回复")]))

        callbacks["error"].assert_awaited_once()
        callbacks["sent"].assert_not_awaited()
        super_send.assert_not_awaited()
        self.assertEqual(event.delivery_future.result().status, "error")
        self.assertIs(event.delivery_future.result().error, restriction)

    async def test_internal_plugin_error_result_is_not_posted(self) -> None:
        event, client, callbacks = self._make_comment_event()
        diagnostic = (
            ":(\n\n在调用插件 cr4zythursday 的处理函数 on_message 时出现异常："
            "invalid literal for int() with base 10: 'xhh-post:186407230'"
        )

        with patch.object(AstrMessageEvent, "send", new=AsyncMock()):
            await event.send(MessageChain([Plain(diagnostic)]))

        client.send_reply.assert_not_awaited()
        callbacks["start"].assert_not_awaited()
        callbacks["sent"].assert_not_awaited()
        callbacks["empty"].assert_awaited_once()
        self.assertEqual(event.delivery_future.result().status, "empty")

    async def test_internal_plugin_error_suffix_is_removed_from_reply(self) -> None:
        event, client, callbacks = self._make_comment_event()
        diagnostic = (
            ":(\n\n在调用插件 cr4zythursday 的处理函数 on_message 时出现异常：boom"
        )

        with patch.object(AstrMessageEvent, "send", new=AsyncMock()):
            await event.send(MessageChain([Plain("正常回复\n\n"), Plain(diagnostic)]))

        self.assertEqual(client.send_reply.await_args.kwargs["text"], "正常回复")
        callbacks["start"].assert_awaited_once()
        callbacks["sent"].assert_awaited_once()
        callbacks["empty"].assert_not_awaited()

    async def test_plain_sad_face_is_not_filtered(self) -> None:
        event, client, callbacks = self._make_comment_event()

        with patch.object(AstrMessageEvent, "send", new=AsyncMock()):
            await event.send(MessageChain([Plain(":(")]))

        self.assertEqual(client.send_reply.await_args.kwargs["text"], ":(")
        callbacks["sent"].assert_awaited_once()

    def _make_comment_event(
        self,
    ) -> tuple[XhhMessageEvent, AsyncMock, dict[str, AsyncMock]]:
        message_obj = build_comment_message(
            self_user_id="42",
            session_id="post!100",
            message_id="7",
            sender_id="99",
            sender_name="Alice",
            message_text="评论正文",
            image_urls=(),
            link_id=100,
            link_title="帖子标题",
            timestamp=123,
            raw_message={},
        )
        client = AsyncMock()
        callbacks = {
            "start": AsyncMock(),
            "sent": AsyncMock(),
            "error": AsyncMock(),
            "empty": AsyncMock(),
        }
        event = XhhMessageEvent(
            message_obj=message_obj,
            target=EventTarget(
                kind="comment",
                source="mention",
                event_key="comment:100:7",
                raw_user_id="99",
                link_id=100,
                comment_id=7,
                root_comment_id=7,
            ),
            client=client,
            max_reply_chars=1000,
            max_outgoing_images=2,
            max_local_image_bytes=1024,
            allowed_local_roots=(self.root,),
            direct_message_cooldown_seconds=0,
            clean_text=lambda value: value.strip(),
            on_send_start=callbacks["start"],
            on_sent=callbacks["sent"],
            on_send_error=callbacks["error"],
            on_empty=callbacks["empty"],
        )
        return event, client, callbacks


if __name__ == "__main__":
    unittest.main()
