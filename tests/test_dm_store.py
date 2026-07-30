from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from astrbot_plugin_xhhrobot.dm_store import DirectMessageStore
from astrbot_plugin_xhhrobot.models import DirectMessage


class DirectMessageStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = Path(self.temp_dir.name) / "direct_messages.sqlite3"
        self.store = DirectMessageStore(
            self.path,
            retention_days=0,
            max_records=10_000,
        )
        await self.store.initialize()

    async def asyncTearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def message(
        message_id: str,
        *,
        user_id: str = "100",
        text: str = "你好",
        source: str = "direct_message",
        timestamp: int = 1_700_000_000,
        images: tuple[str, ...] = (),
    ) -> DirectMessage:
        return DirectMessage(
            event_key=f"{source}:{user_id}:{message_id}",
            message_id=message_id,
            user_id=user_id,
            user_name=f"用户{user_id}",
            text=text,
            image_urls=images,
            timestamp=timestamp,
            source=source,
        )

    async def test_baseline_is_archived_but_not_dispatched(self) -> None:
        inserted = await self.store.enqueue([self.message("1")], baseline=True)

        self.assertEqual(inserted, 1)
        self.assertEqual(await self.store.due(limit=10), [])
        result = await self.store.search(include_content=True)
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["records"][0]["status"], "baseline")

    async def test_restart_recovers_dispatched_and_sending_states(self) -> None:
        dispatched = self.message("2")
        sending = self.message("3")
        await self.store.enqueue([dispatched, sending])
        await self.store.mark_dispatched(dispatched.event_key)
        await self.store.mark_sending(sending.event_key)

        recovered = DirectMessageStore(
            self.path,
            retention_days=0,
            max_records=10_000,
        )
        await recovered.initialize()

        self.assertEqual(await recovered.status(dispatched.event_key), "pending")
        self.assertEqual(await recovered.status(sending.event_key), "uncertain")
        due = await recovered.due(limit=10)
        self.assertEqual([item.event_key for item in due], [dispatched.event_key])

    async def test_review_hold_is_not_automatically_dispatched(self) -> None:
        message = self.message("4")
        await self.store.enqueue([message])
        await self.store.mark_dispatched(message.event_key)

        self.assertTrue(await self.store.mark_review_pending(message.event_key))
        self.assertEqual(
            await self.store.status(message.event_key),
            "pending_review",
        )
        self.assertEqual(await self.store.due(limit=10), [])
        self.assertTrue(await self.store.mark_review_sending(message.event_key))

        await self.store.return_to_review(message.event_key, "稍后重试")
        self.assertEqual(
            await self.store.status(message.event_key),
            "pending_review",
        )
        await self.store.mark_rejected(message.event_key, "无需回复")
        self.assertEqual(
            await self.store.status(message.event_key),
            "rejected",
        )

    async def test_pre_generation_approval_is_due_and_survives_restart(self) -> None:
        message = self.message("5")
        await self.store.enqueue([message])
        self.assertTrue(await self.store.mark_review_pending(message.event_key))
        self.assertTrue(await self.store.approve_for_generation(message.event_key))
        self.assertTrue(await self.store.is_review_approved(message.event_key))

        recovered = DirectMessageStore(
            self.path,
            retention_days=0,
            max_records=10_000,
        )
        await recovered.initialize()
        self.assertTrue(await recovered.is_review_approved(message.event_key))
        self.assertEqual(await recovered.due(limit=10), [message])

    async def test_statistics_search_redaction_and_pagination(self) -> None:
        first = self.message(
            "10",
            text="第一条私信",
            images=("https://example.com/a.png",),
        )
        second = self.message(
            "11",
            user_id="200",
            text="第二条私信",
            source="stranger_direct_message",
            timestamp=1_700_000_100,
        )
        await self.store.enqueue([first, second])
        await self.store.mark_sent(
            first.event_key,
            reply_text="第一条回复",
            reply_image_sources=("https://example.com/reply.png",),
        )
        await self.store.mark_skipped(second.event_key, "测试跳过")

        stats = await self.store.statistics()
        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["unique_users"], 2)
        self.assertEqual(stats["with_images"], 1)
        self.assertEqual(stats["status_counts"], {"sent": 1, "skipped": 1})

        page = await self.store.search(limit=1, offset=1, include_content=False)
        self.assertEqual(page["total"], 2)
        self.assertEqual(len(page["records"]), 1)
        self.assertNotIn("第一条私信", page["records"][0]["content"])

        visible = await self.store.search(
            keyword="第一条回复",
            status="sent",
            user_id="100",
            include_content=True,
        )
        self.assertEqual(visible["total"], 1)
        self.assertEqual(visible["records"][0]["reply_text"], "第一条回复")


if __name__ == "__main__":
    unittest.main()
