from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from astrbot_plugin_xhhrobot.review_store import (
    ReviewConflictError,
    ReviewStore,
)


class ReviewStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = Path(self.temp_dir.name) / "review.sqlite3"
        self.store = ReviewStore(self.path)
        await self.store.initialize()

    async def asyncTearDown(self) -> None:
        self.temp_dir.cleanup()

    async def enqueue(self, *, key: str = "comment:10:20") -> dict:
        return await self.store.enqueue(
            review_key=key,
            kind="comment",
            source="mention",
            source_event_key="30",
            message_id="30",
            user_id="40",
            user_name="测试用户",
            incoming_text="请回复我",
            incoming_image_urls=["https://example.com/in.png"],
            target={
                "message_id": 30,
                "comment_id": 20,
                "root_comment_id": 20,
                "link_id": 10,
                "user_id": 40,
                "comment_text": "请回复我",
            },
            reply_text="原始草稿",
            reply_image_sources=["https://example.com/out.png"],
        )

    async def test_enqueue_search_and_redaction(self) -> None:
        item = await self.enqueue()
        self.assertEqual(item["status"], "pending")

        visible = await self.store.search(status="pending", include_content=True)
        self.assertEqual(visible["total"], 1)
        self.assertEqual(visible["records"][0]["reply_text"], "原始草稿")
        self.assertTrue(visible["records"][0]["content_visible"])

        hidden = await self.store.search(status="pending", include_content=False)
        self.assertIn("已隐藏", hidden["records"][0]["incoming_text"])
        self.assertFalse(hidden["records"][0]["content_visible"])
        self.assertNotIn("comment_text", hidden["records"][0]["target"])
        self.assertEqual(hidden["records"][0]["incoming_image_urls"], [])
        self.assertEqual(hidden["records"][0]["reply_image_sources"], [])

    async def test_claim_is_atomic_and_preserves_images(self) -> None:
        item = await self.enqueue()
        claimed = await self.store.claim(
            item["id"],
            expected_revision=item["revision"],
            reply_text="人工修改",
        )
        self.assertEqual(claimed["status"], "sending")
        self.assertEqual(claimed["reply_text"], "人工修改")
        self.assertEqual(
            claimed["reply_image_sources"],
            ["https://example.com/out.png"],
        )

        with self.assertRaises(ReviewConflictError):
            await self.store.claim(
                item["id"],
                expected_revision=item["revision"],
            )

        sent = await self.store.mark_sent(item["id"])
        self.assertEqual(sent["status"], "sent")

    async def test_reject_requires_current_revision(self) -> None:
        item = await self.enqueue()
        with self.assertRaises(ReviewConflictError):
            await self.store.reject(
                item["id"],
                expected_revision=item["revision"] + 1,
                reason="版本过期",
            )
        rejected = await self.store.reject(
            item["id"],
            expected_revision=item["revision"],
            reason="无需回复",
        )
        self.assertEqual(rejected["status"], "rejected")
        self.assertEqual(rejected["status_reason"], "无需回复")

    async def test_restart_marks_approved_in_flight_delivery_uncertain(self) -> None:
        item = await self.enqueue()
        await self.store.claim(
            item["id"],
            expected_revision=item["revision"],
        )

        restarted = ReviewStore(self.path)
        await restarted.initialize()
        result = await restarted.search(status="uncertain")

        self.assertEqual(result["total"], 1)
        self.assertIn("重启", result["records"][0]["status_reason"])

    async def test_pre_generation_approval_has_persistent_audit_lifecycle(
        self,
    ) -> None:
        item = await self.store.enqueue(
            review_key="comment:10:21",
            kind="comment",
            source="mention",
            source_event_key="31",
            message_id="31",
            user_id="40",
            user_name="测试用户",
            incoming_text="先看看要不要回复",
            incoming_image_urls=[],
            target={"message_id": 31, "comment_id": 21, "link_id": 10},
            reply_text="",
            reply_image_sources=[],
            phase="incoming_message",
        )
        self.assertEqual(item["phase"], "incoming_message")

        approved = await self.store.approve_for_generation(
            item["id"],
            expected_revision=item["revision"],
        )
        self.assertEqual(approved["status"], "approved")

        await self.store.mark_approved_sending(item["review_key"])
        sent = await self.store.mark_key_sent(item["review_key"])
        self.assertEqual(sent["status"], "sent")


if __name__ == "__main__":
    unittest.main()
