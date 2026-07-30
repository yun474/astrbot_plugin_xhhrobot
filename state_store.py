from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import Any

from .models import Mention

STATE_VERSION = 6


class StateStore:
    def __init__(
        self,
        *,
        load_value: Callable[[str, Any], Awaitable[Any]],
        save_value: Callable[[str, Any], Awaitable[None]],
        key: str = "runtime_state_v1",
        max_queue: int = 500,
        max_recent: int = 200,
        max_dead: int = 200,
        max_browse_records: int = 500,
    ) -> None:
        self._load_value = load_value
        self._save_value = save_value
        self._key = key
        self._max_queue = max(20, max_queue)
        self._max_recent = max(20, max_recent)
        self._max_dead = max(20, max_dead)
        self._max_browse_records = max(100, max_browse_records)
        self._lock = asyncio.Lock()
        self._state = self._default_state()

    async def initialize(self) -> None:
        raw = await self._load_value(self._key, None)
        async with self._lock:
            self._state = self._normalise(raw)
            recovered = self._recover_sending_locked()
            browse_recovered = self._recover_browse_sending_locked()
            self._prune_browse_records_locked()
            if recovered or browse_recovered:
                await self._save_locked()

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return copy.deepcopy(self._state)

    async def set_paused(self, paused: bool) -> None:
        async with self._lock:
            self._state["paused"] = bool(paused)
            await self._save_locked()

    async def set_initial_cursor(
        self,
        newest_message_id: int,
        *,
        source: str = "mention",
    ) -> None:
        async with self._lock:
            initialized_key, cursor_key = self._cursor_keys(source)
            self._state[initialized_key] = True
            self._state[cursor_key] = max(0, int(newest_message_id))
            self._state["stats"]["baseline_skipped"] += 1
            await self._save_locked()

    async def ingest(
        self,
        *,
        newest_message_id: int,
        queued: Iterable[Mention],
        ignored: Iterable[tuple[Mention, str]],
        source: str = "mention",
    ) -> tuple[int, int]:
        queued_count = 0
        ignored_count = 0
        now = time.time()
        async with self._lock:
            queue = self._state["queue"]
            dead = self._state["dead"]
            recent_ids = {
                str(item.get("message_id") or "") for item in self._state["recent"]
            }
            for mention in queued:
                key = str(mention.message_id)
                if key in queue or key in dead or key in recent_ids:
                    continue
                duplicate = self._find_active_target_locked(mention)
                if duplicate is not None:
                    duplicate_key, duplicate_item = duplicate
                    if (
                        duplicate_key in queue
                        and mention.source == "mention"
                        and duplicate_item.get("source") != "mention"
                    ):
                        duplicate_item.update(
                            {
                                "source": "mention",
                                "user_id": mention.user_id,
                                "comment_text": mention.comment_text,
                                "root_comment_id": mention.root_comment_id,
                                "updated_at": now,
                            }
                        )
                        queue[duplicate_key] = duplicate_item
                    continue
                if len(queue) >= self._max_queue:
                    self._append_dead_locked(
                        mention, "queue_overflow", "待处理队列已满。", now
                    )
                    continue
                queue[key] = {
                    **mention.to_dict(),
                    "status": "pending",
                    "attempts": 0,
                    "next_attempt_at": 0.0,
                    "last_error": "",
                    "review_approved": False,
                    "created_at": now,
                    "updated_at": now,
                }
                queued_count += 1
            for mention, reason in ignored:
                key = str(mention.message_id)
                if key in queue or key in dead or key in recent_ids:
                    continue
                if self._find_active_target_locked(mention) is not None:
                    continue
                self._append_recent_locked(mention, "ignored", reason, "", now)
                ignored_count += 1
            initialized_key, cursor_key = self._cursor_keys(source)
            self._state[initialized_key] = True
            self._state[cursor_key] = max(
                int(self._state.get(cursor_key) or 0),
                int(newest_message_id or 0),
            )
            self._state["stats"]["seen"] += queued_count + ignored_count
            self._state["stats"]["queued"] += queued_count
            self._state["stats"]["ignored"] += ignored_count
            await self._save_locked()
        return queued_count, ignored_count

    async def due_items(self, *, limit: int, now: float | None = None) -> list[Mention]:
        current = time.time() if now is None else now
        async with self._lock:
            items = [
                value
                for value in self._state["queue"].values()
                if value.get("status") == "pending"
                and float(value.get("next_attempt_at") or 0) <= current
            ]
            items.sort(
                key=lambda item: (
                    float(item.get("created_at") or 0),
                    int(item.get("message_id") or 0),
                )
            )
            return [Mention.from_dict(item) for item in items[: max(0, limit)]]

    async def mark_sending(self, message_id: int) -> bool:
        """Atomically claim the one allowed outbound reply for a comment.

        A notification can arrive through both the mention stream and the
        own-post-comment stream.  The queue normally merges those records, but
        this final gate also protects against stale or concurrently dispatched
        events before a request reaches Xiaoheihe.
        """

        async with self._lock:
            key = str(message_id)
            item = self._state["queue"].get(key)
            if item is None:
                return False
            if item.get("status") not in {"pending", "dispatched"}:
                return False

            if self._target_already_claimed_locked(key, item):
                self._skip_duplicate_locked(key, item)
                await self._save_locked()
                return False

            item["status"] = "sending"
            item["updated_at"] = time.time()
            await self._save_locked()
            return True

    async def mark_review_pending(self, message_id: int) -> bool:
        """Hold a generated reply without making it eligible for auto dispatch."""

        async with self._lock:
            item = self._state["queue"].get(str(message_id))
            if item is None or item.get("status") not in {"pending", "dispatched"}:
                return False
            item["status"] = "pending_review"
            item["updated_at"] = time.time()
            await self._save_locked()
            return True

    async def approve_for_generation(self, message_id: int) -> bool:
        """Release a pre-generation review item back to the event pipeline."""

        async with self._lock:
            item = self._state["queue"].get(str(message_id))
            if item is None or item.get("status") != "pending_review":
                return False
            item.update(
                {
                    "status": "pending",
                    "review_approved": True,
                    "last_error": "",
                    "next_attempt_at": 0.0,
                    "updated_at": time.time(),
                }
            )
            await self._save_locked()
            return True

    async def is_review_approved(self, message_id: int) -> bool:
        async with self._lock:
            item = self._state["queue"].get(str(message_id))
            return bool(item and item.get("review_approved", False))

    async def mark_review_sending(self, message_id: int) -> bool:
        """Atomically claim a human-approved comment for platform delivery."""

        async with self._lock:
            key = str(message_id)
            item = self._state["queue"].get(key)
            if item is None or item.get("status") != "pending_review":
                return False
            if self._target_already_claimed_locked(key, item):
                self._skip_duplicate_locked(key, item)
                await self._save_locked()
                return False
            item["status"] = "sending"
            item["updated_at"] = time.time()
            await self._save_locked()
            return True

    async def return_to_review(self, message_id: int, reason: str) -> None:
        async with self._lock:
            item = self._state["queue"].get(str(message_id))
            if item is None or item.get("status") != "sending":
                return
            item.update(
                {
                    "status": "pending_review",
                    "last_error": str(reason or "")[:1000],
                    "next_attempt_at": 0.0,
                    "updated_at": time.time(),
                }
            )
            await self._save_locked()

    async def mark_dispatched(self, message_id: int) -> bool:
        """Claim a pending item before building its standard AstrBot event."""

        async with self._lock:
            key = str(message_id)
            item = self._state["queue"].get(key)
            if item is None or item.get("status") != "pending":
                return False
            if self._target_already_claimed_locked(key, item):
                self._skip_duplicate_locked(key, item)
                await self._save_locked()
                return False
            item["status"] = "dispatched"
            item["updated_at"] = time.time()
            await self._save_locked()
            return True

    async def item_status(self, message_id: int) -> str:
        async with self._lock:
            item = self._state["queue"].get(str(message_id))
            if item is not None:
                return str(item.get("status") or "")
            dead = self._state["dead"].get(str(message_id))
            if dead is not None:
                return str(dead.get("reason") or "dead")
            for recent in reversed(self._state["recent"]):
                if int(recent.get("message_id") or 0) == int(message_id):
                    return str(recent.get("status") or "")
            return ""

    async def mark_retry(
        self,
        message_id: int,
        error: str,
        *,
        max_attempts: int,
        delay_seconds: float,
    ) -> bool:
        async with self._lock:
            key = str(message_id)
            item = self._state["queue"].get(key)
            if item is None:
                return False
            attempts = int(item.get("attempts") or 0) + 1
            item.update(
                {
                    "status": "pending",
                    "attempts": attempts,
                    "last_error": error[:1000],
                    "next_attempt_at": time.time() + max(0.0, delay_seconds),
                    "updated_at": time.time(),
                }
            )
            self._state["stats"]["failed_attempts"] += 1
            if attempts >= max(1, max_attempts):
                mention = Mention.from_dict(item)
                self._state["queue"].pop(key, None)
                self._append_dead_locked(
                    mention, "retry_exhausted", error, time.time(), attempts=attempts
                )
                self._state["stats"]["dead"] += 1
                await self._save_locked()
                return False
            await self._save_locked()
            return True

    async def defer(self, message_id: int, error: str, *, delay_seconds: float) -> None:
        async with self._lock:
            item = self._state["queue"].get(str(message_id))
            if item is None:
                return
            item.update(
                {
                    "status": "pending",
                    "last_error": error[:1000],
                    "next_attempt_at": time.time() + max(0.0, delay_seconds),
                    "updated_at": time.time(),
                }
            )
            await self._save_locked()

    async def mark_uncertain(self, message_id: int, error: str) -> None:
        async with self._lock:
            key = str(message_id)
            item = self._state["queue"].pop(key, None)
            if item is None:
                return
            mention = Mention.from_dict(item)
            self._append_dead_locked(
                mention,
                "uncertain_delivery",
                error,
                time.time(),
                attempts=int(item.get("attempts") or 0),
            )
            self._state["stats"]["dead"] += 1
            await self._save_locked()

    async def mark_done(self, message_id: int, reply_text: str) -> None:
        async with self._lock:
            key = str(message_id)
            item = self._state["queue"].pop(key, None)
            if item is None:
                return
            self._append_recent_locked(
                Mention.from_dict(item), "replied", "", reply_text, time.time()
            )
            self._state["stats"]["replied"] += 1
            await self._save_locked()

    async def mark_skipped(self, message_id: int, reason: str) -> None:
        async with self._lock:
            key = str(message_id)
            item = self._state["queue"].pop(key, None)
            if item is None:
                return
            self._append_recent_locked(
                Mention.from_dict(item), "skipped", reason, "", time.time()
            )
            self._state["stats"]["skipped"] += 1
            await self._save_locked()

    async def mark_rejected(self, message_id: int, reason: str) -> None:
        async with self._lock:
            key = str(message_id)
            item = self._state["queue"].pop(key, None)
            if item is None:
                return
            self._append_recent_locked(
                Mention.from_dict(item), "rejected", reason, "", time.time()
            )
            self._state["stats"]["rejected"] += 1
            await self._save_locked()

    async def retry_dead(self, *, include_uncertain: bool = True) -> int:
        async with self._lock:
            moved = 0
            now = time.time()
            for key, item in list(self._state["dead"].items()):
                if len(self._state["queue"]) >= self._max_queue:
                    break
                if item.get("reason") == "uncertain_delivery" and not include_uncertain:
                    continue
                mention = Mention.from_dict(item)
                self._state["queue"][key] = {
                    **mention.to_dict(),
                    "status": "pending",
                    "attempts": 0,
                    "next_attempt_at": 0.0,
                    "last_error": "",
                    "created_at": now,
                    "updated_at": now,
                }
                self._state["dead"].pop(key, None)
                moved += 1
            if moved:
                await self._save_locked()
            return moved

    async def conversation_history(
        self, *, link_id: int, user_id: int, turns: int
    ) -> list[dict[str, str]]:
        if turns <= 0:
            return []
        async with self._lock:
            matched = [
                item
                for item in self._state["recent"]
                if item.get("status") == "replied"
                and int(item.get("link_id") or 0) == link_id
                and int(item.get("user_id") or 0) == user_id
            ]
            matched = matched[-turns:]
            return [
                {
                    "user": str(item.get("comment_text") or ""),
                    "assistant": str(item.get("reply_text") or ""),
                }
                for item in matched
            ]

    async def begin_browse_run(self, *, now: float, next_run_at: float) -> None:
        async with self._lock:
            browse = self._state["auto_browse"]
            browse["last_run_at"] = max(0.0, float(now))
            browse["next_run_at"] = max(0.0, float(next_run_at))
            browse["last_error"] = ""
            browse["stats"]["runs"] += 1
            await self._save_locked()

    async def schedule_browse(self, next_run_at: float) -> None:
        async with self._lock:
            self._state["auto_browse"]["next_run_at"] = max(0.0, float(next_run_at))
            await self._save_locked()

    async def note_browse_feed(self, count: int) -> None:
        async with self._lock:
            self._state["auto_browse"]["stats"]["seen"] += max(0, int(count))
            await self._save_locked()

    async def finish_browse_run(self, error: str = "") -> None:
        async with self._lock:
            browse = self._state["auto_browse"]
            if error:
                browse["last_error"] = str(error)[:1000]
                browse["stats"]["failed_runs"] += 1
            else:
                browse["last_error"] = ""
                browse["last_success_at"] = time.time()
            await self._save_locked()

    async def record_browse(
        self,
        *,
        link_id: int,
        title: str,
        author_id: str,
        status: str,
        reason: str = "",
        comment_text: str = "",
        evaluated: bool = False,
        now: float | None = None,
    ) -> None:
        current = time.time() if now is None else float(now)
        key = str(max(0, int(link_id)))
        if key == "0":
            return
        async with self._lock:
            browse = self._state["auto_browse"]
            previous = browse["records"].get(key, {})
            record = {
                "link_id": int(link_id),
                "title": str(title)[:500],
                "author_id": str(author_id)[:100],
                "status": str(status),
                "reason": str(reason)[:1000],
                "comment_text": str(comment_text)[:5000],
                "attempted_at": (
                    current
                    if status == "sending"
                    else float(previous.get("attempted_at") or current)
                ),
                "completed_at": 0.0 if status == "sending" else current,
            }
            browse["records"][key] = record

            if status != "sending":
                if evaluated:
                    browse["stats"]["evaluated"] += 1
                counter = {
                    "commented": "commented",
                    "skipped": "skipped",
                    "dry_run": "dry_runs",
                    "failed": "failed",
                    "uncertain": "uncertain",
                }.get(status)
                if counter:
                    browse["stats"][counter] += 1
            self._prune_browse_records_locked()
            await self._save_locked()

    def _recover_sending_locked(self) -> int:
        recovered = 0
        uncertain_recovered = 0
        now = time.time()
        for key, item in list(self._state["queue"].items()):
            if item.get("status") == "dispatched":
                item["status"] = "pending"
                item["last_error"] = "AstrBot 重启前事件尚未开始发送，已重新排队。"
                item["next_attempt_at"] = 0.0
                item["updated_at"] = now
                recovered += 1
                continue
            if item.get("status") != "sending":
                continue
            self._state["queue"].pop(key, None)
            self._append_dead_locked(
                Mention.from_dict(item),
                "uncertain_delivery",
                "AstrBot 在回帖发送过程中重启，无法确认是否已发布。",
                now,
                attempts=int(item.get("attempts") or 0),
            )
            recovered += 1
            uncertain_recovered += 1
        if uncertain_recovered:
            self._state["stats"]["dead"] += uncertain_recovered
        return recovered

    def _recover_browse_sending_locked(self) -> int:
        recovered = 0
        now = time.time()
        browse = self._state["auto_browse"]
        for item in browse["records"].values():
            if item.get("status") != "sending":
                continue
            item["status"] = "uncertain"
            item["reason"] = "AstrBot 在自动评论发送过程中重启，无法确认是否已发布。"
            item["completed_at"] = now
            recovered += 1
        if recovered:
            browse["stats"]["uncertain"] += recovered
        return recovered

    def _prune_browse_records_locked(self) -> None:
        records = self._state["auto_browse"]["records"]
        if len(records) <= self._max_browse_records:
            return
        ordered = sorted(
            records,
            key=lambda key: float(
                records[key].get("completed_at")
                or records[key].get("attempted_at")
                or 0
            ),
        )
        for key in ordered[: len(records) - self._max_browse_records]:
            records.pop(key, None)

    def _append_recent_locked(
        self,
        mention: Mention,
        status: str,
        reason: str,
        reply_text: str,
        now: float,
    ) -> None:
        self._state["recent"].append(
            {
                **mention.to_dict(),
                "status": status,
                "reason": reason[:500],
                "reply_text": reply_text[:5000],
                "completed_at": now,
            }
        )
        self._state["recent"] = self._state["recent"][-self._max_recent :]

    def _append_dead_locked(
        self,
        mention: Mention,
        reason: str,
        error: str,
        now: float,
        *,
        attempts: int = 0,
    ) -> None:
        self._state["dead"][str(mention.message_id)] = {
            **mention.to_dict(),
            "reason": reason,
            "last_error": error[:1000],
            "attempts": attempts,
            "failed_at": now,
        }
        if len(self._state["dead"]) > self._max_dead:
            oldest = min(
                self._state["dead"],
                key=lambda key: float(self._state["dead"][key].get("failed_at") or 0),
            )
            self._state["dead"].pop(oldest, None)

    def _find_active_target_locked(
        self,
        mention: Mention,
    ) -> tuple[str, dict[str, Any]] | None:
        if mention.link_id <= 0 or mention.comment_id <= 0:
            return None
        target = mention.target_key
        for key, item in self._state["queue"].items():
            if self._item_target(item) == target:
                return str(key), item
        for collection in (self._state["dead"],):
            for key, item in collection.items():
                if self._item_target(item) == target:
                    return str(key), item
        for item in self._state["recent"]:
            if item.get("status") == "replied" and self._item_target(item) == target:
                return str(item.get("message_id") or ""), item
        return None

    def _target_already_claimed_locked(
        self,
        key: str,
        item: Mapping[str, Any],
    ) -> bool:
        target = self._item_target(item)
        if target == (0, 0):
            return False
        for other_key, other in self._state["queue"].items():
            if other_key == key or self._item_target(other) != target:
                continue
            if other.get("status") in {"dispatched", "pending_review", "sending"}:
                return True
        return any(
            recent.get("status") == "replied"
            and self._item_target(recent) == target
            for recent in self._state["recent"]
        )

    def _skip_duplicate_locked(self, key: str, item: Mapping[str, Any]) -> None:
        self._state["queue"].pop(key, None)
        self._append_recent_locked(
            Mention.from_dict(item),
            "skipped",
            "同一条评论已经在发送或已完成回复",
            "",
            time.time(),
        )
        self._state["stats"]["skipped"] += 1

    @staticmethod
    def _item_target(item: Mapping[str, Any]) -> tuple[int, int]:
        try:
            return int(item.get("link_id") or 0), int(item.get("comment_id") or 0)
        except (TypeError, ValueError):
            return 0, 0

    @staticmethod
    def _cursor_keys(source: str) -> tuple[str, str]:
        if source == "own_post_comment":
            return "comments_initialized", "last_comment_message_id"
        return "initialized", "last_message_id"

    async def _save_locked(self) -> None:
        await self._save_value(self._key, self._state)

    @classmethod
    def _normalise(cls, raw: Any) -> dict[str, Any]:
        state = cls._default_state()
        if not isinstance(raw, Mapping):
            return state
        state["initialized"] = bool(raw.get("initialized", False))
        state["comments_initialized"] = bool(raw.get("comments_initialized", False))
        try:
            state["last_message_id"] = int(raw.get("last_message_id") or 0)
        except (TypeError, ValueError):
            state["last_message_id"] = 0
        try:
            state["last_comment_message_id"] = int(
                raw.get("last_comment_message_id") or 0
            )
        except (TypeError, ValueError):
            state["last_comment_message_id"] = 0
        state["paused"] = bool(raw.get("paused", False))
        for key in ("queue", "dead"):
            value = raw.get(key)
            if isinstance(value, Mapping):
                state[key] = {
                    str(item_key): dict(item)
                    for item_key, item in value.items()
                    if isinstance(item, Mapping)
                }
        recent = raw.get("recent")
        if isinstance(recent, list):
            state["recent"] = [
                dict(item) for item in recent if isinstance(item, Mapping)
            ]
        stats = raw.get("stats")
        if isinstance(stats, Mapping):
            for key in state["stats"]:
                try:
                    state["stats"][key] = int(stats.get(key) or 0)
                except (TypeError, ValueError):
                    pass
        browse = raw.get("auto_browse")
        if isinstance(browse, Mapping):
            for key in ("next_run_at", "last_run_at", "last_success_at"):
                try:
                    state["auto_browse"][key] = max(0.0, float(browse.get(key) or 0))
                except (TypeError, ValueError):
                    pass
            state["auto_browse"]["last_error"] = str(browse.get("last_error") or "")[
                :1000
            ]
            records = browse.get("records")
            if isinstance(records, Mapping):
                state["auto_browse"]["records"] = {
                    str(item_key): dict(item)
                    for item_key, item in records.items()
                    if isinstance(item, Mapping)
                }
            browse_stats = browse.get("stats")
            if isinstance(browse_stats, Mapping):
                for key in state["auto_browse"]["stats"]:
                    try:
                        state["auto_browse"]["stats"][key] = int(
                            browse_stats.get(key) or 0
                        )
                    except (TypeError, ValueError):
                        pass
        return state

    @staticmethod
    def _default_state() -> dict[str, Any]:
        return {
            "version": STATE_VERSION,
            "initialized": False,
            "last_message_id": 0,
            "comments_initialized": False,
            "last_comment_message_id": 0,
            "paused": False,
            "queue": {},
            "dead": {},
            "recent": [],
            "auto_browse": {
                "next_run_at": 0.0,
                "last_run_at": 0.0,
                "last_success_at": 0.0,
                "last_error": "",
                "records": {},
                "stats": {
                    "runs": 0,
                    "seen": 0,
                    "evaluated": 0,
                    "commented": 0,
                    "skipped": 0,
                    "dry_runs": 0,
                    "failed": 0,
                    "uncertain": 0,
                    "failed_runs": 0,
                },
            },
            "stats": {
                "seen": 0,
                "queued": 0,
                "ignored": 0,
                "replied": 0,
                "rejected": 0,
                "skipped": 0,
                "failed_attempts": 0,
                "dead": 0,
                "baseline_skipped": 0,
            },
        }
