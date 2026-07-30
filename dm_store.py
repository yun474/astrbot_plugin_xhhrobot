from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .models import DirectMessage


class DirectMessageStore:
    """Persistent direct-message inbox, delivery state, and audit archive."""

    def __init__(
        self,
        path: Path,
        *,
        retention_days: int = 365,
        max_records: int = 100_000,
    ) -> None:
        self.path = Path(path)
        self.retention_days = max(0, int(retention_days))
        self.max_records = max(1_000, int(max_records))
        self._lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            await asyncio.to_thread(self._initialize_sync)
            self._initialized = True

    async def enqueue(
        self,
        messages: Sequence[DirectMessage],
        *,
        baseline: bool = False,
    ) -> int:
        if not messages:
            return 0
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(self._enqueue_sync, messages, baseline)

    async def due(self, *, limit: int) -> list[DirectMessage]:
        await self.initialize()
        async with self._lock:
            rows = await asyncio.to_thread(self._due_sync, max(0, int(limit)))
        return [self._row_to_message(row) for row in rows]

    async def mark_dispatched(self, event_key: str) -> None:
        await self._set_status(event_key, "dispatched")

    async def mark_sending(self, event_key: str) -> None:
        await self._set_status(event_key, "sending")

    async def mark_review_pending(self, event_key: str) -> bool:
        changed = await self._compare_set_status(
            event_key,
            from_status="dispatched",
            to_status="pending_review",
        )
        if changed:
            return True
        return await self._compare_set_status(
            event_key,
            from_status="pending",
            to_status="pending_review",
        )

    async def approve_for_generation(self, event_key: str) -> bool:
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(
                self._approve_for_generation_sync,
                event_key,
            )

    async def is_review_approved(self, event_key: str) -> bool:
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(
                self._is_review_approved_sync,
                event_key,
            )

    async def mark_review_sending(self, event_key: str) -> bool:
        return await self._compare_set_status(
            event_key,
            from_status="pending_review",
            to_status="sending",
        )

    async def return_to_review(self, event_key: str, reason: str) -> None:
        await self._compare_set_status(
            event_key,
            from_status="sending",
            to_status="pending_review",
            reason=reason,
        )

    async def mark_rejected(self, event_key: str, reason: str) -> None:
        await self._compare_set_status(
            event_key,
            from_status="pending_review",
            to_status="rejected",
            reason=reason,
        )

    async def mark_sent(
        self,
        event_key: str,
        *,
        reply_text: str,
        reply_image_sources: Sequence[str] = (),
    ) -> None:
        await self.initialize()
        async with self._lock:
            await asyncio.to_thread(
                self._mark_sent_sync,
                event_key,
                reply_text,
                list(reply_image_sources),
            )

    async def mark_retry(
        self,
        event_key: str,
        error: str,
        *,
        max_attempts: int,
        delay_seconds: float,
    ) -> bool:
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(
                self._mark_retry_sync,
                event_key,
                error,
                max_attempts,
                delay_seconds,
            )

    async def mark_skipped(self, event_key: str, reason: str) -> None:
        await self._set_status(event_key, "skipped", reason)

    async def defer(
        self,
        event_key: str,
        reason: str,
        *,
        delay_seconds: float,
    ) -> None:
        await self.initialize()
        async with self._lock:
            await asyncio.to_thread(
                self._defer_sync,
                event_key,
                reason,
                delay_seconds,
            )

    async def mark_uncertain(
        self,
        event_key: str,
        reason: str,
        *,
        reply_text: str = "",
        reply_image_sources: Sequence[str] = (),
    ) -> None:
        await self.initialize()
        async with self._lock:
            await asyncio.to_thread(
                self._mark_terminal_sync,
                event_key,
                "uncertain",
                reason,
                reply_text,
                list(reply_image_sources),
            )

    async def status(self, event_key: str) -> str:
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(self._status_sync, event_key)

    async def is_stream_initialized(self, source: str) -> bool:
        value = await self._meta(f"initialized:{source}")
        return value == "1"

    async def set_stream_initialized(self, source: str) -> None:
        await self._set_meta(f"initialized:{source}", "1")

    async def conversation_marker(self, source: str, user_id: str) -> str:
        return await self._meta(f"conversation:{source}:{user_id}")

    async def set_conversation_marker(
        self,
        source: str,
        user_id: str,
        marker: str,
    ) -> None:
        await self._set_meta(f"conversation:{source}:{user_id}", marker)

    async def recent_delivery_count(
        self,
        *,
        since: float,
        user_id: str = "",
    ) -> int:
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(
                self._recent_delivery_count_sync,
                float(since),
                str(user_id or ""),
            )

    async def last_delivery_at(self, user_id: str) -> float:
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(
                self._last_delivery_at_sync,
                str(user_id or ""),
            )

    async def statistics(self) -> dict[str, Any]:
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(self._statistics_sync)

    async def search(
        self,
        *,
        keyword: str = "",
        source: str = "",
        status: str = "",
        user_id: str = "",
        limit: int = 50,
        offset: int = 0,
        include_content: bool = False,
    ) -> dict[str, Any]:
        await self.initialize()
        filters = {
            "keyword": str(keyword or "").strip(),
            "source": str(source or "").strip(),
            "status": str(status or "").strip(),
            "user_id": str(user_id or "").strip(),
            "limit": max(1, min(200, int(limit or 50))),
            "offset": max(0, int(offset or 0)),
            "include_content": bool(include_content),
        }
        async with self._lock:
            return await asyncio.to_thread(self._search_sync, filters)

    async def _set_status(
        self,
        event_key: str,
        status: str,
        reason: str = "",
    ) -> None:
        await self.initialize()
        async with self._lock:
            await asyncio.to_thread(
                self._set_status_sync,
                event_key,
                status,
                reason,
            )

    async def _compare_set_status(
        self,
        event_key: str,
        *,
        from_status: str,
        to_status: str,
        reason: str = "",
    ) -> bool:
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(
                self._compare_set_status_sync,
                event_key,
                from_status,
                to_status,
                reason,
            )

    async def _meta(self, key: str) -> str:
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(self._meta_sync, key)

    async def _set_meta(self, key: str, value: str) -> None:
        await self.initialize()
        async with self._lock:
            await asyncio.to_thread(self._set_meta_sync, key, value)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _initialize_sync(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS direct_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT NOT NULL UNIQUE,
                    message_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    user_name TEXT NOT NULL DEFAULT '',
                    content TEXT NOT NULL DEFAULT '',
                    image_urls TEXT NOT NULL DEFAULT '[]',
                    occurred_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    status_reason TEXT NOT NULL DEFAULT '',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL DEFAULT 0,
                    reply_text TEXT NOT NULL DEFAULT '',
                    reply_image_sources TEXT NOT NULL DEFAULT '[]',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    delivered_at REAL NOT NULL DEFAULT 0,
                    review_approved INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS direct_message_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_dm_status_due
                    ON direct_messages(status, next_attempt_at);
                CREATE INDEX IF NOT EXISTS idx_dm_user
                    ON direct_messages(user_id);
                CREATE INDEX IF NOT EXISTS idx_dm_source
                    ON direct_messages(source);
                CREATE INDEX IF NOT EXISTS idx_dm_occurred
                    ON direct_messages(occurred_at);
                CREATE INDEX IF NOT EXISTS idx_dm_delivered
                    ON direct_messages(delivered_at);
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(direct_messages)"
                ).fetchall()
            }
            if "review_approved" not in columns:
                connection.execute(
                    "ALTER TABLE direct_messages ADD COLUMN "
                    "review_approved INTEGER NOT NULL DEFAULT 0"
                )
            now = time.time()
            connection.execute(
                """
                UPDATE direct_messages
                SET status = 'pending',
                    status_reason = 'AstrBot 重启前事件尚未开始发送，已重新排队。',
                    next_attempt_at = 0,
                    updated_at = ?
                WHERE status = 'dispatched'
                """,
                (now,),
            )
            connection.execute(
                """
                UPDATE direct_messages
                SET status = 'uncertain',
                    status_reason = 'AstrBot 在私信发送过程中重启，无法确认是否送达。',
                    delivered_at = CASE WHEN delivered_at > 0 THEN delivered_at ELSE ? END,
                    updated_at = ?
                WHERE status = 'sending'
                """,
                (now, now),
            )
            self._prune_sync(connection)
            connection.commit()
        finally:
            connection.close()

    def _enqueue_sync(
        self,
        messages: Sequence[DirectMessage],
        baseline: bool,
    ) -> int:
        now = time.time()
        status = "baseline" if baseline else "pending"
        connection = self._connect()
        inserted = 0
        try:
            for message in messages:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO direct_messages (
                        event_key, message_id, source, user_id, user_name,
                        content, image_urls, occurred_at, status,
                        status_reason, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message.event_key,
                        message.message_id,
                        message.source,
                        message.user_id,
                        message.user_name,
                        message.text,
                        _json_list(message.image_urls),
                        float(message.timestamp or now),
                        status,
                        "首次启用仅建立私信基线。" if baseline else "",
                        now,
                        now,
                    ),
                )
                inserted += int(cursor.rowcount > 0)
            self._prune_sync(connection)
            connection.commit()
            return inserted
        finally:
            connection.close()

    def _due_sync(self, limit: int) -> list[sqlite3.Row]:
        if limit <= 0:
            return []
        connection = self._connect()
        try:
            return list(
                connection.execute(
                    """
                    SELECT * FROM direct_messages
                    WHERE status = 'pending' AND next_attempt_at <= ?
                    ORDER BY occurred_at ASC, id ASC
                    LIMIT ?
                    """,
                    (time.time(), limit),
                ).fetchall()
            )
        finally:
            connection.close()

    def _set_status_sync(
        self,
        event_key: str,
        status: str,
        reason: str,
    ) -> None:
        connection = self._connect()
        try:
            connection.execute(
                """
                UPDATE direct_messages
                SET status = ?, status_reason = ?, updated_at = ?
                WHERE event_key = ?
                """,
                (status, str(reason or "")[:2000], time.time(), event_key),
            )
            connection.commit()
        finally:
            connection.close()

    def _compare_set_status_sync(
        self,
        event_key: str,
        from_status: str,
        to_status: str,
        reason: str,
    ) -> bool:
        connection = self._connect()
        try:
            cursor = connection.execute(
                """
                UPDATE direct_messages
                SET status = ?, status_reason = ?, updated_at = ?
                WHERE event_key = ? AND status = ?
                """,
                (
                    to_status,
                    str(reason or "")[:2000],
                    time.time(),
                    event_key,
                    from_status,
                ),
            )
            connection.commit()
            return cursor.rowcount == 1
        finally:
            connection.close()

    def _approve_for_generation_sync(self, event_key: str) -> bool:
        connection = self._connect()
        try:
            cursor = connection.execute(
                """
                UPDATE direct_messages
                SET status = 'pending', status_reason = '',
                    review_approved = 1, next_attempt_at = 0, updated_at = ?
                WHERE event_key = ? AND status = 'pending_review'
                """,
                (time.time(), event_key),
            )
            connection.commit()
            return cursor.rowcount == 1
        finally:
            connection.close()

    def _is_review_approved_sync(self, event_key: str) -> bool:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT review_approved FROM direct_messages WHERE event_key = ?",
                (event_key,),
            ).fetchone()
            return bool(row and int(row["review_approved"] or 0))
        finally:
            connection.close()

    def _mark_sent_sync(
        self,
        event_key: str,
        reply_text: str,
        reply_image_sources: list[str],
    ) -> None:
        now = time.time()
        connection = self._connect()
        try:
            connection.execute(
                """
                UPDATE direct_messages
                SET status = 'sent', status_reason = '', reply_text = ?,
                    reply_image_sources = ?, delivered_at = ?, updated_at = ?
                WHERE event_key = ?
                """,
                (
                    str(reply_text or "")[:10000],
                    _json_list(reply_image_sources),
                    now,
                    now,
                    event_key,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def _mark_retry_sync(
        self,
        event_key: str,
        error: str,
        max_attempts: int,
        delay_seconds: float,
    ) -> bool:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT attempts FROM direct_messages WHERE event_key = ?",
                (event_key,),
            ).fetchone()
            if row is None:
                return False
            attempts = int(row["attempts"] or 0) + 1
            terminal = attempts >= max(1, int(max_attempts))
            now = time.time()
            connection.execute(
                """
                UPDATE direct_messages
                SET status = ?, status_reason = ?, attempts = ?,
                    next_attempt_at = ?, updated_at = ?
                WHERE event_key = ?
                """,
                (
                    "failed" if terminal else "pending",
                    str(error or "")[:2000],
                    attempts,
                    0 if terminal else now + max(0.0, float(delay_seconds)),
                    now,
                    event_key,
                ),
            )
            connection.commit()
            return not terminal
        finally:
            connection.close()

    def _defer_sync(
        self,
        event_key: str,
        reason: str,
        delay_seconds: float,
    ) -> None:
        now = time.time()
        connection = self._connect()
        try:
            connection.execute(
                """
                UPDATE direct_messages
                SET status = 'pending', status_reason = ?,
                    next_attempt_at = ?, updated_at = ?
                WHERE event_key = ?
                """,
                (
                    str(reason or "")[:2000],
                    now + max(0.0, float(delay_seconds)),
                    now,
                    event_key,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def _mark_terminal_sync(
        self,
        event_key: str,
        status: str,
        reason: str,
        reply_text: str,
        reply_image_sources: list[str],
    ) -> None:
        now = time.time()
        connection = self._connect()
        try:
            connection.execute(
                """
                UPDATE direct_messages
                SET status = ?, status_reason = ?, reply_text = ?,
                    reply_image_sources = ?, delivered_at = ?, updated_at = ?
                WHERE event_key = ?
                """,
                (
                    status,
                    str(reason or "")[:2000],
                    str(reply_text or "")[:10000],
                    _json_list(reply_image_sources),
                    now,
                    now,
                    event_key,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def _status_sync(self, event_key: str) -> str:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT status FROM direct_messages WHERE event_key = ?",
                (event_key,),
            ).fetchone()
            return str(row["status"] or "") if row else ""
        finally:
            connection.close()

    def _meta_sync(self, key: str) -> str:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT value FROM direct_message_meta WHERE key = ?",
                (key,),
            ).fetchone()
            return str(row["value"] or "") if row else ""
        finally:
            connection.close()

    def _set_meta_sync(self, key: str, value: str) -> None:
        connection = self._connect()
        try:
            connection.execute(
                """
                INSERT INTO direct_message_meta(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, str(value or ""), time.time()),
            )
            connection.commit()
        finally:
            connection.close()

    def _recent_delivery_count_sync(self, since: float, user_id: str) -> int:
        connection = self._connect()
        try:
            where = "status IN ('sent', 'uncertain') AND delivered_at >= ?"
            params: list[Any] = [since]
            if user_id:
                where += " AND user_id = ?"
                params.append(user_id)
            row = connection.execute(
                f"SELECT COUNT(*) AS count FROM direct_messages WHERE {where}",
                params,
            ).fetchone()
            return int(row["count"] or 0)
        finally:
            connection.close()

    def _last_delivery_at_sync(self, user_id: str) -> float:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT MAX(delivered_at) AS delivered_at
                FROM direct_messages
                WHERE user_id = ? AND status IN ('sent', 'uncertain')
                """,
                (user_id,),
            ).fetchone()
            return float(row["delivered_at"] or 0)
        finally:
            connection.close()

    def _statistics_sync(self) -> dict[str, Any]:
        connection = self._connect()
        try:
            status_rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM direct_messages GROUP BY status"
            ).fetchall()
            source_rows = connection.execute(
                "SELECT source, COUNT(*) AS count FROM direct_messages GROUP BY source"
            ).fetchall()
            totals = connection.execute(
                """
                SELECT COUNT(*) AS total,
                       COUNT(DISTINCT user_id) AS users,
                       MIN(occurred_at) AS first_at,
                       MAX(occurred_at) AS last_at,
                       SUM(
                           CASE
                               WHEN TRIM(COALESCE(image_urls, '')) NOT IN ('', '[]')
                               THEN 1 ELSE 0
                           END
                       )
                           AS with_images
                FROM direct_messages
                """
            ).fetchone()
            return {
                "total": int(totals["total"] or 0),
                "unique_users": int(totals["users"] or 0),
                "with_images": int(totals["with_images"] or 0),
                "first_at": float(totals["first_at"] or 0),
                "last_at": float(totals["last_at"] or 0),
                "status_counts": {
                    str(row["status"]): int(row["count"] or 0) for row in status_rows
                },
                "source_counts": {
                    str(row["source"]): int(row["count"] or 0) for row in source_rows
                },
            }
        finally:
            connection.close()

    def _search_sync(self, filters: Mapping[str, Any]) -> dict[str, Any]:
        clauses: list[str] = []
        params: list[Any] = []
        for field in ("source", "status", "user_id"):
            value = str(filters[field])
            if value:
                clauses.append(f"{field} = ?")
                params.append(value)
        keyword = str(filters["keyword"])
        if keyword:
            clauses.append("(content LIKE ? OR reply_text LIKE ? OR user_name LIKE ?)")
            pattern = f"%{keyword}%"
            params.extend([pattern, pattern, pattern])
        where = " WHERE " + " AND ".join(clauses) if clauses else ""

        connection = self._connect()
        try:
            total = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM direct_messages" + where,
                    params,
                ).fetchone()["count"]
            )
            rows = connection.execute(
                "SELECT * FROM direct_messages"
                + where
                + " ORDER BY occurred_at DESC, id DESC LIMIT ? OFFSET ?",
                [*params, int(filters["limit"]), int(filters["offset"])],
            ).fetchall()
            records = []
            for row in rows:
                content = str(row["content"] or "")
                reply = str(row["reply_text"] or "")
                if not filters["include_content"]:
                    content = _redacted_preview(content)
                    reply = _redacted_preview(reply)
                records.append(
                    {
                        "event_key": str(row["event_key"]),
                        "message_id": str(row["message_id"]),
                        "source": str(row["source"]),
                        "user_id": str(row["user_id"]),
                        "user_name": str(row["user_name"]),
                        "content": content,
                        "image_count": len(_json_strings(row["image_urls"])),
                        "occurred_at": float(row["occurred_at"] or 0),
                        "status": str(row["status"]),
                        "status_reason": str(row["status_reason"]),
                        "attempts": int(row["attempts"] or 0),
                        "reply_text": reply,
                        "reply_image_count": len(
                            _json_strings(row["reply_image_sources"])
                        ),
                        "delivered_at": float(row["delivered_at"] or 0),
                    }
                )
            return {
                "total": total,
                "limit": int(filters["limit"]),
                "offset": int(filters["offset"]),
                "records": records,
            }
        finally:
            connection.close()

    def _prune_sync(self, connection: sqlite3.Connection) -> None:
        if self.retention_days > 0:
            cutoff = time.time() - self.retention_days * 86400
            connection.execute(
                """
                DELETE FROM direct_messages
                WHERE occurred_at < ?
                  AND status NOT IN (
                      'pending', 'dispatched', 'pending_review', 'sending'
                  )
                """,
                (cutoff,),
            )
        connection.execute(
            """
            DELETE FROM direct_messages
            WHERE id IN (
                SELECT id FROM direct_messages
                WHERE status NOT IN (
                    'pending', 'dispatched', 'pending_review', 'sending'
                )
                ORDER BY occurred_at DESC, id DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (self.max_records,),
        )

    @staticmethod
    def _row_to_message(row: sqlite3.Row) -> DirectMessage:
        return DirectMessage(
            event_key=str(row["event_key"]),
            message_id=str(row["message_id"]),
            user_id=str(row["user_id"]),
            user_name=str(row["user_name"]),
            text=str(row["content"]),
            image_urls=tuple(_json_strings(row["image_urls"])),
            timestamp=int(float(row["occurred_at"] or 0)),
            source=str(row["source"]),
        )


def _json_list(values: Sequence[Any]) -> str:
    return json.dumps(
        [str(value) for value in values if str(value or "").strip()],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _json_strings(value: Any) -> list[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item or "").strip()]


def _redacted_preview(value: str) -> str:
    if not value:
        return ""
    return f"[已隐藏，{len(value)} 字符]"
