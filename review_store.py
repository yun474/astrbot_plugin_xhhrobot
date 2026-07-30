from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


class ReviewConflictError(RuntimeError):
    """Raised when a review item changed before an operator action completed."""


class ReviewStore:
    """Persistent, auditable queue for replies that require human approval."""

    ACTIVE_STATUSES = ("pending", "approved", "sending")

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
        *,
        review_key: str,
        kind: str,
        source: str,
        source_event_key: str,
        message_id: str,
        user_id: str,
        user_name: str,
        incoming_text: str,
        incoming_image_urls: Sequence[str],
        target: Mapping[str, Any],
        reply_text: str,
        reply_image_sources: Sequence[str],
        phase: str = "generated_reply",
    ) -> dict[str, Any]:
        await self.initialize()
        values = {
            "review_key": str(review_key or "").strip()[:300],
            "kind": str(kind or "").strip()[:32],
            "source": str(source or "").strip()[:64],
            "source_event_key": str(source_event_key or "").strip()[:300],
            "message_id": str(message_id or "").strip()[:300],
            "user_id": str(user_id or "").strip()[:100],
            "user_name": str(user_name or "").strip()[:300],
            "incoming_text": str(incoming_text or "")[:20_000],
            "incoming_image_urls": _json_list(incoming_image_urls),
            "target": json.dumps(
                dict(target),
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            ),
            "reply_text": str(reply_text or "")[:20_000],
            "reply_image_sources": _json_list(reply_image_sources),
            "phase": str(phase or "generated_reply").strip(),
        }
        if not values["review_key"] or values["kind"] not in {
            "comment",
            "direct_message",
        }:
            raise ValueError("审核记录缺少有效的唯一键或类型。")
        if values["phase"] not in {"generated_reply", "incoming_message"}:
            raise ValueError("审核记录阶段无效。")
        async with self._lock:
            row = await asyncio.to_thread(self._enqueue_sync, values)
        return self._row_to_dict(row, include_content=True)

    async def search(
        self,
        *,
        status: str = "pending",
        kind: str = "",
        source: str = "",
        keyword: str = "",
        limit: int = 50,
        offset: int = 0,
        include_content: bool = True,
    ) -> dict[str, Any]:
        await self.initialize()
        filters = {
            "status": str(status or "").strip(),
            "kind": str(kind or "").strip(),
            "source": str(source or "").strip(),
            "keyword": str(keyword or "").strip()[:500],
            "limit": max(1, min(200, int(limit or 50))),
            "offset": max(0, int(offset or 0)),
            "include_content": bool(include_content),
        }
        async with self._lock:
            return await asyncio.to_thread(self._search_sync, filters)

    async def statistics(self) -> dict[str, Any]:
        await self.initialize()
        async with self._lock:
            return await asyncio.to_thread(self._statistics_sync)

    async def claim(
        self,
        review_id: int,
        *,
        expected_revision: int,
        reply_text: str | None = None,
        reply_image_sources: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        await self.initialize()
        async with self._lock:
            row = await asyncio.to_thread(
                self._claim_sync,
                int(review_id),
                int(expected_revision),
                reply_text,
                None
                if reply_image_sources is None
                else _json_list(reply_image_sources),
            )
        return self._row_to_dict(row, include_content=True)

    async def reject(
        self,
        review_id: int,
        *,
        expected_revision: int,
        reason: str,
    ) -> dict[str, Any]:
        return await self._transition(
            review_id,
            expected_revision=expected_revision,
            from_status="pending",
            to_status="rejected",
            reason=reason or "已由管理员拒绝。",
            reviewed=True,
        )

    async def approve_for_generation(
        self,
        review_id: int,
        *,
        expected_revision: int,
    ) -> dict[str, Any]:
        return await self._transition(
            review_id,
            expected_revision=expected_revision,
            from_status="pending",
            to_status="approved",
            reason="",
            reviewed=True,
        )

    async def mark_approved_sending(self, review_key: str) -> dict[str, Any]:
        return await self._transition_by_key(
            review_key,
            from_status="approved",
            to_status="sending",
            reason="",
        )

    async def return_generation_pending(
        self, review_id: int, reason: str
    ) -> dict[str, Any]:
        return await self._transition(
            review_id,
            expected_revision=None,
            from_status="approved",
            to_status="pending",
            reason=reason,
        )

    async def mark_key_sent(self, review_key: str) -> dict[str, Any]:
        return await self._transition_by_key(
            review_key,
            from_status="sending",
            to_status="sent",
            reason="",
            delivered=True,
        )

    async def mark_key_uncertain(
        self, review_key: str, reason: str
    ) -> dict[str, Any]:
        return await self._transition_by_key(
            review_key,
            from_status="sending",
            to_status="uncertain",
            reason=reason,
            delivered=True,
        )

    async def mark_key_failed(
        self, review_key: str, reason: str
    ) -> dict[str, Any]:
        return await self._transition_by_key(
            review_key,
            from_status="sending",
            to_status="failed",
            reason=reason,
        )

    async def mark_approved_failed(
        self, review_key: str, reason: str
    ) -> dict[str, Any]:
        return await self._transition_by_key(
            review_key,
            from_status="approved",
            to_status="failed",
            reason=reason,
        )

    async def return_approved(
        self, review_key: str, reason: str
    ) -> dict[str, Any]:
        return await self._transition_by_key(
            review_key,
            from_status="sending",
            to_status="approved",
            reason=reason,
        )

    async def mark_sent(self, review_id: int) -> dict[str, Any]:
        return await self._transition(
            review_id,
            expected_revision=None,
            from_status="sending",
            to_status="sent",
            reason="",
            delivered=True,
        )

    async def mark_uncertain(self, review_id: int, reason: str) -> dict[str, Any]:
        return await self._transition(
            review_id,
            expected_revision=None,
            from_status="sending",
            to_status="uncertain",
            reason=reason,
            delivered=True,
        )

    async def mark_failed(self, review_id: int, reason: str) -> dict[str, Any]:
        return await self._transition(
            review_id,
            expected_revision=None,
            from_status="sending",
            to_status="failed",
            reason=reason,
        )

    async def return_pending(self, review_id: int, reason: str) -> dict[str, Any]:
        return await self._transition(
            review_id,
            expected_revision=None,
            from_status="sending",
            to_status="pending",
            reason=reason,
        )

    async def _transition(
        self,
        review_id: int,
        *,
        expected_revision: int | None,
        from_status: str,
        to_status: str,
        reason: str,
        reviewed: bool = False,
        delivered: bool = False,
    ) -> dict[str, Any]:
        await self.initialize()
        async with self._lock:
            row = await asyncio.to_thread(
                self._transition_sync,
                int(review_id),
                expected_revision,
                from_status,
                to_status,
                str(reason or "")[:2_000],
                reviewed,
                delivered,
            )
        return self._row_to_dict(row, include_content=True)

    async def _transition_by_key(
        self,
        review_key: str,
        *,
        from_status: str,
        to_status: str,
        reason: str,
        delivered: bool = False,
    ) -> dict[str, Any]:
        await self.initialize()
        async with self._lock:
            row = await asyncio.to_thread(
                self._transition_by_key_sync,
                str(review_key or ""),
                from_status,
                to_status,
                str(reason or "")[:2_000],
                delivered,
            )
        return self._row_to_dict(row, include_content=True)

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
                CREATE TABLE IF NOT EXISTS review_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_key TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_event_key TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    user_name TEXT NOT NULL DEFAULT '',
                    incoming_text TEXT NOT NULL DEFAULT '',
                    incoming_image_urls TEXT NOT NULL DEFAULT '[]',
                    target_json TEXT NOT NULL DEFAULT '{}',
                    reply_text TEXT NOT NULL DEFAULT '',
                    reply_image_sources TEXT NOT NULL DEFAULT '[]',
                    phase TEXT NOT NULL DEFAULT 'generated_reply',
                    status TEXT NOT NULL DEFAULT 'pending',
                    status_reason TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    reviewed_at REAL NOT NULL DEFAULT 0,
                    delivered_at REAL NOT NULL DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_review_status_created
                    ON review_items(status, created_at);
                CREATE INDEX IF NOT EXISTS idx_review_kind
                    ON review_items(kind);
                CREATE INDEX IF NOT EXISTS idx_review_source
                    ON review_items(source);
                CREATE INDEX IF NOT EXISTS idx_review_user
                    ON review_items(user_id);
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(review_items)"
                ).fetchall()
            }
            if "phase" not in columns:
                connection.execute(
                    "ALTER TABLE review_items ADD COLUMN "
                    "phase TEXT NOT NULL DEFAULT 'generated_reply'"
                )
            now = time.time()
            connection.execute(
                """
                UPDATE review_items
                SET status = 'uncertain',
                    status_reason = 'AstrBot 在人工批准后的发送过程中重启，无法确认是否送达。',
                    delivered_at = CASE WHEN delivered_at > 0 THEN delivered_at ELSE ? END,
                    updated_at = ?,
                    revision = revision + 1
                WHERE status = 'sending'
                """,
                (now, now),
            )
            self._prune_sync(connection)
            connection.commit()
        finally:
            connection.close()

    def _enqueue_sync(self, values: Mapping[str, Any]) -> sqlite3.Row:
        now = time.time()
        connection = self._connect()
        try:
            connection.execute(
                """
                INSERT INTO review_items (
                    review_key, kind, source, source_event_key, message_id,
                    user_id, user_name, incoming_text, incoming_image_urls,
                    target_json, reply_text, reply_image_sources, status,
                    status_reason, created_at, updated_at, phase
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', '', ?, ?, ?)
                ON CONFLICT(review_key) DO NOTHING
                """,
                (
                    values["review_key"],
                    values["kind"],
                    values["source"],
                    values["source_event_key"],
                    values["message_id"],
                    values["user_id"],
                    values["user_name"],
                    values["incoming_text"],
                    values["incoming_image_urls"],
                    values["target"],
                    values["reply_text"],
                    values["reply_image_sources"],
                    now,
                    now,
                    values["phase"],
                ),
            )
            row = connection.execute(
                "SELECT * FROM review_items WHERE review_key = ?",
                (values["review_key"],),
            ).fetchone()
            if row is None:
                raise RuntimeError("审核记录创建失败。")
            self._prune_sync(connection)
            connection.commit()
            return row
        finally:
            connection.close()

    def _claim_sync(
        self,
        review_id: int,
        expected_revision: int,
        reply_text: str | None,
        reply_image_sources: str | None,
    ) -> sqlite3.Row:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM review_items WHERE id = ?",
                (review_id,),
            ).fetchone()
            if row is None:
                raise KeyError("审核记录不存在。")
            if str(row["status"]) != "pending":
                raise ReviewConflictError("审核记录已被其他操作处理，请刷新页面。")
            if int(row["revision"]) != expected_revision:
                raise ReviewConflictError("审核记录内容已更新，请刷新后重试。")

            next_text = (
                str(row["reply_text"])
                if reply_text is None
                else str(reply_text)[:20_000]
            )
            next_images = (
                str(row["reply_image_sources"])
                if reply_image_sources is None
                else reply_image_sources
            )
            if not next_text.strip() and not _json_strings(next_images):
                raise ValueError("批准发送的回复正文和图片不能同时为空。")
            now = time.time()
            cursor = connection.execute(
                """
                UPDATE review_items
                SET status = 'sending', status_reason = '', reply_text = ?,
                    reply_image_sources = ?, reviewed_at = ?, updated_at = ?,
                    revision = revision + 1
                WHERE id = ? AND status = 'pending' AND revision = ?
                """,
                (
                    next_text,
                    next_images,
                    now,
                    now,
                    review_id,
                    expected_revision,
                ),
            )
            if cursor.rowcount != 1:
                raise ReviewConflictError("审核记录已被其他操作处理，请刷新页面。")
            connection.commit()
            claimed = connection.execute(
                "SELECT * FROM review_items WHERE id = ?",
                (review_id,),
            ).fetchone()
            assert claimed is not None
            return claimed
        finally:
            connection.close()

    def _transition_sync(
        self,
        review_id: int,
        expected_revision: int | None,
        from_status: str,
        to_status: str,
        reason: str,
        reviewed: bool,
        delivered: bool,
    ) -> sqlite3.Row:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM review_items WHERE id = ?",
                (review_id,),
            ).fetchone()
            if row is None:
                raise KeyError("审核记录不存在。")
            if str(row["status"]) != from_status:
                raise ReviewConflictError("审核记录已被其他操作处理，请刷新页面。")
            if (
                to_status == "approved"
                and str(row["phase"] or "") != "incoming_message"
            ):
                raise ValueError("只有先审核后生成的记录可以进入等待生成状态。")
            if (
                expected_revision is not None
                and int(row["revision"]) != expected_revision
            ):
                raise ReviewConflictError("审核记录内容已更新，请刷新后重试。")

            now = time.time()
            reviewed_at = now if reviewed else float(row["reviewed_at"] or 0)
            delivered_at = now if delivered else float(row["delivered_at"] or 0)
            params: list[Any] = [
                to_status,
                reason,
                reviewed_at,
                delivered_at,
                now,
                review_id,
                from_status,
            ]
            revision_clause = ""
            if expected_revision is not None:
                revision_clause = " AND revision = ?"
                params.append(expected_revision)
            cursor = connection.execute(
                """
                UPDATE review_items
                SET status = ?, status_reason = ?, reviewed_at = ?,
                    delivered_at = ?, updated_at = ?, revision = revision + 1
                WHERE id = ? AND status = ?
                """
                + revision_clause,
                params,
            )
            if cursor.rowcount != 1:
                raise ReviewConflictError("审核记录已被其他操作处理，请刷新页面。")
            connection.commit()
            updated = connection.execute(
                "SELECT * FROM review_items WHERE id = ?",
                (review_id,),
            ).fetchone()
            assert updated is not None
            return updated
        finally:
            connection.close()

    def _transition_by_key_sync(
        self,
        review_key: str,
        from_status: str,
        to_status: str,
        reason: str,
        delivered: bool,
    ) -> sqlite3.Row:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM review_items WHERE review_key = ?",
                (review_key,),
            ).fetchone()
            if row is None:
                raise KeyError("审核记录不存在。")
            if str(row["status"]) != from_status:
                raise ReviewConflictError("审核记录状态已经变化。")
            now = time.time()
            delivered_at = now if delivered else float(row["delivered_at"] or 0)
            cursor = connection.execute(
                """
                UPDATE review_items
                SET status = ?, status_reason = ?, delivered_at = ?,
                    updated_at = ?, revision = revision + 1
                WHERE review_key = ? AND status = ?
                """,
                (
                    to_status,
                    reason,
                    delivered_at,
                    now,
                    review_key,
                    from_status,
                ),
            )
            if cursor.rowcount != 1:
                raise ReviewConflictError("审核记录状态已经变化。")
            connection.commit()
            updated = connection.execute(
                "SELECT * FROM review_items WHERE review_key = ?",
                (review_key,),
            ).fetchone()
            assert updated is not None
            return updated
        finally:
            connection.close()

    def _search_sync(self, filters: Mapping[str, Any]) -> dict[str, Any]:
        clauses: list[str] = []
        params: list[Any] = []
        for field in ("status", "kind", "source"):
            value = str(filters[field])
            if value:
                clauses.append(f"{field} = ?")
                params.append(value)
        keyword = str(filters["keyword"])
        if keyword:
            clauses.append(
                "(incoming_text LIKE ? OR reply_text LIKE ? "
                "OR user_name LIKE ? OR user_id LIKE ?)"
            )
            pattern = f"%{keyword}%"
            params.extend([pattern, pattern, pattern, pattern])
        where = " WHERE " + " AND ".join(clauses) if clauses else ""

        connection = self._connect()
        try:
            total = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM review_items" + where,
                    params,
                ).fetchone()["count"]
            )
            rows = connection.execute(
                "SELECT * FROM review_items"
                + where
                + " ORDER BY created_at ASC, id ASC LIMIT ? OFFSET ?",
                [*params, int(filters["limit"]), int(filters["offset"])],
            ).fetchall()
            return {
                "total": total,
                "limit": int(filters["limit"]),
                "offset": int(filters["offset"]),
                "records": [
                    self._row_to_dict(
                        row,
                        include_content=bool(filters["include_content"]),
                    )
                    for row in rows
                ],
            }
        finally:
            connection.close()

    def _statistics_sync(self) -> dict[str, Any]:
        connection = self._connect()
        try:
            status_rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM review_items GROUP BY status"
            ).fetchall()
            kind_rows = connection.execute(
                "SELECT kind, COUNT(*) AS count FROM review_items GROUP BY kind"
            ).fetchall()
            return {
                "total": sum(int(row["count"]) for row in status_rows),
                "pending": next(
                    (
                        int(row["count"])
                        for row in status_rows
                        if str(row["status"]) == "pending"
                    ),
                    0,
                ),
                "status_counts": {
                    str(row["status"]): int(row["count"]) for row in status_rows
                },
                "kind_counts": {
                    str(row["kind"]): int(row["count"]) for row in kind_rows
                },
            }
        finally:
            connection.close()

    def _prune_sync(self, connection: sqlite3.Connection) -> None:
        protected = "('pending', 'approved', 'sending')"
        if self.retention_days > 0:
            cutoff = time.time() - self.retention_days * 86400
            connection.execute(
                f"""
                DELETE FROM review_items
                WHERE updated_at < ? AND status NOT IN {protected}
                """,
                (cutoff,),
            )
        connection.execute(
            f"""
            DELETE FROM review_items
            WHERE id IN (
                SELECT id FROM review_items
                WHERE status NOT IN {protected}
                ORDER BY updated_at DESC, id DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (self.max_records,),
        )

    @staticmethod
    def _row_to_dict(
        row: sqlite3.Row,
        *,
        include_content: bool,
    ) -> dict[str, Any]:
        incoming_text = str(row["incoming_text"] or "")
        reply_text = str(row["reply_text"] or "")
        if not include_content:
            incoming_text = _redacted_preview(incoming_text)
            reply_text = _redacted_preview(reply_text)
        try:
            target = json.loads(str(row["target_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            target = {}
        if not isinstance(target, dict):
            target = {}
        incoming_images = _json_strings(row["incoming_image_urls"])
        reply_images = _json_strings(row["reply_image_sources"])
        if not include_content:
            for key in (
                "comment_text",
                "replied_text",
                "text",
                "image_urls",
                "replied_image_urls",
            ):
                target.pop(key, None)
            incoming_images = []
            reply_images = []
        return {
            "id": int(row["id"]),
            "review_key": str(row["review_key"]),
            "kind": str(row["kind"]),
            "source": str(row["source"]),
            "source_event_key": str(row["source_event_key"]),
            "message_id": str(row["message_id"]),
            "user_id": str(row["user_id"]),
            "user_name": str(row["user_name"]),
            "incoming_text": incoming_text,
            "incoming_image_urls": incoming_images,
            "target": target,
            "reply_text": reply_text,
            "reply_image_sources": reply_images,
            "phase": str(row["phase"] or "generated_reply"),
            "status": str(row["status"]),
            "status_reason": str(row["status_reason"]),
            "revision": int(row["revision"]),
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
            "reviewed_at": float(row["reviewed_at"] or 0),
            "delivered_at": float(row["delivered_at"] or 0),
            "content_visible": bool(include_content),
        }


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
