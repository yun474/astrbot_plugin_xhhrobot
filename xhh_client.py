from __future__ import annotations

import asyncio
import hashlib
import html
import json
import re
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urlparse

import aiohttp
from aiohttp_socks import (
    ProxyConnectionError,
    ProxyConnector,
    ProxyError,
    ProxyTimeoutError,
)

from .media import (
    ImagePayload,
    cos_authorization,
    cos_quote,
    extract_image_urls,
    is_http_url,
    is_xhh_image_url,
    load_image_payload,
    normalize_http_image_url,
    unique_strings,
)
from .models import (
    AuthInfo,
    DirectConversation,
    DirectMessage,
    FeedPost,
    Mention,
    NotificationPage,
    PostContext,
    QrChallenge,
    QrPollResult,
    ReplyReceipt,
)
from .rich_content import (
    RichContentError,
    content_blocks_image_sources,
    content_blocks_plain_text,
    normalize_rich_content_blocks,
    parse_inbound_content_blocks,
    platform_html_for_block,
)
from .signing import get_heybox_request_keys, get_request_keys

COS_UPLOAD_INFO_PATH = "/bbs/app/api/qcloud/cos/upload/info/v2"
COS_UPLOAD_TOKEN_PATH = "/bbs/app/api/qcloud/cos/upload/token/v2"
COS_UPLOAD_CALLBACK_PATH = "/bbs/app/api/qcloud/cos/upload/callback/v2"
DEFAULT_COS_REGION = "ap-shanghai"
DIRECT_MESSAGE_API_PARAM_KEYS = frozenset(
    {
        "os_type",
        "app",
        "client_type",
        "version",
        "web_version",
        "x_client_type",
        "x_app",
        "x_os_type",
        "device_info",
        "device_id",
    }
)
DIRECT_MESSAGE_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)
LOGIN_COOKIE_NAMES = frozenset(
    {
        "user_pkey",
        "user_heybox_id",
        "heybox_id",
        "avatar",
        "level",
        "nickname",
        "x_xhh_tokenid",
    }
)

# The COS upload workflow is adapted from
# advent259141/astrbot_plugin_xiaoheihe_adapter under Apache-2.0.
# See THIRD_PARTY_NOTICES.md for the modification notice.


class XhhError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool = True,
        terminal: bool = False,
        auth_required: bool = False,
        delivery_uncertain: bool = False,
        action_restricted: bool = False,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.terminal = terminal
        self.auth_required = auth_required
        self.delivery_uncertain = delivery_uncertain
        self.action_restricted = action_restricted
        self.retry_after = retry_after


@dataclass(frozen=True, slots=True)
class _JsonResponse:
    payload: dict[str, Any]
    cookies: dict[str, str]


class XhhClient:
    def __init__(
        self,
        *,
        api_base_url: str,
        reply_base_url: str,
        version: str,
        web_version: str,
        device_id: str,
        timeout_seconds: int = 20,
        proxy_url: str = "",
        direct_message_api_params_url: str = "",
        direct_message_restriction_pause_seconds: int = 1800,
        auth: AuthInfo | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.reply_base_url = reply_base_url.rstrip("/")
        self.version = version
        self.web_version = web_version
        self.device_id = device_id
        self.timeout_seconds = max(5, timeout_seconds)
        self.proxy_url = self._validate_proxy_url(proxy_url)
        self._direct_message_api_params = self._parse_direct_message_api_params(
            direct_message_api_params_url
        )
        self._direct_message_restriction_pause_seconds = max(
            0, int(direct_message_restriction_pause_seconds)
        )
        self.auth = auth
        self._session = session
        self._owns_session = session is None
        self._login_session_active = False
        self._direct_message_ack_id = int(time.time() * 1000) % 1_000_000_000
        self._direct_message_send_lock = asyncio.Lock()
        self._last_direct_message_sent_at = 0.0
        self._direct_message_action_restriction = ""
        self._direct_message_action_restricted_until = 0.0

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            session_kwargs: dict[str, Any] = {
                "timeout": timeout,
                "cookie_jar": (
                    aiohttp.CookieJar()
                    if self._login_session_active
                    else aiohttp.DummyCookieJar()
                ),
            }
            if self.proxy_url:
                try:
                    session_kwargs["connector"] = ProxyConnector.from_url(
                        self.proxy_url,
                        rdns=True,
                    )
                except (TypeError, ValueError) as exc:
                    raise XhhError(
                        "SOCKS5 家庭代理配置无效。",
                        retryable=False,
                    ) from exc
            self._session = aiohttp.ClientSession(**session_kwargs)
            self._owns_session = True

    async def close(self) -> None:
        if (
            self._session is not None
            and self._owns_session
            and not self._session.closed
        ):
            await self._session.close()
        if self._owns_session:
            self._session = None

    def set_auth(self, auth: AuthInfo | None) -> None:
        self.auth = auth
        self._direct_message_action_restriction = ""
        self._direct_message_action_restricted_until = 0.0

    async def begin_qr_login(self) -> QrChallenge:
        self._login_session_active = True
        await self._restart_owned_session()
        try:
            response = await self._request_json(
                "GET",
                "/account/get_qrcode_url/",
                params={"app": "web", "_notip": "true"},
                auth_required=False,
                allow_api_failure=False,
                login_request=True,
            )
        except BaseException:
            await self._finish_login_session()
            raise
        result = self._result_mapping(response.payload)
        qr_url = str(result.get("qr_url") or "").strip()
        if not qr_url:
            await self._finish_login_session()
            raise XhhError("小黑盒没有返回登录二维码地址。", retryable=False)
        query = dict(parse_qsl(urlparse(qr_url).query, keep_blank_values=True))
        qr_id = str(query.get("qr") or "").strip()
        if not qr_id:
            await self._finish_login_session()
            raise XhhError("小黑盒登录二维码缺少二维码 ID。", retryable=False)
        expires_in = self._to_int(result.get("expire"), 120)
        return QrChallenge(
            qr_url=qr_url,
            state_params={"qr": qr_id},
            expires_in=max(30, expires_in),
        )

    async def poll_qr_login(self, challenge: QrChallenge) -> QrPollResult:
        qr_id = str(challenge.state_params.get("qr") or "").strip()
        if not qr_id:
            await self._finish_login_session()
            raise XhhError("登录二维码缺少二维码 ID。", retryable=False)
        try:
            response = await self._request_json(
                "GET",
                "/account/qr_state/",
                params={"qr": qr_id, "app": "web"},
                auth_required=False,
                allow_api_failure=False,
                login_request=True,
            )
        except BaseException:
            await self._finish_login_session()
            raise
        result = self._result_mapping(response.payload)
        state = str(result.get("error") or "").strip().lower()
        message = str(
            result.get("error_msg") or response.payload.get("msg") or ""
        ).strip()

        if state == "ok":
            cookies = self._filter_login_cookies(self._all_session_cookies())
            cookies.update(self._filter_login_cookies(response.cookies))
            heybox_id = str(
                result.get("heyboxid")
                or result.get("heybox_id")
                or cookies.get("heybox_id")
                or cookies.get("user_heybox_id")
                or ""
            ).strip()
            cookie_header = self._format_cookie_header(cookies)
            if not cookie_header:
                await self._finish_login_session()
                return QrPollResult("failed", "登录成功但没有取得 Cookie。")
            if not heybox_id:
                await self._finish_login_session()
                return QrPollResult("failed", "登录成功但没有取得账号 ID。")
            auth = AuthInfo(
                cookie=cookie_header,
                heybox_id=heybox_id,
                nickname=str(result.get("nickname") or "").strip(),
                login_at=int(time.time()),
            )
            self.set_auth(auth)
            await self._finish_login_session()
            return QrPollResult("success", message or "登录成功。", auth)

        combined = f"{state} {message}".lower()
        if any(
            word in combined
            for word in ("expire", "expired", "timeout", "过期", "失效")
        ):
            await self._finish_login_session()
            return QrPollResult("expired", message or "二维码已过期。")
        if any(
            word in combined
            for word in ("cancel", "canceled", "denied", "拒绝", "取消")
        ):
            await self._finish_login_session()
            return QrPollResult("failed", message or "登录已取消。")
        return QrPollResult("pending", message or "等待扫码确认。")

    async def _restart_owned_session(self) -> None:
        if not self._owns_session:
            return
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        await self.start()

    async def end_qr_login(self) -> None:
        await self._finish_login_session()

    async def _finish_login_session(self) -> None:
        if not self._login_session_active:
            return
        self._login_session_active = False
        if self._owns_session:
            await self._restart_owned_session()
            return
        if self._session is not None:
            try:
                self._session.cookie_jar.clear()
            except (AttributeError, TypeError):
                pass

    async def fetch_mentions(
        self, *, offset: int = 0, limit: int = 20
    ) -> list[Mention]:
        page = await self.fetch_mentions_page(offset=offset, limit=limit)
        return list(page.items)

    async def fetch_mentions_page(
        self,
        *,
        offset: int = 0,
        limit: int = 20,
    ) -> NotificationPage:
        payload = await self.fetch_messages(
            message_type="16",
            offset=offset,
            limit=limit,
        )
        return self.parse_notification_page(payload, source="mention")

    async def fetch_comment_messages(
        self,
        *,
        offset: int = 0,
        limit: int = 20,
    ) -> list[Mention]:
        page = await self.fetch_comment_messages_page(offset=offset, limit=limit)
        return list(page.items)

    async def fetch_comment_messages_page(
        self,
        *,
        offset: int = 0,
        limit: int = 20,
    ) -> NotificationPage:
        payload = await self.fetch_messages(
            message_type="",
            offset=offset,
            limit=limit,
        )
        return self.parse_notification_page(
            payload,
            source="own_post_comment",
            allowed_message_types={"1", "2"},
            source_by_message_type={"2": "comment_reply"},
        )

    async def fetch_notifications(
        self,
        *,
        kind: str = "all",
        offset: int = 0,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Merge mentions and replies to the current account into one feed.

        The upstream message endpoint has separate views for mentions and comment
        replies. Fetching from the beginning of each view lets the caller use one
        stable, time-sorted offset across both sources.
        """

        normalized_kind = str(kind or "all").strip().lower()
        if normalized_kind not in {"all", "mention", "comment"}:
            raise XhhError("通知类型只能是 all、mention 或 comment。", retryable=False)

        normalized_offset = max(0, self._to_int(offset))
        normalized_limit = max(1, min(50, self._to_int(limit, 20)))
        target_count = normalized_offset + normalized_limit
        max_window = 200
        if target_count > max_window:
            raise XhhError(
                f"通知分页单次最多读取前 {max_window} 条，请缩小 offset 或 limit。",
                retryable=False,
            )

        async def collect(
            fetch_page: Any,
        ) -> tuple[list[Mention], int]:
            items: list[Mention] = []
            raw_count = 0
            source_offset = 0
            while len(items) < target_count:
                request_limit = min(50, target_count - len(items))
                page = await fetch_page(offset=source_offset, limit=request_limit)
                items.extend(page.items)
                raw_count += page.raw_count
                if page.raw_count < request_limit:
                    break
                source_offset += request_limit
            return items, raw_count

        sources: list[tuple[str, Any]] = []
        if normalized_kind in {"all", "mention"}:
            sources.append(("mention", self.fetch_mentions_page))
        if normalized_kind in {"all", "comment"}:
            sources.append(("own_post_comment", self.fetch_comment_messages_page))

        collected = await asyncio.gather(
            *(collect(fetch_page) for _, fetch_page in sources)
        )
        fetched_counts = {
            source: {
                "items": len(items),
                "raw_messages": raw_count,
            }
            for (source, _), (items, raw_count) in zip(sources, collected)
        }

        deduped: dict[tuple[Any, ...], Mention] = {}
        for items, _ in collected:
            for item in items:
                if item.link_id > 0 and item.comment_id > 0:
                    key = ("comment", item.link_id, item.comment_id)
                elif item.message_id > 0:
                    key = ("message", item.message_id)
                else:
                    key = (
                        "fallback",
                        item.source,
                        item.user_id,
                        item.message_time,
                        item.comment_text,
                    )
                existing = deduped.get(key)
                if existing is None or (item.message_time, item.message_id) > (
                    existing.message_time,
                    existing.message_id,
                ):
                    deduped[key] = item

        ordered = sorted(
            deduped.values(),
            key=lambda item: (item.message_time, item.message_id, item.comment_id),
            reverse=True,
        )
        page_items = ordered[normalized_offset : normalized_offset + normalized_limit]
        return {
            "kind": normalized_kind,
            "offset": normalized_offset,
            "limit": normalized_limit,
            "items": [item.to_dict() for item in page_items],
            "returned": len(page_items),
            "available_in_window": len(ordered),
            "fetched_source_counts": fetched_counts,
        }

    @classmethod
    def parse_notification_page(
        cls,
        payload: Mapping[str, Any],
        *,
        source: str,
        allowed_message_types: set[str] | None = None,
        source_by_message_type: Mapping[str, str] | None = None,
    ) -> NotificationPage:
        result = cls._result_mapping(dict(payload))
        raw_messages = result.get("messages")
        if not isinstance(raw_messages, list):
            return NotificationPage()

        message_ids: list[int] = []
        items: list[Mention] = []
        for raw in raw_messages:
            if not isinstance(raw, Mapping):
                continue
            message_id = cls._to_int(raw.get("message_id"))
            if message_id > 0:
                message_ids.append(message_id)
            if (
                allowed_message_types is not None
                and str(raw.get("message_type") or "") not in allowed_message_types
            ):
                continue
            message_type = str(raw.get("message_type") or "")
            item_source = (
                str((source_by_message_type or {}).get(message_type) or source)
            )
            mention = Mention.from_mapping(raw, source=item_source)
            if mention.message_id > 0:
                items.append(mention)
        return NotificationPage(
            items=tuple(items),
            message_ids=tuple(message_ids),
            raw_count=len(raw_messages),
        )

    async def fetch_post_context(self, link_id: int) -> PostContext:
        response = await self._request_json(
            "GET",
            "/bbs/app/link/tree",
            params={"h_src": "", "link_id": str(link_id)},
            auth_required=True,
        )
        result = self._result_mapping(response.payload)
        link = result.get("link")
        if not isinstance(link, Mapping):
            raise XhhError(
                "帖子详情响应中缺少 link 数据。", terminal=True, retryable=False
            )

        content_blocks = list(parse_inbound_content_blocks(link.get("text")))
        known_images = set(content_blocks_image_sources(content_blocks))
        for image_url in extract_image_urls(link.get("text")):
            image_url = self._normalise_media_url(image_url)
            if image_url and image_url not in known_images:
                content_blocks.append({"type": "image", "url": image_url})
                known_images.add(image_url)
        text_parts = [
            content_blocks_plain_text([block])
            for block in content_blocks
            if block.get("type") != "image"
        ]
        image_urls = [
            self._normalise_media_url(url)
            for url in content_blocks_image_sources(content_blocks)
        ]

        user = link.get("user") or link.get("author") or link.get("userinfo")
        user = user if isinstance(user, Mapping) else {}
        return PostContext(
            title=str(link.get("title") or "").strip(),
            author_id=str(
                user.get("heybox_id")
                or user.get("user_heybox_id")
                or user.get("userid")
                or user.get("user_id")
                or user.get("uid")
                or user.get("id")
                or ""
            ).strip(),
            author_name=str(
                user.get("username") or user.get("nickname") or user.get("name") or ""
            ).strip(),
            text_parts=tuple(part for part in text_parts if part),
            image_urls=tuple(dict.fromkeys(image_urls)),
            content_blocks=tuple(content_blocks),
            topics=tuple(self._extract_names(link.get("topics"))),
            tags=tuple(self._extract_names(link.get("hashtags"))),
        )

    async def fetch_messages(
        self,
        *,
        message_type: str = "16",
        list_type: str = "0",
        offset: int = 0,
        limit: int = 20,
    ) -> dict[str, Any]:
        params = {
            "list_type": str(list_type or "0"),
            "offset": str(max(0, offset)),
            "limit": str(max(1, min(50, limit))),
            "no_more": "false",
        }
        if message_type:
            params["message_type"] = str(message_type)
        response = await self._request_json(
            "GET",
            "/bbs/app/user/message",
            params=params,
            auth_required=True,
        )
        return response.payload

    async def fetch_feed(
        self, *, offset: int = 0, pull: bool = False
    ) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/app/feeds",
            params={
                "offset": str(max(0, offset)),
                "pull": "1" if pull else "0",
                "use_history": "0" if pull else "1",
                "is_first": "1" if offset <= 0 else "0",
            },
            auth_required=True,
        )
        return response.payload

    async def fetch_feed_posts(
        self,
        *,
        offset: int = 0,
        pull: bool = True,
        limit: int = 20,
    ) -> list[FeedPost]:
        payload = await self.fetch_feed(offset=offset, pull=pull)
        return self.parse_feed_posts(payload, limit=limit)

    @classmethod
    def parse_feed_posts(
        cls,
        payload: Mapping[str, Any],
        *,
        limit: int = 20,
    ) -> list[FeedPost]:
        result = payload.get("result")
        result = result if isinstance(result, Mapping) else {}
        raw_links: Any = []
        for value in (
            result.get("links"),
            result.get("feeds"),
            result.get("list"),
            payload.get("links"),
        ):
            if isinstance(value, list):
                raw_links = value
                break

        posts: list[FeedPost] = []
        seen: set[int] = set()
        for raw_item in raw_links:
            if not isinstance(raw_item, Mapping):
                continue
            nested = raw_item.get("link")
            link = nested if isinstance(nested, Mapping) else raw_item
            link_id = cls._to_int(
                link.get("linkid")
                or link.get("link_id")
                or link.get("id")
                or link.get("linkId")
            )
            if link_id <= 0 or link_id in seen:
                continue
            seen.add(link_id)

            user = link.get("user") or link.get("author") or link.get("userinfo")
            user = user if isinstance(user, Mapping) else {}
            author_id = str(
                user.get("heybox_id")
                or user.get("user_heybox_id")
                or user.get("userid")
                or user.get("user_id")
                or user.get("uid")
                or user.get("id")
                or ""
            ).strip()
            author_name = str(
                user.get("username") or user.get("nickname") or user.get("name") or ""
            ).strip()
            created_at = cls._to_int(
                link.get("create_at")
                or link.get("created_at")
                or link.get("create_time")
                or link.get("time")
                or link.get("timestamp")
            )
            if created_at > 100_000_000_000:
                created_at //= 1000

            posts.append(
                FeedPost(
                    link_id=link_id,
                    title=str(
                        link.get("title")
                        or link.get("topic_title")
                        or link.get("name")
                        or ""
                    ).strip(),
                    description=str(
                        link.get("description")
                        or link.get("desc")
                        or link.get("summary")
                        or ""
                    ).strip(),
                    author_id=author_id,
                    author_name=author_name,
                    created_at=created_at,
                    likes=cls._to_int(
                        link.get("up") or link.get("like_num") or link.get("like_count")
                    ),
                    comments=cls._to_int(
                        link.get("comment_num")
                        or link.get("comments_num")
                        or link.get("comment_count")
                    ),
                    topics=tuple(cls._extract_names(link.get("topics"))),
                    tags=tuple(
                        cls._extract_names(link.get("hashtags") or link.get("tags"))
                    ),
                )
            )
            if len(posts) >= max(1, min(50, int(limit or 20))):
                break
        return posts

    async def search(
        self,
        query: str,
        *,
        search_type: str = "link",
        offset: int = 0,
        limit: int = 10,
        time_range: str = "",
        filter_tag: str = "",
    ) -> dict[str, Any]:
        allowed_types = {"general", "link", "game", "user", "hashtag", "mall"}
        normalized_type = search_type if search_type in allowed_types else "link"
        params = {
            "q": query,
            "search_type": normalized_type,
            "offset": str(max(0, offset)),
            "limit": str(max(1, min(30, limit))),
            "is_pull_down": "0",
            "dw": "628",
        }
        if time_range:
            params["time_range"] = time_range
        if filter_tag:
            params["filter_tag"] = filter_tag
        response = await self._request_json(
            "GET",
            "/bbs/app/api/general/search/v1",
            params=params,
            auth_required=True,
        )
        return response.payload

    async def fetch_post(
        self,
        link_id: int,
        *,
        page: int = 1,
        limit: int = 20,
        sort_filter: str = "hot",
        owner_only: bool = False,
    ) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/app/link/tree",
            params={
                "h_src": "",
                "link_id": str(link_id),
                "is_first": "1" if page <= 1 else "0",
                "page": str(max(1, page)),
                "index": str(max(1, page)),
                "limit": str(max(1, min(50, limit))),
                "owner_only": "1" if owner_only else "0",
                "sort_filter": sort_filter if sort_filter in {"hot", "time"} else "hot",
            },
            auth_required=True,
        )
        return response.payload

    async def fetch_sub_comments(
        self,
        root_comment_id: int,
        *,
        last_value: int = 0,
    ) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/app/comment/sub/comments",
            params={
                "root_comment_id": str(root_comment_id),
                "lastval": str(max(0, last_value)),
            },
            auth_required=True,
        )
        return response.payload

    async def fetch_user_profile(self, user_id: str) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/app/profile/user/profile",
            params={"userid": user_id},
            auth_required=True,
        )
        return response.payload

    async def fetch_user_posts(
        self,
        user_id: str,
        *,
        offset: int = 0,
        limit: int = 20,
    ) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/web/profile/post/links",
            params={
                "userid": user_id,
                "offset": str(max(0, offset)),
                "limit": str(max(1, min(50, limit))),
                "post_type": "1",
            },
            auth_required=True,
        )
        return response.payload

    async def fetch_user_comments(
        self,
        user_id: str,
        *,
        offset: int = 0,
        limit: int = 20,
    ) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/web/profile/post/comments",
            params={
                "userid": user_id,
                "offset": str(max(0, offset)),
                "limit": str(max(1, min(50, limit))),
            },
            auth_required=True,
        )
        return response.payload

    async def fetch_user_events(
        self,
        user_id: str,
        *,
        offset: int = 0,
        limit: int = 20,
    ) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/app/profile/events",
            params={
                "userid": user_id,
                "list_type": "moment",
                "offset": str(max(0, offset)),
                "limit": str(max(1, min(50, limit))),
            },
            auth_required=True,
        )
        return response.payload

    async def fetch_user_relations(
        self,
        user_id: str,
        *,
        relation: str,
        offset: int = 0,
        limit: int = 20,
    ) -> dict[str, Any]:
        path = (
            "/bbs/app/profile/follower/list"
            if relation == "followers"
            else "/bbs/app/profile/following/list"
        )
        response = await self._request_json(
            "GET",
            path,
            params={
                "userid": user_id,
                "offset": str(max(0, offset)),
                "limit": str(max(1, min(50, limit))),
            },
            auth_required=True,
        )
        return response.payload

    async def fetch_topics(self) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/app/api/topic/index/",
            params={"type": "list", "is_post": "1", "post_tab": "1"},
            auth_required=True,
        )
        return response.payload

    async def search_topics(self, query: str) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/app/api/post_editor/topic_selection/search",
            params={"q": query},
            auth_required=True,
        )
        return response.payload

    async def fetch_favorite_folders(self) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/app/profile/fav/folders",
            auth_required=True,
        )
        return response.payload

    async def fetch_my_favorites(
        self,
        *,
        offset: int = 0,
        limit: int = 20,
    ) -> dict[str, Any]:
        user_id = self._current_auth_user_id()
        normalized_offset = max(0, self._to_int(offset))
        normalized_limit = max(1, min(50, self._to_int(limit, 20)))
        response = await self._request_json(
            "GET",
            "/bbs/web/profile/favours",
            params={
                "userid": user_id,
                "offset": str(normalized_offset),
                "limit": str(normalized_limit),
            },
            auth_required=True,
        )
        result = self._result_mapping(response.payload)
        items = self._first_mapping_list(
            result.get("items"),
            result.get("links"),
            result.get("favours"),
            result.get("result"),
            response.payload.get("result"),
            response.payload.get("items"),
            response.payload.get("links"),
            response.payload.get("favours"),
        )
        return {
            "account_id": user_id,
            "offset": normalized_offset,
            "limit": normalized_limit,
            "total_page": self._to_int(
                result.get("total_page") or response.payload.get("total_page")
            ),
            "items": [self._summarize_profile_link(item) for item in items],
        }

    async def fetch_remote_drafts(self) -> dict[str, Any]:
        user_id = self._current_auth_user_id()
        response = await self._request_json(
            "GET",
            "/bbs/app/link/drafts",
            auth_required=True,
        )
        result = self._result_mapping(response.payload)
        items = self._first_mapping_list(
            result.get("links"),
            result.get("items"),
            response.payload.get("result"),
            response.payload.get("links"),
        )
        return {
            "account_id": user_id,
            "drafts": [self._summarize_remote_draft(item) for item in items],
            "count": len(items),
        }

    async def fetch_emojis(self) -> dict[str, Any]:
        response = await self._request_json(
            "GET",
            "/bbs/app/api/emojis/list",
            auth_required=True,
        )
        return response.payload

    async def fetch_direct_message_entries(
        self,
        *,
        limit: int = 20,
        strangers: bool = False,
    ) -> dict[str, Any]:
        if strangers:
            path = "/chat/stranger_messages/"
            params = {
                "offset": "0",
                "limit": str(max(1, min(50, limit))),
            }
        else:
            path = "/bbs/app/user/message"
            params = {
                "list_type": "2",
                "offset": "0",
                "limit": str(max(1, min(50, limit))),
            }
        response = await self._request_json(
            "GET",
            path,
            params=params,
            auth_required=True,
        )
        return response.payload

    async def fetch_direct_messages(
        self,
        user_id: str,
        *,
        limit: int = 30,
        sequence: str = "",
    ) -> dict[str, Any]:
        params = {
            "to_user_id": user_id,
            "offset": "0",
            "limit": str(max(1, min(50, limit))),
        }
        if sequence:
            params["seq"] = sequence
        response = await self._request_json(
            "GET",
            "/chatroom/v2/msg/user",
            params=params,
            auth_required=True,
        )
        return response.payload

    @staticmethod
    def parse_direct_conversations(
        payload: Mapping[str, Any],
        *,
        source: str,
    ) -> list[DirectConversation]:
        result = payload.get("result")
        result = result if isinstance(result, Mapping) else {}
        raw_items = (
            result.get("messages")
            or result.get("list")
            or result.get("items")
            or payload.get("messages")
            or payload.get("list")
            or []
        )
        if not isinstance(raw_items, list):
            return []
        conversations: list[DirectConversation] = []
        seen: set[str] = set()
        for item in raw_items:
            if not isinstance(item, Mapping):
                continue
            if source == "direct_message" and str(item.get("entry") or "") not in {
                "",
                "message",
            }:
                continue
            conversation = DirectConversation.from_mapping(item, source=source)
            if conversation is None or conversation.user_id in seen:
                continue
            seen.add(conversation.user_id)
            conversations.append(conversation)
        return conversations

    def parse_direct_messages(
        self,
        payload: Mapping[str, Any],
        *,
        conversation: DirectConversation,
    ) -> list[DirectMessage]:
        result = payload.get("result")
        result = result if isinstance(result, Mapping) else {}
        raw_items = (
            result.get("list")
            or result.get("messages")
            or result.get("items")
            or payload.get("list")
            or []
        )
        if not isinstance(raw_items, list):
            return []
        self_user_id = self.auth.heybox_id if self.auth is not None else ""
        messages: list[DirectMessage] = []
        seen: set[str] = set()
        for item in raw_items:
            if not isinstance(item, Mapping):
                continue
            message = DirectMessage.from_mapping(
                item,
                conversation=conversation,
                self_user_id=self_user_id,
            )
            if message is None or message.event_key in seen:
                continue
            seen.add(message.event_key)
            messages.append(message)
        messages.sort(key=lambda item: (item.timestamp, item.message_id))
        return messages

    async def copy_image_by_url(self, image_url: str) -> str:
        try:
            image_url = normalize_http_image_url(image_url)
        except ValueError as exc:
            raise XhhError(str(exc), retryable=False) from exc
        if is_xhh_image_url(image_url):
            return image_url
        response = await self._request_json(
            "GET",
            "/bbs/app/api/qcloud/cos/copy/image/by/url",
            params={"target_url": image_url, "watermark": "false"},
            auth_required=True,
        )
        result = self._result_mapping(response.payload)
        copied = str(result.get("url") or result.get("preview_url") or "").strip()
        if not copied:
            raise XhhError("小黑盒图片转存响应中缺少 URL。", retryable=False)
        return self._normalise_media_url(copied)

    async def prepare_image_sources(
        self,
        image_sources: Iterable[Any],
        *,
        allowed_local_roots: Sequence[Path] = (),
        max_local_image_bytes: int = 20 * 1024 * 1024,
    ) -> list[str]:
        prepared: list[str] = []
        for source in unique_strings(image_sources):
            if is_http_url(source):
                prepared.append(await self.copy_image_by_url(source))
                continue
            try:
                payload = await asyncio.to_thread(
                    load_image_payload,
                    source,
                    max_bytes=max(1, int(max_local_image_bytes)),
                    allowed_roots=allowed_local_roots,
                )
            except ValueError as exc:
                raise XhhError(str(exc), retryable=False) from exc
            prepared.append(await self.upload_image_payload_to_cos(payload))
        return unique_strings(prepared)

    async def upload_local_image_to_cos(
        self,
        image_source: Any,
        *,
        allowed_local_roots: Sequence[Path] = (),
        max_local_image_bytes: int = 20 * 1024 * 1024,
    ) -> str:
        try:
            payload = await asyncio.to_thread(
                load_image_payload,
                image_source,
                max_bytes=max(1, int(max_local_image_bytes)),
                allowed_roots=allowed_local_roots,
            )
        except ValueError as exc:
            raise XhhError(str(exc), retryable=False) from exc
        return await self.upload_image_payload_to_cos(payload)

    async def upload_image_payload_to_cos(self, image: ImagePayload) -> str:
        if self.auth is None or not self.auth.heybox_id:
            raise XhhError(
                "登录凭据中缺少 heybox_id。",
                auth_required=True,
                retryable=False,
            )
        upload_info = await self._request_cos_upload_info(image)
        keys = upload_info.get("keys")
        keys = keys if isinstance(keys, list) else []
        key = str((keys[0] if keys else upload_info.get("key")) or "").strip()
        bucket = str(upload_info.get("bucket") or "").strip()
        region = str(upload_info.get("region") or DEFAULT_COS_REGION).strip()
        if not key or not bucket:
            raise XhhError("图片上传初始化响应缺少 bucket 或 key。", retryable=False)
        token = await self._request_cos_upload_token(
            bucket=bucket,
            keys=[key],
            mimetypes=[image.mimetype],
        )
        await self._put_cos_object(
            image=image,
            bucket=bucket,
            region=region,
            key=key,
            token=token,
        )
        return await self._finish_cos_upload([key])

    async def _request_cos_upload_info(
        self,
        image: ImagePayload,
    ) -> Mapping[str, Any]:
        response = await self._request_json(
            "POST",
            COS_UPLOAD_INFO_PATH,
            data={
                "file_infos": json.dumps(
                    [
                        {
                            "name": image.name,
                            "mimetype": image.mimetype,
                            "fsize": len(image.data),
                            "width": image.width,
                            "height": image.height,
                            "duration": image.duration,
                        }
                    ],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "scope": "bbs",
                "need_cache": "0",
            },
            auth_required=True,
        )
        return self._result_mapping(response.payload)

    async def _request_cos_upload_token(
        self,
        *,
        bucket: str,
        keys: list[str],
        mimetypes: list[str],
    ) -> Mapping[str, Any]:
        response = await self._request_json(
            "POST",
            COS_UPLOAD_TOKEN_PATH,
            data={
                "bucket": bucket,
                "keys": json.dumps(keys, ensure_ascii=False, separators=(",", ":")),
                "mimetypes": json.dumps(
                    mimetypes, ensure_ascii=False, separators=(",", ":")
                ),
                "is_multipart_upload": "0",
            },
            auth_required=True,
        )
        result = self._result_mapping(response.payload)
        credentials = result.get("credentials")
        credentials = credentials if isinstance(credentials, Mapping) else {}
        if not all(
            str(credentials.get(key) or "").strip()
            for key in ("tmpSecretId", "tmpSecretKey", "sessionToken")
        ):
            raise XhhError("图片上传授权响应缺少临时凭证。", retryable=False)
        return result

    async def _put_cos_object(
        self,
        *,
        image: ImagePayload,
        bucket: str,
        region: str,
        key: str,
        token: Mapping[str, Any],
    ) -> None:
        credentials = token.get("credentials")
        credentials = credentials if isinstance(credentials, Mapping) else {}
        secret_id = str(credentials.get("tmpSecretId") or "").strip()
        secret_key = str(credentials.get("tmpSecretKey") or "").strip()
        session_token = str(credentials.get("sessionToken") or "").strip()
        if not secret_id or not secret_key or not session_token:
            raise XhhError("图片上传授权响应缺少临时凭证。", retryable=False)

        now = int(time.time())
        start_time = self._to_int(token.get("startTime"), max(0, now - 60))
        end_time = self._to_int(token.get("expiredTime"), now + 300)
        host = f"{bucket}.cos.{region}.myqcloud.com"
        object_path = "/" + key.lstrip("/")
        headers = {
            "Host": host,
            "Content-Type": image.mimetype,
            "x-cos-security-token": session_token,
        }
        headers["Authorization"] = cos_authorization(
            secret_id=secret_id,
            secret_key=secret_key,
            method="PUT",
            path=object_path,
            headers=headers,
            start_time=start_time,
            end_time=end_time,
        )
        await self.start()
        assert self._session is not None
        url = f"https://{host}{cos_quote(object_path)}"
        try:
            async with self._session.request(
                "PUT",
                url,
                data=image.data,
                headers=headers,
            ) as response:
                if response.status < 200 or response.status >= 300:
                    raw = await response.text(errors="replace")
                    raise XhhError(
                        f"COS 图片上传失败（HTTP {response.status}）：{raw[:200]}",
                        retryable=response.status == 429 or response.status >= 500,
                    )
        except XhhError:
            raise
        except (
            asyncio.TimeoutError,
            aiohttp.ClientError,
            ProxyConnectionError,
            ProxyError,
            ProxyTimeoutError,
        ) as exc:
            raise XhhError(
                f"COS 图片上传失败：{self._safe_network_error(exc)}",
                retryable=True,
            ) from exc

    async def _finish_cos_upload(self, keys: list[str]) -> str:
        response = await self._request_json(
            "POST",
            COS_UPLOAD_CALLBACK_PATH,
            params={"is_finished": "true"},
            data={"keys": json.dumps(keys, ensure_ascii=False, separators=(",", ":"))},
            auth_required=True,
        )
        result = self._result_mapping(response.payload)
        preview_urls = result.get("preview_urls")
        preview_urls = preview_urls if isinstance(preview_urls, list) else []
        thumbs = result.get("thumbs")
        thumbs = thumbs if isinstance(thumbs, list) else []
        image_url = str(
            (preview_urls[0] if preview_urls else "")
            or (thumbs[0] if thumbs else "")
            or ""
        ).strip()
        if not image_url:
            raise XhhError("图片上传回调响应缺少预览 URL。", retryable=False)
        return self._normalise_media_url(image_url)

    async def publish_post(
        self,
        *,
        title: str,
        body: str,
        description: str = "",
        topic_ids: list[str] | None = None,
        hashtags: list[str] | None = None,
        image_urls: list[str] | None = None,
        content_blocks: Sequence[Mapping[str, Any]] | None = None,
        allowed_local_roots: Sequence[Path] = (),
        max_local_image_bytes: int = 20 * 1024 * 1024,
    ) -> dict[str, Any]:
        try:
            blocks = normalize_rich_content_blocks(
                content_blocks or [],
                max_text_chars=100_000,
            )
        except RichContentError as exc:
            raise XhhError(str(exc), retryable=False) from exc

        if blocks and not body and blocks[0].get("type") == "image":
            raise XhhError(
                "使用 content_blocks 发帖时，第一项必须是 text 或 html 内容块。",
                retryable=False,
            )
        if body:
            blocks.insert(0, {"type": "text", "text": body})

        content: list[dict[str, str]] = []
        for block in blocks:
            if block["type"] == "image":
                copied = await self.prepare_image_sources(
                    [block["url"]],
                    allowed_local_roots=allowed_local_roots,
                    max_local_image_bytes=max_local_image_bytes,
                )
                content.extend({"type": "img", "url": url} for url in copied)
                continue
            try:
                content.append(
                    {"type": "html", "text": platform_html_for_block(block)}
                )
            except RichContentError as exc:
                raise XhhError(str(exc), retryable=False) from exc

        copied_images = await self.prepare_image_sources(
            image_urls or [],
            allowed_local_roots=allowed_local_roots,
            max_local_image_bytes=max_local_image_bytes,
        )
        content.extend({"type": "img", "url": url} for url in copied_images)
        if not content:
            raise XhhError("帖子正文和图片不能同时为空。", retryable=False)

        content_text = content_blocks_plain_text(blocks)
        payload = await self._write_json(
            "/bbs/app/api/link/post",
            data={
                "title": title,
                "desc": (description or content_text or title)[:100],
                "post_type": "1",
                "words_count": str(len(content_text)),
                "topic_ids": ",".join(topic_ids or []),
                "hashtags": json.dumps(
                    hashtags or [], ensure_ascii=False, separators=(",", ":")
                ),
                "text": json.dumps(content, ensure_ascii=False, separators=(",", ":")),
                "link_tag": "11",
            },
        )
        result = self._result_mapping(payload)
        link_id = payload.get("link_id") or result.get("link_id")
        if not str(link_id or "").strip():
            raise XhhError(
                "小黑盒发帖响应中缺少 link_id，无法确认帖子已发布。", retryable=False
            )
        return payload

    async def create_comment(
        self,
        *,
        text: str,
        link_id: int,
        reply_id: int = -1,
        root_id: int = -1,
        image_urls: list[str] | None = None,
        allowed_local_roots: Sequence[Path] = (),
        max_local_image_bytes: int = 20 * 1024 * 1024,
    ) -> dict[str, Any]:
        copied_images = await self.prepare_image_sources(
            image_urls or [],
            allowed_local_roots=allowed_local_roots,
            max_local_image_bytes=max_local_image_bytes,
        )
        data = {
            "is_cy": "0",
            "link_id": str(link_id),
            "reply_id": str(reply_id),
            "root_id": str(root_id),
            "text": text,
        }
        if copied_images:
            data["imgs"] = ";".join(copied_images)
        return await self._write_json(
            "/bbs/app/comment/create",
            data=data,
            use_reply_api=True,
        )

    async def set_favorite(
        self,
        *,
        link_id: int,
        favorite: bool,
        folder_id: str = "",
    ) -> dict[str, Any]:
        if self.auth is None or not self.auth.heybox_id:
            raise XhhError(
                "登录凭据中缺少 heybox_id。", auth_required=True, retryable=False
            )
        data = {
            "link_id": str(link_id),
            "userid": self.auth.heybox_id,
            "favour_type": "1" if favorite else "2",
        }
        if folder_id:
            data["folder_id"] = folder_id
        return await self._write_json(
            "/bbs/app/link/favour",
            data=data,
            use_reply_api=True,
        )

    async def set_post_like(self, *, link_id: int, liked: bool) -> dict[str, Any]:
        return await self._write_json(
            "/bbs/app/profile/award/link",
            data={"link_id": str(link_id), "award_type": "1" if liked else "0"},
            use_reply_api=True,
        )

    async def set_comment_like(self, *, comment_id: int, liked: bool) -> dict[str, Any]:
        return await self._write_json(
            "/bbs/app/comment/support",
            data={"comment_id": str(comment_id), "support_type": "1" if liked else "2"},
            use_reply_api=True,
        )

    async def set_follow(
        self,
        *,
        user_id: str,
        followed: bool,
        link_id: int = 0,
    ) -> dict[str, Any]:
        path = (
            "/bbs/app/profile/follow/user"
            if followed
            else "/bbs/app/profile/follow/user/cancel"
        )
        data = {"following_id": user_id}
        if link_id > 0:
            data["link_id"] = str(link_id)
        return await self._write_json(path, data=data)

    async def delete_post(self, *, link_id: int) -> dict[str, Any]:
        return await self._write_json(
            "/bbs/app/link/delete",
            data={"link_id": str(link_id)},
        )

    async def send_direct_message(
        self,
        *,
        user_id: str,
        text: str,
        image_url: str = "",
        image_sources: Sequence[str] | None = None,
        allowed_local_roots: Sequence[Path] = (),
        max_local_image_bytes: int = 20 * 1024 * 1024,
        cooldown_seconds: int = 0,
    ) -> dict[str, Any]:
        self._ensure_direct_message_sending_allowed()
        sources = [*(image_sources or [])]
        if image_url:
            sources.insert(0, image_url)
        prepared = await self.prepare_image_sources(
            sources[:1],
            allowed_local_roots=allowed_local_roots,
            max_local_image_bytes=max_local_image_bytes,
        )
        copied_image = prepared[0] if prepared else ""
        return await self._send_direct_message_prepared(
            user_id=user_id,
            text=text,
            image_url=copied_image,
            cooldown_seconds=cooldown_seconds,
        )

    async def _send_direct_message_prepared(
        self,
        *,
        user_id: str,
        text: str,
        image_url: str,
        cooldown_seconds: int,
    ) -> dict[str, Any]:
        if not str(user_id or "").strip():
            raise XhhError("缺少私信目标用户 ID。", retryable=False)
        if not str(text or "").strip() and not image_url:
            raise XhhError("私信内容和图片不能同时为空。", retryable=False)
        async with self._direct_message_send_lock:
            self._ensure_direct_message_sending_allowed()
            cooldown = max(0, int(cooldown_seconds))
            elapsed = time.time() - self._last_direct_message_sent_at
            if cooldown and elapsed < cooldown:
                await asyncio.sleep(cooldown - elapsed)
            self._direct_message_ack_id += 1
            try:
                payload = await self._write_json(
                    "/chatroom/v2/msg/user",
                    params={"to_user_id": str(user_id)},
                    data={
                        "heybox_ack_id": str(self._direct_message_ack_id),
                        "img": image_url,
                        "msg": str(text or ""),
                        "msg_type": "6",
                    },
                    direct_message_request=True,
                )
            except XhhError as exc:
                if exc.action_restricted:
                    self._direct_message_action_restriction = str(exc)
                    self._direct_message_action_restricted_until = (
                        time.time() + self._direct_message_restriction_pause_seconds
                    )
                raise
            result = self._result_mapping(payload)
            acknowledged = any(
                str(result.get(key) or "").strip()
                for key in ("heychat_ack_id", "msg_id", "msg_seq")
            )
            if not acknowledged:
                protocol = unquote(
                    str(result.get("heybox__protocol__execute__directly") or "")
                ).lower()
                if "web_auth" in protocol or "name_verify" in protocol:
                    message = (
                        "小黑盒要求安全认证或实名认证，请先在 App 中完成后再发送私信。"
                    )
                else:
                    message = "小黑盒私信响应中缺少消息 ID，无法确认私信已发送。"
                raise XhhError(message, retryable=False)
            self._last_direct_message_sent_at = time.time()
            return payload

    async def send_direct_message_chain(
        self,
        *,
        user_id: str,
        text: str,
        image_sources: Sequence[str],
        allowed_local_roots: Sequence[Path] = (),
        max_local_image_bytes: int = 20 * 1024 * 1024,
        cooldown_seconds: int = 5,
    ) -> list[dict[str, Any]]:
        self._ensure_direct_message_sending_allowed()
        prepared = await self.prepare_image_sources(
            image_sources,
            allowed_local_roots=allowed_local_roots,
            max_local_image_bytes=max_local_image_bytes,
        )
        deliveries: list[dict[str, Any]] = []
        parts = prepared or [""]
        for index, image_url in enumerate(parts):
            try:
                deliveries.append(
                    await self._send_direct_message_prepared(
                        user_id=user_id,
                        text=text if index == 0 else "",
                        image_url=image_url,
                        cooldown_seconds=cooldown_seconds,
                    )
                )
            except XhhError as exc:
                if deliveries:
                    raise XhhError(
                        "私信图片链只完成了部分发送，已停止重试以避免重复。",
                        retryable=False,
                        delivery_uncertain=True,
                        action_restricted=exc.action_restricted,
                    ) from exc
                raise
        return deliveries

    def _ensure_direct_message_sending_allowed(self) -> None:
        if not self._direct_message_action_restriction:
            return
        remaining = self._direct_message_action_restricted_until - time.time()
        if remaining <= 0:
            self._direct_message_action_restriction = ""
            self._direct_message_action_restricted_until = 0.0
            return
        raise XhhError(
            "小黑盒刚刚拒绝了私信发送；为避免连续触发限制，"
            f"将在约 {max(1, int(remaining))} 秒后允许再次尝试。",
            retryable=False,
            terminal=True,
            action_restricted=True,
            retry_after=remaining,
        )

    async def send_reply(
        self,
        *,
        text: str,
        link_id: int,
        reply_id: int,
        root_id: int,
        image_sources: Sequence[str] = (),
        allowed_local_roots: Sequence[Path] = (),
        max_local_image_bytes: int = 20 * 1024 * 1024,
    ) -> ReplyReceipt:
        prepared_images = await self.prepare_image_sources(
            image_sources,
            allowed_local_roots=allowed_local_roots,
            max_local_image_bytes=max_local_image_bytes,
        )
        data = {
            "is_cy": "",
            "link_id": str(link_id),
            "reply_id": str(reply_id),
            "root_id": str(root_id),
            "text": text,
        }
        if prepared_images:
            data["imgs"] = ";".join(prepared_images)
        response = await self._request_json(
            "POST",
            "/bbs/app/comment/create",
            data=data,
            use_reply_api=True,
            auth_required=True,
            allow_api_failure=True,
            write_request=True,
        )
        status = self._api_status(response.payload)
        message = str(response.payload.get("msg") or "").strip()
        if status in {"ok", "success"}:
            return ReplyReceipt(status=status, message=message)

        combined = f"{status} {message}".lower()
        if self._looks_like_auth_error(combined):
            raise XhhError(
                message or "小黑盒登录已失效。", auth_required=True, retryable=False
            )
        if any(
            word in combined
            for word in (
                "评论已被删除",
                "帖子已删除",
                "无法评论",
                "不存在",
                "not found",
            )
        ):
            raise XhhError(
                message or "目标评论无法回复。", terminal=True, retryable=False
            )
        if self._looks_like_rate_limit(combined):
            raise XhhError(
                message or "小黑盒请求过于频繁。", retryable=True, retry_after=60
            )
        if status == "failed":
            raise XhhError(
                message or "目标评论当前无法回复。", terminal=True, retryable=False
            )
        raise XhhError(
            message or f"小黑盒回帖失败：{status or 'unknown'}", retryable=True
        )

    async def _write_json(
        self,
        path: str,
        *,
        data: Mapping[str, str],
        params: Mapping[str, str] | None = None,
        use_reply_api: bool = False,
        direct_message_request: bool = False,
    ) -> dict[str, Any]:
        response = await self._request_json(
            "POST",
            path,
            params=params,
            data=data,
            use_reply_api=use_reply_api,
            auth_required=True,
            allow_api_failure=True,
            write_request=True,
            direct_message_request=direct_message_request,
        )
        status = self._api_status(response.payload)
        if status not in {"ok", "success"}:
            if status:
                self._raise_for_api_failure(response.payload)
            raise XhhError(
                str(response.payload.get("msg") or "小黑盒写入接口没有返回成功状态。"),
                retryable=False,
            )
        return response.payload

    async def validate_auth(self) -> bool:
        await self.fetch_mentions(offset=0, limit=1)
        return True

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        data: Mapping[str, str] | None = None,
        use_reply_api: bool = False,
        auth_required: bool,
        allow_api_failure: bool = False,
        write_request: bool = False,
        direct_message_request: bool = False,
        login_request: bool = False,
    ) -> _JsonResponse:
        await self.start()
        if auth_required and (self.auth is None or not self.auth.cookie):
            raise XhhError("尚未登录小黑盒。", auth_required=True, retryable=False)

        if login_request:
            query = {
                str(key): str(value)
                for key, value in dict(params or {}).items()
                if value is not None
            }
        elif direct_message_request:
            hkey, nonce, request_time = get_heybox_request_keys(path)
            query = {
                "os_type": "web",
                "app": "heybox",
                "client_type": "web",
                "version": self.version,
                "web_version": self.web_version,
                "x_client_type": "web",
                "x_app": "heybox_website",
                "x_os_type": "Windows",
                "device_info": "Chrome",
            }
            if self.device_id:
                query["device_id"] = self.device_id
            query.update(self._direct_message_api_params)
            query.update(dict(params or {}))
            query.update(
                {
                    "hkey": hkey,
                    "_time": str(request_time),
                    "nonce": nonce,
                }
            )
        else:
            hkey, nonce, request_time = get_request_keys(path)
            query = dict(params or {})
            query.update(
                {
                    "os_type": "web",
                    "app": "web",
                    "client_type": "web",
                    "version": self.version,
                    "web_version": self.web_version,
                    "x_client_type": "web",
                    "x_app": "heybox_website",
                    "x_os_type": "Windows",
                    "device_info": "Chrome",
                    "device_id": self.device_id,
                    "hkey": hkey,
                    "_time": str(request_time),
                    "nonce": nonce,
                    "_notip": "true",
                }
            )
        if auth_required and self.auth is not None and self.auth.heybox_id:
            query["heybox_id"] = self.auth.heybox_id

        if direct_message_request:
            headers = {
                "Accept": "application/json",
                "Accept-Language": "zh,zh-CN;q=0.9",
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                "Origin": "https://www.xiaoheihe.cn",
                "Referer": "https://www.xiaoheihe.cn/",
                "User-Agent": DIRECT_MESSAGE_USER_AGENT,
            }
            request_data: Mapping[str, str] | str | None = (
                urlencode(dict(data)) if data is not None else None
            )
        elif login_request:
            headers = {
                "Accept": "application/json",
                "Accept-Language": "zh,zh-CN;q=0.9",
                "Referer": "https://www.xiaoheihe.cn/",
                "User-Agent": DIRECT_MESSAGE_USER_AGENT,
            }
            request_data = data
        else:
            headers = {
                "Referer": "https://www.xiaoheihe.cn/",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
            }
            request_data = data
        if auth_required and self.auth is not None and self.auth.cookie:
            headers["Cookie"] = (
                self._direct_message_cookie_header()
                if direct_message_request
                else self.auth.cookie
            )
        if method.upper() == "POST" and not direct_message_request:
            headers["Origin"] = "https://www.xiaoheihe.cn"

        base_url = self.reply_base_url if use_reply_api else self.api_base_url
        url = f"{base_url}{path}"
        assert self._session is not None
        try:
            async with self._session.request(
                method, url, params=query, data=request_data, headers=headers
            ) as response:
                raw = await response.text(errors="replace")
                cookies = {
                    name: morsel.value for name, morsel in response.cookies.items()
                }
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise XhhError(
                        f"小黑盒返回了非 JSON 响应（HTTP {response.status}）：{raw[:200]}",
                        retryable=response.status == 429 or response.status >= 500,
                        auth_required=response.status in {401, 403},
                        delivery_uncertain=write_request and response.status >= 500,
                        retry_after=self._retry_after(
                            response.headers.get("Retry-After")
                        ),
                    ) from exc

                if not isinstance(payload, dict):
                    raise XhhError("小黑盒返回的 JSON 不是对象。", retryable=True)
                if response.status < 200 or response.status >= 300:
                    retry_after = self._retry_after(response.headers.get("Retry-After"))
                    raise XhhError(
                        f"小黑盒 HTTP {response.status}：{str(payload.get('msg') or raw)[:200]}",
                        retryable=response.status == 429 or response.status >= 500,
                        auth_required=response.status in {401, 403},
                        delivery_uncertain=write_request and response.status >= 500,
                        retry_after=retry_after,
                    )
                if not allow_api_failure:
                    self._raise_for_api_failure(payload)
                return _JsonResponse(payload=payload, cookies=cookies)
        except XhhError:
            raise
        except (
            asyncio.TimeoutError,
            aiohttp.ClientError,
            ProxyConnectionError,
            ProxyError,
            ProxyTimeoutError,
        ) as exc:
            raise XhhError(
                f"请求小黑盒失败：{self._safe_network_error(exc)}",
                retryable=True,
                delivery_uncertain=write_request,
            ) from exc

    @staticmethod
    def _parse_direct_message_api_params(value: str) -> dict[str, str]:
        text = str(value or "").strip()
        if not text:
            return {}
        try:
            parsed = urlparse(text)
            query_text = parsed.query if parsed.query else text.lstrip("?")
            pairs = parse_qsl(query_text, keep_blank_values=False)
        except ValueError:
            return {}
        return {
            key: item
            for key, item in pairs
            if key in DIRECT_MESSAGE_API_PARAM_KEYS and str(item).strip()
        }

    @staticmethod
    def _validate_proxy_url(proxy_url: str) -> str:
        value = str(proxy_url or "").strip()
        if not value:
            return ""
        try:
            parsed = urlparse(value)
            host = parsed.hostname
            port = parsed.port
        except (TypeError, ValueError):
            raise ValueError("家庭代理地址无效，请使用 socks5://主机:端口。") from None
        if (
            parsed.scheme.lower() != "socks5"
            or not host
            or port is None
            or not 1 <= port <= 65535
            or parsed.path not in {"", "/"}
            or parsed.params
            or parsed.query
            or parsed.fragment
            or any(character.isspace() for character in value)
        ):
            raise ValueError("家庭代理地址无效，请使用 socks5://主机:端口。")
        return value

    def _safe_network_error(self, exc: BaseException) -> str:
        message = str(exc) or exc.__class__.__name__
        if not self.proxy_url:
            return message

        message = message.replace(self.proxy_url, "[SOCKS5 家庭代理]")
        try:
            parsed = urlparse(self.proxy_url)
        except ValueError:
            parsed = None
        if parsed is not None:
            for secret in (parsed.username, parsed.password):
                if secret:
                    message = message.replace(unquote(secret), "[已隐藏]")
                    message = message.replace(secret, "[已隐藏]")
        return re.sub(
            r"(?i)(socks5://)[^/@\s]+@",
            r"\1[已隐藏]@",
            message,
        )

    def _raise_for_api_failure(self, payload: Mapping[str, Any]) -> None:
        status = self._api_status(payload)
        if not status or status in {"ok", "success"}:
            return
        message = str(payload.get("msg") or "").strip()
        combined = f"{status} {message}".lower()
        if self._looks_like_auth_error(combined):
            raise XhhError(
                message or "小黑盒登录已失效。", auth_required=True, retryable=False
            )
        if self._looks_like_direct_message_action_restriction(combined):
            raise XhhError(
                message or "小黑盒已禁止当前账号发送私信。",
                retryable=False,
                terminal=True,
                action_restricted=True,
            )
        if self._looks_like_rate_limit(combined):
            raise XhhError(
                message or "小黑盒请求过于频繁。", retryable=True, retry_after=60
            )
        terminal = status == "failed" and any(
            word in combined for word in ("删除", "不存在", "不可见", "not found")
        )
        raise XhhError(
            message or f"小黑盒接口返回 {status}。",
            retryable=not terminal,
            terminal=terminal,
        )

    def _all_session_cookies(self) -> dict[str, str]:
        if self._session is None:
            return {}
        try:
            return {morsel.key: morsel.value for morsel in self._session.cookie_jar}
        except (AttributeError, TypeError):
            return {}

    @staticmethod
    def _filter_login_cookies(cookies: Mapping[str, str]) -> dict[str, str]:
        return {
            str(name): str(value)
            for name, value in cookies.items()
            if str(name) in LOGIN_COOKIE_NAMES and str(value)
        }

    def _direct_message_cookie_header(self) -> str:
        if self.auth is None:
            return ""
        original = self.auth.cookie
        filtered = self._filter_login_cookies(self.parse_cookie_header(original))
        return self._format_cookie_header(filtered) or original

    def direct_message_diagnostics(self) -> dict[str, Any]:
        params = {
            "app": "heybox",
            "version": self.version,
            "web_version": self.web_version,
            "device_id": self.device_id,
        }
        params.update(self._direct_message_api_params)
        effective_cookie = self._direct_message_cookie_header()
        original_names = set(
            self.parse_cookie_header(self.auth.cookie if self.auth is not None else "")
        )
        effective_names = sorted(self.parse_cookie_header(effective_cookie))
        device_id = str(params.get("device_id") or "")
        return {
            "parameter_source": (
                "api_params_url" if self._direct_message_api_params else "defaults"
            ),
            "app": str(params.get("app") or ""),
            "version": str(params.get("version") or ""),
            "web_version": str(params.get("web_version") or ""),
            "device_id_sha256": (
                hashlib.sha256(device_id.encode("utf-8")).hexdigest()[:12]
                if device_id
                else ""
            ),
            "cookie_names": effective_names,
            "cookie_filtered": set(effective_names) != original_names,
            "proxy_enabled": bool(self.proxy_url),
            "user_agent": DIRECT_MESSAGE_USER_AGENT,
        }

    def _current_auth_user_id(self) -> str:
        user_id = str(
            self.auth.heybox_id if self.auth is not None else ""
        ).strip()
        if not user_id:
            raise XhhError(
                "当前登录凭据缺少 heybox_id，请重新扫码登录。",
                auth_required=True,
                retryable=False,
            )
        return user_id

    @staticmethod
    def parse_cookie_header(header: str) -> dict[str, str]:
        parsed = SimpleCookie()
        try:
            parsed.load(header)
        except Exception:
            return {}
        return {name: morsel.value for name, morsel in parsed.items()}

    @staticmethod
    def _format_cookie_header(cookies: Mapping[str, str]) -> str:
        return "; ".join(
            f"{name}={value}" for name, value in cookies.items() if name and value
        )

    @staticmethod
    def _result_mapping(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        result = payload.get("result")
        return result if isinstance(result, Mapping) else {}

    @staticmethod
    def _first_mapping_list(*values: Any) -> list[Mapping[str, Any]]:
        for value in values:
            if isinstance(value, list):
                return [item for item in value if isinstance(item, Mapping)]
        return []

    @staticmethod
    def _api_status(payload: Mapping[str, Any]) -> str:
        return str(payload.get("status") or payload.get("stat") or "").strip().lower()

    @staticmethod
    def _looks_like_auth_error(value: str) -> bool:
        return any(
            word in value
            for word in (
                "未登录",
                "登录失效",
                "请登录",
                "unauthorized",
                "forbidden",
                "cookie",
            )
        )

    @staticmethod
    def _looks_like_rate_limit(value: str) -> bool:
        return any(word in value for word in ("频繁", "稍后", "rate limit", "too many"))

    @staticmethod
    def _looks_like_direct_message_action_restriction(value: str) -> bool:
        return any(
            phrase in value
            for phrase in (
                "禁止发送消息",
                "被禁止发消息",
                "禁止私信",
                "私信功能已被限制",
            )
        )

    @staticmethod
    def _normalise_media_url(value: str) -> str:
        if value.startswith("//"):
            return "https:" + value
        return value

    @classmethod
    def _summarize_profile_link(cls, raw: Mapping[str, Any]) -> dict[str, Any]:
        link = raw.get("link")
        link = link if isinstance(link, Mapping) else raw
        raw_content = (
            link.get("text")
            or link.get("description")
            or link.get("desc")
            or ""
        )
        content_blocks = parse_inbound_content_blocks(raw_content)
        return {
            "link_id": str(
                link.get("linkid")
                or link.get("link_id")
                or link.get("id")
                or ""
            ).strip(),
            "title": str(link.get("title") or "").strip(),
            "description": (
                content_blocks_plain_text(content_blocks)
                or cls._plain_text(raw_content)
            )[:500],
            "created_at": cls._to_int(
                link.get("create_at")
                or link.get("create_time")
                or link.get("timestamp")
            ),
            "likes": cls._to_int(link.get("up") or link.get("like_num")),
            "comment_count": cls._to_int(
                link.get("comment_num") or link.get("comment_count")
            ),
            "topics": cls._extract_names(
                link.get("topics") or link.get("topic") or []
            ),
            "image_urls": extract_image_urls(
                [link.get("imgs"), link.get("images"), link.get("text")]
            ),
            "content_blocks": list(content_blocks),
        }

    @classmethod
    def _summarize_remote_draft(cls, raw: Mapping[str, Any]) -> dict[str, Any]:
        link = raw.get("link")
        link = link if isinstance(link, Mapping) else raw
        raw_text = link.get("text") or link.get("content") or ""
        content_blocks = parse_inbound_content_blocks(raw_text)
        topic = link.get("topic")
        topic = topic if isinstance(topic, Mapping) else {}
        return {
            "link_id": str(
                link.get("linkid")
                or link.get("link_id")
                or link.get("id")
                or ""
            ).strip(),
            "title": str(link.get("title") or "").strip(),
            "description": cls._plain_text(
                link.get("description") or link.get("desc")
            )[:500],
            "body_preview": (
                content_blocks_plain_text(content_blocks)
                or cls._plain_text(raw_text)
            )[:500],
            "created_at": cls._to_int(
                link.get("create_at")
                or link.get("create_time")
                or link.get("timestamp")
            ),
            "topic": str(topic.get("name") or link.get("topic_name") or "").strip(),
            "image_urls": extract_image_urls(
                [link.get("imgs"), link.get("images"), raw_text]
            ),
            "content_blocks": list(content_blocks),
        }

    @staticmethod
    def _plain_text(value: Any) -> str:
        text = html.unescape(str(value or ""))
        text = re.sub(r"(?i)<br\\s*/?>", "\n", text)
        text = re.sub(r"(?i)</p\\s*>", "\n", text)
        text = re.sub(r"<[^>]+>", "", text)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @classmethod
    def _extract_names(cls, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        names: list[str] = []
        for item in value:
            if isinstance(item, Mapping):
                name = next(
                    (
                        str(item.get(key) or "").strip()
                        for key in ("name", "title", "text", "topic_name", "hashtag")
                        if str(item.get(key) or "").strip()
                    ),
                    "",
                )
            else:
                name = str(item).strip()
            if name:
                names.append(name)
        return list(dict.fromkeys(names))

    @staticmethod
    def _retry_after(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            return None

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
