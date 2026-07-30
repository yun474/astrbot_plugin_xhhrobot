from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import (
    At,
    AtAll,
    File,
    Image,
    Json,
    Plain,
    Record,
    Reply,
    Video,
)
from astrbot.api.platform import (
    AstrBotMessage,
    Group,
    MessageMember,
    MessageType,
    PlatformMetadata,
)

from .media import is_http_url, unique_strings
from .xhh_client import XhhClient, XhhError

XHH_PLATFORM_ID = "xhhrobot"
XHH_PLATFORM_META = PlatformMetadata(
    name=XHH_PLATFORM_ID,
    description="小黑盒bot 标准事件桥",
    id=XHH_PLATFORM_ID,
    adapter_display_name="小黑盒bot",
    support_streaming_message=False,
    support_proactive_message=False,
)

_ASTRBOT_PLUGIN_ERROR_RE = re.compile(
    r":\([ \t]*(?:\r?\n[ \t]*){1,3}"
    r"在调用插件[^\r\n]+?的处理函数[^\r\n]+?时出现异常[：:]"
    r"[\s\S]*\Z"
)
_INTERNAL_XHH_IDENTIFIER_RE = re.compile(r"(?<![A-Za-z0-9_:-])@?xhh:[A-Za-z0-9_-]+")


@dataclass(frozen=True, slots=True)
class EventTarget:
    kind: str
    source: str
    event_key: str
    raw_user_id: str
    link_id: int = 0
    comment_id: int = 0
    root_comment_id: int = 0


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    status: str
    text: str = ""
    image_sources: tuple[str, ...] = ()
    error: BaseException | None = None


class DeliveryPreparationError(RuntimeError):
    """The generated reply could not be claimed or persisted before sending."""


DeliveryStartCallback = Callable[
    [str, list[str]],
    Awaitable[bool | str | None],
]
DeliveryCallback = Callable[[str, list[str]], Awaitable[None]]
ErrorCallback = Callable[[BaseException, str, list[str]], Awaitable[None]]


class XhhMessageEvent(AstrMessageEvent):
    def __init__(
        self,
        *,
        message_obj: AstrBotMessage,
        target: EventTarget,
        client: XhhClient,
        max_reply_chars: int,
        max_outgoing_images: int,
        max_local_image_bytes: int,
        allowed_local_roots: Sequence[Path],
        direct_message_cooldown_seconds: int,
        clean_text: Callable[[str], str],
        on_send_start: DeliveryStartCallback,
        on_sent: DeliveryCallback,
        on_send_error: ErrorCallback,
        on_empty: Callable[[], Awaitable[None]],
    ) -> None:
        super().__init__(
            message_str=message_obj.message_str,
            message_obj=message_obj,
            platform_meta=XHH_PLATFORM_META,
            session_id=message_obj.session_id,
        )
        self.target = target
        self.client = client
        self.max_reply_chars = max(1, int(max_reply_chars))
        self.max_outgoing_images = max(0, int(max_outgoing_images))
        self.max_local_image_bytes = max(1, int(max_local_image_bytes))
        self.allowed_local_roots = tuple(Path(path) for path in allowed_local_roots)
        self.direct_message_cooldown_seconds = max(
            0, int(direct_message_cooldown_seconds)
        )
        self._clean_text = clean_text
        self._on_send_start = on_send_start
        self._on_sent = on_sent
        self._on_send_error = on_send_error
        self._on_empty = on_empty
        self._send_lock = asyncio.Lock()
        self._delivery_started = False
        self._outbound_started = False
        self._delivery_expired = False
        self.delivery_future: asyncio.Future[DeliveryResult] = (
            asyncio.get_running_loop().create_future()
        )

    @property
    def outbound_started(self) -> bool:
        """Whether the reply crossed into the platform delivery phase."""

        return self._outbound_started

    def expire_if_not_started(self) -> bool:
        """Stop a late event before it can create a duplicate platform reply.

        AstrBot owns the task that consumes its event queue, so the plugin cannot
        cancel that task directly. Once this event is expired, a later `send()`
        becomes a no-op. If platform delivery has already started, the result is
        intentionally left uncertain instead of allowing an automatic retry.
        """

        if self._outbound_started or self.delivery_future.done():
            return False
        self._delivery_expired = True
        self.stop_event()
        self._finish_delivery(DeliveryResult(status="expired"))
        return True

    async def send(self, message: MessageChain) -> None:
        async with self._send_lock:
            if self._delivery_started:
                logger.debug(
                    "xhhrobot ignored duplicate send call: event_key=%s target=%s",
                    self.target.event_key,
                    self.target.kind,
                )
                return
            if self._delivery_expired:
                return
            self._delivery_started = True

            raw_text = strip_internal_xhh_identifiers(
                self._message_chain_to_text(message)
            )
            raw_text, suppressed_internal_error = _strip_astrbot_plugin_error(raw_text)
            if suppressed_internal_error:
                logger.warning(
                    "xhhrobot suppressed AstrBot plugin error result: "
                    "event_key=%s target=%s",
                    self.target.event_key,
                    self.target.kind,
                )
            text = self._clean_text(raw_text).strip()
            if len(text) > self.max_reply_chars:
                text = text[: self.max_reply_chars].rstrip()
            image_sources = self._message_chain_to_image_sources(message)
            if self.max_outgoing_images >= 0:
                image_sources = image_sources[: self.max_outgoing_images]

            if not text and not image_sources:
                await self._on_empty()
                self._finish_delivery(DeliveryResult(status="empty"))
                await super().send(message)
                return

            try:
                delivery_claimed = await self._on_send_start(text, image_sources)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = DeliveryPreparationError(
                    f"发送前状态准备失败：{exc}"
                )
                self._finish_delivery(
                    DeliveryResult(
                        status="error",
                        text=text,
                        image_sources=tuple(image_sources),
                        error=error,
                    )
                )
                await self._on_send_error(error, text, image_sources)
                raise error from exc
            if delivery_claimed == "review":
                logger.info(
                    "xhhrobot held generated reply for human review: "
                    "event_key=%s target=%s",
                    self.target.event_key,
                    self.target.kind,
                )
                self._finish_delivery(
                    DeliveryResult(
                        status="pending_review",
                        text=text,
                        image_sources=tuple(image_sources),
                    )
                )
                return
            if delivery_claimed is False:
                logger.info(
                    "xhhrobot suppressed duplicate platform delivery: "
                    "event_key=%s target=%s",
                    self.target.event_key,
                    self.target.kind,
                )
                self._finish_delivery(
                    DeliveryResult(
                        status="suppressed",
                        text=text,
                        image_sources=tuple(image_sources),
                    )
                )
                return

            self._outbound_started = True
            try:
                if self.target.kind == "direct_message":
                    await self.client.send_direct_message_chain(
                        user_id=self.target.raw_user_id,
                        text=text,
                        image_sources=image_sources,
                        allowed_local_roots=self.allowed_local_roots,
                        max_local_image_bytes=self.max_local_image_bytes,
                        cooldown_seconds=self.direct_message_cooldown_seconds,
                    )
                else:
                    await self.client.send_reply(
                        text=text,
                        link_id=self.target.link_id,
                        reply_id=self.target.comment_id,
                        root_id=self.target.root_comment_id,
                        image_sources=image_sources,
                        allowed_local_roots=self.allowed_local_roots,
                        max_local_image_bytes=self.max_local_image_bytes,
                    )
            except asyncio.CancelledError:
                error = RuntimeError(
                    "AstrBot 在小黑盒发送过程中停止，无法确认消息是否送达。"
                )
                await asyncio.shield(self._on_send_error(error, text, image_sources))
                self._finish_delivery(
                    DeliveryResult(
                        status="error",
                        text=text,
                        image_sources=tuple(image_sources),
                        error=error,
                    )
                )
                raise
            except Exception as exc:
                await self._on_send_error(exc, text, image_sources)
                self._finish_delivery(
                    DeliveryResult(
                        status="error",
                        text=text,
                        image_sources=tuple(image_sources),
                        error=exc,
                    )
                )
                if isinstance(exc, XhhError) and exc.action_restricted:
                    return
                raise

            await self._on_sent(text, image_sources)
            self._finish_delivery(
                DeliveryResult(
                    status="sent",
                    text=text,
                    image_sources=tuple(image_sources),
                )
            )
            await super().send(message)

    def _finish_delivery(self, result: DeliveryResult) -> None:
        if not self.delivery_future.done():
            self.delivery_future.set_result(result)

    @staticmethod
    def _message_chain_to_text(message: MessageChain) -> str:
        parts: list[str] = []
        for component in message.chain:
            if isinstance(component, Plain):
                parts.append(str(component.text or ""))
            elif isinstance(component, AtAll):
                parts.append("@全体成员")
            elif isinstance(component, At):
                target_id = str(getattr(component, "qq", "") or "").strip()
                # The reply API already targets the source comment. AstrBot's
                # namespaced XHH At values cannot be rendered by Xiaoheihe.
                if target_id.startswith("xhh:"):
                    continue
                label = str(getattr(component, "name", "") or target_id).strip()
                if label:
                    parts.append(f"@{label}")
            elif isinstance(component, (Image, Reply)):
                continue
            elif isinstance(component, Record):
                parts.append("[语音]")
            elif isinstance(component, Video):
                parts.append("[视频]")
            elif isinstance(component, File):
                name = str(
                    getattr(component, "name", "")
                    or getattr(component, "url", "")
                    or ""
                ).strip()
                parts.append(f"[文件:{name}]" if name else "[文件]")
            elif isinstance(component, Json):
                parts.append("[结构化消息]")
            else:
                parts.append(f"[{component.__class__.__name__}]")
        return "".join(parts)

    @staticmethod
    def _message_chain_to_image_sources(message: MessageChain) -> list[str]:
        sources: list[str] = []
        for component in message.chain:
            if not isinstance(component, Image):
                continue
            values = (
                getattr(component, "url", ""),
                getattr(component, "file", ""),
                getattr(component, "path", ""),
            )
            for value in values:
                source = str(value or "").strip()
                if not source:
                    continue
                if is_http_url(source) or source.startswith(
                    ("file://", "base64://", "data:image/")
                ):
                    sources.append(source)
                    break
                try:
                    if Path(source).expanduser().is_file():
                        sources.append(source)
                        break
                except OSError:
                    continue
        return unique_strings(sources)


def build_comment_message(
    *,
    self_user_id: str,
    session_id: str,
    message_id: str,
    sender_id: str,
    sender_name: str,
    message_text: str,
    image_urls: Sequence[str],
    link_id: int,
    link_title: str,
    timestamp: int,
    raw_message: Any,
    image_groups: Sequence[tuple[str, Sequence[str]]] | None = None,
) -> AstrBotMessage:
    sender = MessageMember(user_id=_namespaced_user(sender_id), nickname=sender_name)
    self_id = _namespaced_user(self_user_id or XHH_PLATFORM_ID)
    chain: list[Any] = [At(qq=self_id, name="小黑盒bot"), Plain(message_text)]
    message_text_parts = [message_text]
    if image_groups:
        seen_urls: set[str] = set()
        for label, sources in image_groups:
            urls: list[str] = []
            for url in unique_strings(sources):
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                urls.append(url)
            if not urls:
                continue
            normalized_label = str(label or "评论图片").strip() or "评论图片"
            label_text = f"\n[{normalized_label}：{len(urls)} 张]"
            chain.append(Plain(label_text))
            message_text_parts.append(label_text)
            chain.extend(Image.fromURL(url) for url in urls)
    else:
        chain.extend(Image.fromURL(url) for url in unique_strings(image_urls))
    message = AstrBotMessage()
    message.type = MessageType.GROUP_MESSAGE
    message.self_id = self_id
    message.session_id = session_id
    message.message_id = message_id
    message.group = Group(
        group_id=str(link_id),
        group_name=link_title or f"小黑盒帖子 {link_id}",
    )
    message.sender = sender
    message.message = chain
    message.message_str = "".join(message_text_parts)
    message.raw_message = raw_message
    message.timestamp = int(timestamp or 0)
    return message


def build_direct_message(
    *,
    self_user_id: str,
    session_id: str,
    message_id: str,
    sender_id: str,
    sender_name: str,
    message_text: str,
    image_urls: Sequence[str],
    timestamp: int,
    raw_message: Any,
) -> AstrBotMessage:
    self_id = _namespaced_user(self_user_id or XHH_PLATFORM_ID)
    chain: list[Any] = [At(qq=self_id, name="小黑盒bot"), Plain(message_text)]
    chain.extend(Image.fromURL(url) for url in unique_strings(image_urls))
    message = AstrBotMessage()
    message.type = MessageType.FRIEND_MESSAGE
    message.self_id = self_id
    message.session_id = session_id
    message.message_id = message_id
    message.group = None
    message.sender = MessageMember(
        user_id=_namespaced_user(sender_id),
        nickname=sender_name,
    )
    message.message = chain
    message.message_str = message_text
    message.raw_message = raw_message
    message.timestamp = int(timestamp or 0)
    return message


def _namespaced_user(user_id: str) -> str:
    value = str(user_id or "unknown").strip()
    return value if value.startswith("xhh:") else f"xhh:{value}"


def strip_internal_xhh_identifiers(value: str) -> str:
    """Prevent AstrBot's internal XHH IDs from being posted as reply text."""

    return _INTERNAL_XHH_IDENTIFIER_RE.sub("", str(value or ""))


def _strip_astrbot_plugin_error(text: str) -> tuple[str, bool]:
    match = _ASTRBOT_PLUGIN_ERROR_RE.search(text)
    if match is None:
        return text, False
    return text[: match.start()].rstrip(), True
