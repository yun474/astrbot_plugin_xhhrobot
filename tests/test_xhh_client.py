from __future__ import annotations

import json
import unittest
from collections import deque
from http.cookies import SimpleCookie
from typing import Any
from unittest.mock import patch
from urllib.parse import parse_qs

import aiohttp
from aiohttp_socks import ProxyConnectionError

from astrbot_plugin_xhhrobot.models import AuthInfo, QrChallenge
from astrbot_plugin_xhhrobot.xhh_client import XhhClient, XhhError


class FakeResponse:
    def __init__(self, payload: dict[str, Any], status: int = 200) -> None:
        self._raw = json.dumps(payload, ensure_ascii=False)
        self.status = status
        self.cookies: dict[str, Any] = {}
        self.headers: dict[str, str] = {}

    async def text(self, errors: str = "replace") -> str:
        return self._raw


class FakeRequestContext:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response

    async def __aenter__(self) -> FakeResponse:
        return self.response

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        return None


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = deque(responses)
        self.requests: list[tuple[str, str, dict[str, Any]]] = []
        self.closed = False
        self.cookie_jar: list[Any] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeRequestContext:
        self.requests.append((method, url, kwargs))
        return FakeRequestContext(self.responses.popleft())

    async def close(self) -> None:
        self.closed = True


class FailingSession(FakeSession):
    def __init__(self, message: str) -> None:
        super().__init__([])
        self.message = message

    def request(self, method: str, url: str, **kwargs: Any) -> FakeRequestContext:
        raise ProxyConnectionError(self.message)


class XhhClientTests(unittest.IsolatedAsyncioTestCase):
    def make_client(
        self, responses: list[FakeResponse]
    ) -> tuple[XhhClient, FakeSession]:
        session = FakeSession(responses)
        client = XhhClient(
            api_base_url="https://api.xiaoheihe.cn",
            reply_base_url="https://workshopapi.xiaoheihe.cn",
            version="999.0.4",
            web_version="2.5",
            device_id="device",
            auth=AuthInfo(cookie="user_heybox_id=42; token=value", heybox_id="42"),
            session=session,  # type: ignore[arg-type]
        )
        return client, session

    async def test_start_without_proxy_uses_plain_aiohttp_session(self) -> None:
        created_session = FakeSession([])
        client = XhhClient(
            api_base_url="https://api.xiaoheihe.cn",
            reply_base_url="https://workshopapi.xiaoheihe.cn",
            version="999.0.4",
            web_version="2.5",
            device_id="device",
        )

        with (
            patch(
                "astrbot_plugin_xhhrobot.xhh_client.aiohttp.ClientSession",
                return_value=created_session,
            ) as session_factory,
            patch(
                "astrbot_plugin_xhhrobot.xhh_client.ProxyConnector.from_url"
            ) as connector_factory,
        ):
            await client.start()

        connector_factory.assert_not_called()
        self.assertNotIn("connector", session_factory.call_args.kwargs)
        self.assertIsInstance(
            session_factory.call_args.kwargs["cookie_jar"], aiohttp.DummyCookieJar
        )

    async def test_start_with_proxy_uses_remote_dns_connector(self) -> None:
        proxy_url = "socks5://xhhbot:secret@100.64.0.10:1080"
        connector = object()
        created_session = FakeSession([])
        client = XhhClient(
            api_base_url="https://api.xiaoheihe.cn",
            reply_base_url="https://workshopapi.xiaoheihe.cn",
            version="999.0.4",
            web_version="2.5",
            device_id="device",
            proxy_url=proxy_url,
        )

        with (
            patch(
                "astrbot_plugin_xhhrobot.xhh_client.aiohttp.ClientSession",
                return_value=created_session,
            ) as session_factory,
            patch(
                "astrbot_plugin_xhhrobot.xhh_client.ProxyConnector.from_url",
                return_value=connector,
            ) as connector_factory,
        ):
            await client.start()

        connector_factory.assert_called_once_with(proxy_url, rdns=True)
        self.assertIs(session_factory.call_args.kwargs["connector"], connector)

    def test_proxy_url_rejects_unsupported_or_incomplete_addresses(self) -> None:
        invalid_urls = (
            "http://127.0.0.1:1080",
            "socks5h://127.0.0.1:1080",
            "socks5://",
            "socks5://127.0.0.1",
            "socks5://127.0.0.1:0",
            "socks5://127.0.0.1:1080/path",
        )
        for proxy_url in invalid_urls:
            with (
                self.subTest(proxy_url=proxy_url),
                self.assertRaisesRegex(ValueError, "socks5://"),
            ):
                XhhClient(
                    api_base_url="https://api.xiaoheihe.cn",
                    reply_base_url="https://workshopapi.xiaoheihe.cn",
                    version="999.0.4",
                    web_version="2.5",
                    device_id="device",
                    proxy_url=proxy_url,
                )

    async def test_proxy_credentials_are_removed_from_network_errors(self) -> None:
        proxy_url = "socks5://xhhbot:secret-value@100.64.0.10:1080"
        session = FailingSession(
            f"authentication failed for secret-value via {proxy_url}"
        )
        client = XhhClient(
            api_base_url="https://api.xiaoheihe.cn",
            reply_base_url="https://workshopapi.xiaoheihe.cn",
            version="999.0.4",
            web_version="2.5",
            device_id="device",
            proxy_url=proxy_url,
            auth=AuthInfo(cookie="user_heybox_id=42; token=value", heybox_id="42"),
            session=session,  # type: ignore[arg-type]
        )

        with self.assertRaises(XhhError) as raised:
            await client.fetch_feed()

        error = str(raised.exception)
        self.assertNotIn(proxy_url, error)
        self.assertNotIn("secret-value", error)
        self.assertIn("[已隐藏]", error)

    async def test_fetch_mentions_parses_upstream_fields(self) -> None:
        client, session = self.make_client(
            [
                FakeResponse(
                    {
                        "stat": "ok",
                        "result": {
                            "messages": [
                                {
                                    "message_id": 11,
                                    "comment_a_id": 12,
                                    "root_comment_id": 10,
                                    "linkid": 13,
                                    "userid_a": 14,
                                    "comment_a_text": "@bot hello",
                                }
                            ]
                        },
                    }
                )
            ]
        )
        mentions = await client.fetch_mentions(offset=0, limit=20)
        self.assertEqual(mentions[0].message_id, 11)
        self.assertEqual(mentions[0].comment_text, "@bot hello")
        params = session.requests[0][2]["params"]
        self.assertEqual(params["message_type"], "16")
        self.assertIn("hkey", params)
        self.assertEqual(
            session.requests[0][2]["headers"]["Cookie"],
            "user_heybox_id=42; token=value",
        )

    async def test_fetch_comment_messages_filters_mixed_page_but_keeps_raw_count(
        self,
    ) -> None:
        client, session = self.make_client(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {
                            "messages": [
                                {
                                    "message_id": 31,
                                    "message_type": 1,
                                    "comment_a_id": 41,
                                    "comment_a_text": "普通评论",
                                    "link": {"linkid": 51},
                                    "user_a": {"userid": 61},
                                },
                                {"message_id": 30, "message_type": 4},
                                {
                                    "message_id": 29,
                                    "message_type": "2",
                                    "comment_id": 39,
                                    "content": "回复评论",
                                    "link": {"link_id": 49},
                                    "user_a": {"heybox_id": 59},
                                },
                            ]
                        },
                    }
                )
            ]
        )

        page = await client.fetch_comment_messages_page(offset=0, limit=20)

        self.assertEqual(page.raw_count, 3)
        self.assertEqual(page.message_ids, (31, 30, 29))
        self.assertEqual([item.message_id for item in page.items], [31, 29])
        self.assertEqual(page.items[0].source, "own_post_comment")
        self.assertEqual(page.items[1].source, "comment_reply")
        self.assertEqual(page.items[0].link_id, 51)
        self.assertEqual(page.items[0].user_id, 61)
        self.assertEqual(page.items[0].root_comment_id, 41)
        params = session.requests[0][2]["params"]
        self.assertNotIn("message_type", params)
        self.assertEqual(params["no_more"], "false")

    def test_feed_parser_handles_nested_links_and_deduplicates(self) -> None:
        payload = {
            "result": {
                "feeds": [
                    {
                        "link": {
                            "linkid": "701",
                            "title": "第一帖",
                            "description": "摘要",
                            "user": {"userid": "81", "username": "甲"},
                            "topics": [{"name": "硬件"}],
                            "hashtags": [{"name": "测试"}],
                            "up": 9,
                            "comment_num": 4,
                        }
                    },
                    {"link": {"linkid": 701, "title": "重复项"}},
                    {"link_id": 702, "title": "第二帖"},
                ]
            }
        }

        posts = XhhClient.parse_feed_posts(payload, limit=20)

        self.assertEqual([post.link_id for post in posts], [701, 702])
        self.assertEqual(posts[0].author_id, "81")
        self.assertEqual(posts[0].author_name, "甲")
        self.assertEqual(posts[0].topics, ("硬件",))
        self.assertEqual(posts[0].tags, ("测试",))
        self.assertEqual(posts[0].likes, 9)
        self.assertEqual(posts[0].comments, 4)

    async def test_fetch_post_extracts_text_images_topics_and_tags(self) -> None:
        content = json.dumps(
            [
                {"type": "text", "text": "正文"},
                {"type": "image", "url": "//cdn.example/image.jpg"},
            ],
            ensure_ascii=False,
        )
        client, _ = self.make_client(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {
                            "link": {
                                "title": "标题",
                                "user": {"userid": "42", "username": "机器人"},
                                "text": content,
                                "topics": [{"name": "游戏"}],
                                "hashtags": [{"name": "测试"}],
                            }
                        },
                    }
                )
            ]
        )
        post = await client.fetch_post_context(99)
        self.assertEqual(post.title, "标题")
        self.assertEqual(post.author_id, "42")
        self.assertEqual(post.author_name, "机器人")
        self.assertEqual(post.body_text, "正文")
        self.assertEqual(post.image_urls, ("https://cdn.example/image.jpg",))
        self.assertEqual(
            post.content_blocks,
            (
                {"type": "text", "text": "正文"},
                {"type": "image", "url": "https://cdn.example/image.jpg"},
            ),
        )
        self.assertEqual(post.topics, ("游戏",))
        self.assertEqual(post.tags, ("测试",))

    async def test_fetch_notifications_merges_and_deduplicates_sources(self) -> None:
        client, session = self.make_client(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {
                            "messages": [
                                {
                                    "message_id": 101,
                                    "comment_a_id": 51,
                                    "linkid": 10,
                                    "userid_a": 1,
                                    "timestamp": 100,
                                    "comment_a_text": "@ 我",
                                }
                            ]
                        },
                    }
                ),
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {
                            "messages": [
                                {
                                    "message_id": 102,
                                    "message_type": "1",
                                    "comment_a_id": 51,
                                    "linkid": 10,
                                    "userid_a": 1,
                                    "timestamp": 120,
                                    "comment_a_text": "回复我",
                                },
                                {
                                    "message_id": 103,
                                    "message_type": "2",
                                    "comment_a_id": 52,
                                    "linkid": 10,
                                    "userid_a": 2,
                                    "timestamp": 200,
                                    "comment_a_text": "另一条回复",
                                },
                            ]
                        },
                    }
                ),
            ]
        )

        data = await client.fetch_notifications(kind="all", limit=3)

        self.assertEqual([item["message_id"] for item in data["items"]], [103, 102])
        self.assertEqual(data["items"][0]["source"], "comment_reply")
        self.assertEqual(data["items"][1]["source"], "own_post_comment")
        self.assertEqual(data["fetched_source_counts"]["mention"]["items"], 1)
        self.assertEqual(
            session.requests[0][2]["params"]["message_type"], "16"
        )
        self.assertNotIn("message_type", session.requests[1][2]["params"])

    async def test_fetch_current_account_favorites_and_remote_drafts(self) -> None:
        client, session = self.make_client(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "total_page": 2,
                        "result": [
                            {
                                "link": {
                                    "linkid": 88,
                                    "title": "收藏标题",
                                    "description": "<p>收藏正文</p>",
                                    "imgs": ["https://cdn.example/favorite.png"],
                                }
                            }
                        ],
                    }
                ),
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {
                            "links": [
                                {
                                    "linkid": 99,
                                    "title": "草稿标题",
                                    "text": '[{"type":"text","text":"草稿正文"}]',
                                }
                            ]
                        },
                    }
                ),
            ]
        )

        favorites = await client.fetch_my_favorites(offset=3, limit=4)
        drafts = await client.fetch_remote_drafts()

        self.assertEqual(favorites["account_id"], "42")
        self.assertEqual(favorites["total_page"], 2)
        self.assertEqual(favorites["items"][0]["link_id"], "88")
        self.assertEqual(favorites["items"][0]["description"], "收藏正文")
        self.assertEqual(drafts["drafts"][0]["body_preview"], "草稿正文")
        self.assertEqual(drafts["drafts"][0]["content_blocks"][0]["type"], "text")
        self.assertEqual(
            session.requests[0][1],
            "https://api.xiaoheihe.cn/bbs/web/profile/favours",
        )
        self.assertEqual(session.requests[0][2]["params"]["userid"], "42")
        self.assertEqual(session.requests[0][2]["params"]["offset"], "3")
        self.assertEqual(
            session.requests[1][1],
            "https://api.xiaoheihe.cn/bbs/app/link/drafts",
        )

    async def test_send_reply_uses_workshop_api_and_form_fields(self) -> None:
        client, session = self.make_client(
            [FakeResponse({"status": "ok", "msg": "done"})]
        )
        receipt = await client.send_reply(text="回复", link_id=1, reply_id=2, root_id=3)
        self.assertEqual(receipt.status, "ok")
        method, url, kwargs = session.requests[0]
        self.assertEqual(method, "POST")
        self.assertEqual(url, "https://workshopapi.xiaoheihe.cn/bbs/app/comment/create")
        self.assertEqual(kwargs["data"]["text"], "回复")
        self.assertEqual(kwargs["data"]["reply_id"], "2")

    async def test_qr_login_does_not_send_old_auth_and_builds_new_auth(self) -> None:
        client, session = self.make_client(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {
                            "qr_url": "https://api.xiaoheihe.cn/account/qr_login/?app=xhh&qr=state",
                            "expire": 120,
                        },
                    }
                ),
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {"error": "ok", "nickname": "tester"},
                    }
                ),
            ]
        )
        challenge = await client.begin_qr_login()
        first_request = session.requests[0][2]
        self.assertNotIn("Cookie", first_request["headers"])
        self.assertNotIn("heybox_id", first_request["params"])
        self.assertEqual(challenge.state_params, {"qr": "state"})
        self.assertEqual(
            first_request["params"], {"app": "web", "_notip": "true"}
        )
        self.assertIn("Chrome/125.0.0.0", first_request["headers"]["User-Agent"])

        cookies = SimpleCookie()
        cookies.load("user_heybox_id=88; session=abc")
        session.cookie_jar = list(cookies.values())
        result = await client.poll_qr_login(
            QrChallenge(challenge.qr_url, challenge.state_params, 120)
        )
        self.assertEqual(result.state, "success")
        self.assertIsNotNone(result.auth)
        assert result.auth is not None
        self.assertEqual(result.auth.heybox_id, "88")
        self.assertEqual(result.auth.nickname, "tester")
        self.assertEqual(result.auth.cookie, "user_heybox_id=88")
        self.assertNotIn("session=", result.auth.cookie)
        self.assertNotIn("x_xhh_tokenid=", result.auth.cookie)
        poll_request = session.requests[1][2]
        self.assertEqual(poll_request["params"], {"qr": "state", "app": "web"})
        self.assertNotIn("hkey", poll_request["params"])

    async def test_qr_login_uses_result_heyboxid_when_cookie_has_no_account_id(
        self,
    ) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {"error": "ok", "heyboxid": "88"},
                    }
                )
            ]
        )
        cookies = SimpleCookie()
        cookies.load("user_pkey=secret; tracking=value")
        session.cookie_jar = list(cookies.values())
        client = XhhClient(
            api_base_url="https://api.xiaoheihe.cn",
            reply_base_url="https://workshopapi.xiaoheihe.cn",
            version="999.0.4",
            web_version="2.5",
            device_id="device",
            session=session,  # type: ignore[arg-type]
        )

        result = await client.poll_qr_login(
            QrChallenge("https://example.invalid", {"qr": "state"}, 120)
        )

        self.assertEqual(result.state, "success")
        assert result.auth is not None
        self.assertEqual(result.auth.heybox_id, "88")
        self.assertEqual(result.auth.cookie, "user_pkey=secret")

    async def test_qr_login_rebuilds_owned_session_after_success(self) -> None:
        login_session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {
                            "qr_url": "https://api.xiaoheihe.cn/login?qr=state",
                            "expire": 120,
                        },
                    }
                ),
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {"error": "ok", "heyboxid": "88"},
                    }
                ),
            ]
        )
        cookies = SimpleCookie()
        cookies.load("user_pkey=secret; tracking=value")
        login_session.cookie_jar = list(cookies.values())
        authenticated_session = FakeSession([])
        client = XhhClient(
            api_base_url="https://api.xiaoheihe.cn",
            reply_base_url="https://workshopapi.xiaoheihe.cn",
            version="999.0.4",
            web_version="2.5",
            device_id="device",
        )

        with patch(
            "astrbot_plugin_xhhrobot.xhh_client.aiohttp.ClientSession",
            side_effect=[login_session, authenticated_session],
        ) as session_factory:
            challenge = await client.begin_qr_login()
            result = await client.poll_qr_login(challenge)

        self.assertEqual(result.state, "success")
        self.assertTrue(login_session.closed)
        self.assertIs(client._session, authenticated_session)
        self.assertEqual(session_factory.call_count, 2)
        self.assertIsInstance(
            session_factory.call_args_list[0].kwargs["cookie_jar"], aiohttp.CookieJar
        )
        self.assertIsInstance(
            session_factory.call_args_list[1].kwargs["cookie_jar"],
            aiohttp.DummyCookieJar,
        )

    async def test_end_qr_login_rebuilds_session_after_pending_state(self) -> None:
        login_session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {
                            "qr_url": "https://api.xiaoheihe.cn/login?qr=state",
                            "expire": 120,
                        },
                    }
                ),
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {"error": "wait"},
                    }
                ),
            ]
        )
        daily_session = FakeSession([])
        client = XhhClient(
            api_base_url="https://api.xiaoheihe.cn",
            reply_base_url="https://workshopapi.xiaoheihe.cn",
            version="999.0.4",
            web_version="2.5",
            device_id="device",
        )

        with patch(
            "astrbot_plugin_xhhrobot.xhh_client.aiohttp.ClientSession",
            side_effect=[login_session, daily_session],
        ) as session_factory:
            challenge = await client.begin_qr_login()
            result = await client.poll_qr_login(challenge)
            self.assertEqual(result.state, "pending")
            await client.end_qr_login()
            await client.end_qr_login()

        self.assertTrue(login_session.closed)
        self.assertIs(client._session, daily_session)
        self.assertEqual(session_factory.call_count, 2)
        self.assertIsInstance(
            session_factory.call_args_list[1].kwargs["cookie_jar"],
            aiohttp.DummyCookieJar,
        )

    async def test_publish_post_copies_images_and_uses_verified_form(self) -> None:
        client, session = self.make_client(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {"url": "https://cdn.xiaoheihe.cn/copied.jpg"},
                    }
                ),
                FakeResponse({"status": "ok", "result": {"link_id": 321}}),
            ]
        )

        payload = await client.publish_post(
            title="测试标题",
            body="第一行 <tag>\n第二行",
            description="测试摘要",
            topic_ids=["7214", "18745"],
            hashtags=["AstrBot", "测试"],
            image_urls=["https://images.example/source.jpg"],
        )

        self.assertEqual(payload["result"]["link_id"], 321)
        copy_method, copy_url, copy_kwargs = session.requests[0]
        self.assertEqual(copy_method, "GET")
        self.assertEqual(
            copy_url,
            "https://api.xiaoheihe.cn/bbs/app/api/qcloud/cos/copy/image/by/url",
        )
        self.assertEqual(
            copy_kwargs["params"]["target_url"], "https://images.example/source.jpg"
        )

        method, url, kwargs = session.requests[1]
        self.assertEqual(session.requests[0][2]["params"]["app"], "web")
        self.assertEqual(session.requests[0][2]["params"]["_notip"], "true")
        self.assertEqual(method, "POST")
        self.assertEqual(url, "https://api.xiaoheihe.cn/bbs/app/api/link/post")
        self.assertEqual(kwargs["data"]["post_type"], "1")
        self.assertEqual(kwargs["data"]["topic_ids"], "7214,18745")
        self.assertEqual(json.loads(kwargs["data"]["hashtags"]), ["AstrBot", "测试"])
        content = json.loads(kwargs["data"]["text"])
        self.assertEqual(content[0]["type"], "html")
        self.assertEqual(content[0]["text"], "第一行 &lt;tag&gt;<br>第二行")
        self.assertEqual(
            content[1],
            {"type": "img", "url": "https://cdn.xiaoheihe.cn/copied.jpg"},
        )

    async def test_publish_post_preserves_rich_block_order(self) -> None:
        client, session = self.make_client(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {"url": "https://cdn.xiaoheihe.cn/rich.jpg"},
                    }
                ),
                FakeResponse({"status": "ok", "result": {"link_id": 322}}),
            ]
        )

        await client.publish_post(
            title="富文本标题",
            body="",
            content_blocks=[
                {"type": "text", "text": "第一段"},
                {"type": "html", "text": "<p><strong>重点</strong></p>"},
                {"type": "image", "url": "https://images.example/rich.jpg"},
            ],
        )

        content = json.loads(session.requests[1][2]["data"]["text"])
        self.assertEqual(
            content,
            [
                {"type": "html", "text": "第一段"},
                {"type": "html", "text": "<p><strong>重点</strong></p>"},
                {"type": "img", "url": "https://cdn.xiaoheihe.cn/rich.jpg"},
            ],
        )
        self.assertEqual(session.requests[1][2]["data"]["words_count"], "6")

    async def test_search_profile_and_sub_comments_use_expected_parameters(
        self,
    ) -> None:
        client, session = self.make_client(
            [
                FakeResponse({"status": "ok", "result": {"items": []}}),
                FakeResponse({"status": "ok", "result": {"account_detail": {}}}),
                FakeResponse({"status": "ok", "result": {"comments": []}}),
            ]
        )

        await client.search("AstrBot", search_type="link", offset=10, limit=5)
        await client.fetch_user_profile("88")
        await client.fetch_sub_comments(123, last_value=456)

        self.assertEqual(
            session.requests[0][1],
            "https://api.xiaoheihe.cn/bbs/app/api/general/search/v1",
        )
        self.assertEqual(session.requests[0][2]["params"]["search_type"], "link")
        self.assertEqual(session.requests[0][2]["params"]["offset"], "10")
        self.assertEqual(session.requests[1][2]["params"]["userid"], "88")
        self.assertEqual(session.requests[2][2]["params"]["root_comment_id"], "123")
        self.assertEqual(session.requests[2][2]["params"]["lastval"], "456")

    async def test_direct_message_uses_heybox_profile_and_copied_image(self) -> None:
        client, session = self.make_client(
            [
                FakeResponse(
                    {
                        "status": "ok",
                        "result": {"preview_url": "https://cdn.xiaoheihe.cn/dm.png"},
                    }
                ),
                FakeResponse({"status": "ok", "result": {"msg_id": "message-1"}}),
            ]
        )

        await client.send_direct_message(
            user_id="99",
            text="你好",
            image_url="https://images.example/dm.png",
        )

        method, url, kwargs = session.requests[1]
        self.assertEqual(method, "POST")
        self.assertEqual(url, "https://api.xiaoheihe.cn/chatroom/v2/msg/user")
        self.assertEqual(kwargs["params"]["to_user_id"], "99")
        self.assertEqual(kwargs["params"]["app"], "heybox")
        self.assertEqual(kwargs["params"]["heybox_id"], "42")
        self.assertNotIn("_notip", kwargs["params"])
        self.assertTrue(kwargs["params"]["hkey"])
        body = parse_qs(kwargs["data"], keep_blank_values=True)
        self.assertEqual(body["msg"], ["你好"])
        self.assertEqual(body["msg_type"], ["6"])
        self.assertEqual(body["img"], ["https://cdn.xiaoheihe.cn/dm.png"])
        self.assertTrue(body["heybox_ack_id"][0])
        self.assertEqual(
            kwargs["headers"]["Content-Type"],
            "application/x-www-form-urlencoded;charset=UTF-8",
        )
        self.assertEqual(kwargs["headers"]["Accept"], "application/json")
        self.assertEqual(kwargs["headers"]["Cookie"], "user_heybox_id=42")

    def test_direct_message_diagnostics_do_not_expose_sensitive_values(self) -> None:
        proxy_url = "socks5://user:password@127.0.0.1:1080"
        client = XhhClient(
            api_base_url="https://api.xiaoheihe.cn",
            reply_base_url="https://workshopapi.xiaoheihe.cn",
            version="999.0.4",
            web_version="2.5",
            device_id="sensitive-device-id",
            proxy_url=proxy_url,
            auth=AuthInfo(
                cookie="user_heybox_id=42; user_pkey=secret; tracking=value",
                heybox_id="42",
            ),
        )

        diagnostics = client.direct_message_diagnostics()
        rendered = repr(diagnostics)

        self.assertEqual(
            diagnostics["cookie_names"], ["user_heybox_id", "user_pkey"]
        )
        self.assertTrue(diagnostics["cookie_filtered"])
        self.assertTrue(diagnostics["proxy_enabled"])
        self.assertNotIn("secret", rendered)
        self.assertNotIn("tracking", rendered)
        self.assertNotIn("sensitive-device-id", rendered)
        self.assertNotIn(proxy_url, rendered)

    async def test_direct_message_reuses_only_captured_whitelist_params(self) -> None:
        session = FakeSession(
            [FakeResponse({"status": "ok", "result": {"msg_id": "message-1"}})]
        )
        client = XhhClient(
            api_base_url="https://api.xiaoheihe.cn",
            reply_base_url="https://workshopapi.xiaoheihe.cn",
            version="999.0.4",
            web_version="2.5",
            device_id="generated-device",
            direct_message_api_params_url=(
                "https://api.xiaoheihe.cn/bbs/app/feeds?app=heybox"
                "&version=777.1&web_version=7.7&device_id=captured-device"
                "&hkey=stale&_time=1&nonce=stale&to_user_id=attacker"
            ),
            auth=AuthInfo(
                cookie="user_heybox_id=42; token=value", heybox_id="42"
            ),
            session=session,  # type: ignore[arg-type]
        )

        await client.send_direct_message(user_id="99", text="测试")

        params = session.requests[0][2]["params"]
        self.assertEqual(params["app"], "heybox")
        self.assertEqual(params["version"], "777.1")
        self.assertEqual(params["web_version"], "7.7")
        self.assertEqual(params["device_id"], "captured-device")
        self.assertEqual(params["to_user_id"], "99")
        self.assertEqual(params["heybox_id"], "42")
        self.assertNotEqual(params["hkey"], "stale")
        self.assertNotEqual(params["nonce"], "stale")
        self.assertNotEqual(params["_time"], "1")

    async def test_direct_message_restriction_pauses_then_expires(
        self,
    ) -> None:
        client, session = self.make_client(
            [
                FakeResponse({"status": "failed", "msg": "您已被禁止发送消息行为"}),
                FakeResponse({"status": "ok", "result": {"msg_id": "message-2"}}),
            ]
        )

        with (
            patch("astrbot_plugin_xhhrobot.xhh_client.time.time", return_value=1000),
            self.assertRaises(XhhError) as first_error,
        ):
            await client.send_direct_message(user_id="99", text="测试")

        self.assertFalse(first_error.exception.retryable)
        self.assertTrue(first_error.exception.terminal)
        self.assertTrue(first_error.exception.action_restricted)

        with (
            patch("astrbot_plugin_xhhrobot.xhh_client.time.time", return_value=1001),
            self.assertRaises(XhhError) as second_error,
        ):
            await client.send_direct_message(user_id="100", text="暂停期间不应请求")

        self.assertTrue(second_error.exception.action_restricted)
        self.assertGreater(second_error.exception.retry_after or 0, 0)
        self.assertEqual(len(session.requests), 1)

        with patch(
            "astrbot_plugin_xhhrobot.xhh_client.time.time", return_value=2801
        ):
            await client.send_direct_message(user_id="100", text="暂停结束后发送")

        self.assertEqual(len(session.requests), 2)


if __name__ == "__main__":
    unittest.main()
