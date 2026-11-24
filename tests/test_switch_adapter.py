"""MaiBot-Switch-Adapter 完整行为与真实 HTTP 联调测试。"""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

from tests.fake_host import (
    NOTIFY_GROUP_ID,
    NOTIFY_STREAM_ID,
    NON_WHITELIST_GROUP_ID,
    NON_WHITELIST_STREAM_ID,
    PRIVATE_STREAM_ID,
    WHITELIST_GROUP_ID,
    WHITELIST_GROUP_NAME,
    WHITELIST_STREAM_ID,
    FakeHost,
    ManualClock,
    load_plugin_module,
)

plugin_mod = load_plugin_module()
SwitchAdapterConfig = plugin_mod.SwitchAdapterConfig


class _StubSwitchClient:
    """可注入预设视图与离线故障的 SwitchClient 替身。"""

    def __init__(self) -> None:
        self.posted_batches: List[List[Dict[str, Any]]] = []
        self.context_requests: List[Dict[str, Any]] = []
        self.recall_calls: List[Dict[str, Any]] = []
        self.fail_network = False
        self.context_by_vis: Dict[str, Dict[str, Any]] = {
            "group": {
                "brief_etag": "etag-g1",
                "stable_brief": "【麦麦今天的经历】\n- 下午：在B站刷到手工粘土视频。",
                "live_context": "今晚准备测试跨端直播记忆。",
                "actor_cards": [
                    {
                        "person_id": 1,
                        "display_name": "老熟人甲",
                        "hint": "老熟人甲：刚刚在B站视频评论区见过。",
                        "platforms": ["qq", "bilibili"],
                    }
                ],
                "fresh_topics": [
                    {
                        "id": 11,
                        "title": "粘土手工视频",
                        "summary": "下午留过评的粘土手艺",
                        "reaction": "觉得挺有趣的",
                        "source_branch": "bilibili_ops",
                        "consumed_branches": [],
                        "transition_hint": "",
                    }
                ],
                "open_promises": [
                    {
                        "id": 3,
                        "anchor_type": "promise",
                        "content": "今晚在 MC 建一座新灯塔",
                        "status": "open",
                        "source_branch": "maibot",
                    }
                ],
                "active_sessions": [],
            },
            "private": {
                "brief_etag": "etag-p1",
                "stable_brief": "【私密经历】\n- 私下说过不想公开的小事。",
                "live_context": "",
                "actor_cards": [],
                "fresh_topics": [],
                "open_promises": [],
                "active_sessions": [],
            },
        }
        self.recall_reply = "今日下午刷过手工粘土视频，昨晚也聊过新灯塔计划。"

    async def post_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        if self.fail_network:
            raise plugin_mod.SwitchClientError("network down")
        self.posted_batches.append(list(events))
        return {"ok": True, "accepted": len(events), "deduplicated": 0}

    async def get_context(
        self,
        *,
        branch_id: str,
        target_visibility: str,
        if_brief_etag: str = "",
        active_actors: str = "",
        save_id: str = "",
        include_topics: bool = True,
    ) -> Dict[str, Any]:
        if self.fail_network:
            raise plugin_mod.SwitchClientError("network down")
        self.context_requests.append(
            {
                "branch_id": branch_id,
                "target_visibility": target_visibility,
                "if_brief_etag": if_brief_etag,
                "active_actors": active_actors,
                "save_id": save_id,
                "include_topics": include_topics,
            }
        )
        base = dict(self.context_by_vis[target_visibility])
        if if_brief_etag and if_brief_etag == base.get("brief_etag"):
            base["stable_unchanged"] = True
            base["stable_brief"] = None
        else:
            base["stable_unchanged"] = False
        return base

    async def consume_topic(self, topic_id: int, *, branch_id: str) -> Dict[str, Any]:
        return {"ok": True, "topic_id": topic_id, "branch_id": branch_id}

    async def recall_life(
        self,
        *,
        query: str,
        days: int = 7,
        limit: int = 5,
        target_visibility: str = "public",
    ) -> str:
        if self.fail_network:
            raise plugin_mod.SwitchClientError("network down")
        self.recall_calls.append({"query": query, "days": days, "limit": limit, "target_visibility": target_visibility})
        return self.recall_reply

    async def close(self) -> None:
        return None


def _make_config(
    *,
    group_ids: List[str] | None = None,
    notify_groups: List[str] | None = None,
    delay_ms: int = 300_000,
    sync_private: bool = True,
    server_url: str = "http://127.0.0.1:8768",
    token: str = "",
) -> Any:
    return SwitchAdapterConfig.model_validate(
        {
            "plugin": {"enabled": True},
            "server": {"url": server_url, "token": token, "branch_id": "maibot"},
            "scope": {
                "platform": "qq",
                "group_ids": [WHITELIST_GROUP_ID] if group_ids is None else group_ids,
                "sync_private": sync_private,
            },
            "injection": {
                "inject_planner": True,
                "inject_replyer": True,
                "include_actor_cards": True,
                "include_topics": True,
            },
            "go_live": {
                "group_ids": [] if notify_groups is None else notify_groups,
                "delay_ms": delay_ms,
                "stale_after_ms": 180_000,
            },
        }
    )


def _inbound_msg(
    *,
    session_id: str,
    user_id: str,
    nickname: str,
    text: str,
    group_id: str | None = None,
    group_name: str = WHITELIST_GROUP_NAME,
) -> Dict[str, Any]:
    msg_info: Dict[str, Any] = {"user_info": {"user_id": user_id, "user_nickname": nickname, "user_cardname": nickname}}
    if group_id is not None:
        msg_info["group_info"] = {"group_id": group_id, "group_name": group_name}
    return {
        "message_id": f"in-{session_id}-{user_id}",
        "platform": "qq",
        "session_id": session_id,
        "processed_plain_text": text,
        "message_info": msg_info,
    }


def _outbound_msg(*, session_id: str, message_id: str, text: str, is_notify: bool = False) -> Dict[str, Any]:
    return {
        "message_id": message_id,
        "platform": "qq",
        "session_id": session_id,
        "processed_plain_text": text,
        "is_notify": is_notify,
        "message_info": {"user_info": {"user_id": "bot", "user_nickname": "麦麦"}},
    }


async def test_inbound_tracking_and_outbound_visibility_reporting() -> None:
    host = FakeHost()
    clock = ManualClock()
    client = _StubSwitchClient()
    plugin = plugin_mod.MaiSwitchAdapterPlugin(
        client_factory=lambda _cfg: client, clock_ms=clock, start_background=False
    )
    plugin._ctx = host.make_context()
    plugin._plugin_config_instance = _make_config()
    await plugin.on_load()

    try:
        # 1. 白名单群聊：入站记录发言人，出站 sent=True 按 visibility="group" 上报
        await plugin.track_inbound_message(
            message=_inbound_msg(
                session_id=WHITELIST_STREAM_ID,
                user_id="10001",
                nickname="老熟人甲",
                text="你下午是不是刷了个手工视频",
                group_id=WHITELIST_GROUP_ID,
            )
        )
        await plugin.report_outbound_speech(
            sent=True,
            message=_outbound_msg(
                session_id=WHITELIST_STREAM_ID,
                message_id="out-g1",
                text="对呀！那个粘土甲鱼眼神特别呆滞哈哈哈，晚上直播咱去建个灯塔！",
            ),
        )
        await plugin.flush_pending_tasks()

        # 2. 私聊窗口：入站无 group_info，出站按 visibility="private" 上报
        await plugin.track_inbound_message(
            message=_inbound_msg(
                session_id=PRIVATE_STREAM_ID,
                user_id="10001",
                nickname="老熟人甲",
                text="今天工作好累想悄悄吐槽两句",
                group_id=None,
            )
        )
        await plugin.report_outbound_speech(
            sent=True,
            message=_outbound_msg(
                session_id=PRIVATE_STREAM_ID,
                message_id="out-p1",
                text="辛苦啦，偷偷摸摸头，下播再陪你慢慢说。",
            ),
        )
        await plugin.flush_pending_tasks()

        # 3. 非白名单群聊以及 sent=False 均不上报
        await plugin.track_inbound_message(
            message=_inbound_msg(
                session_id=NON_WHITELIST_STREAM_ID,
                user_id="20005",
                nickname="路人乙",
                text="其他群消息",
                group_id=NON_WHITELIST_GROUP_ID,
                group_name="其他群",
            )
        )
        await plugin.report_outbound_speech(
            sent=True,
            message=_outbound_msg(session_id=NON_WHITELIST_STREAM_ID, message_id="out-x1", text="不应上报"),
        )
        await plugin.report_outbound_speech(
            sent=False,
            message=_outbound_msg(session_id=WHITELIST_STREAM_ID, message_id="out-fail", text="发送失败不上报"),
        )
        await plugin.flush_pending_tasks()

        assert len(client.posted_batches) == 2
        g_evt = client.posted_batches[0][0]
        assert g_evt["visibility"] == "group" and g_evt["actor_role"] == "self"
        assert g_evt["actors"][0]["external_id"] == "10001"
        assert "灯塔" in g_evt["verbatim"]

        p_evt = client.posted_batches[1][0]
        assert p_evt["visibility"] == "private" and p_evt["actor_role"] == "self"
        assert "悄悄" in p_evt["summary"] or "摸摸头" in p_evt["verbatim"]
    finally:
        await plugin.on_unload()


async def test_planner_two_layer_injection_replyer_and_non_whitelist_isolation() -> None:
    host = FakeHost()
    clock = ManualClock()
    client = _StubSwitchClient()
    plugin = plugin_mod.MaiSwitchAdapterPlugin(
        client_factory=lambda _cfg: client, clock_ms=clock, start_background=False
    )
    plugin._ctx = host.make_context()
    plugin._plugin_config_instance = _make_config()
    await plugin.on_load()

    try:
        await plugin.track_inbound_message(
            message=_inbound_msg(
                session_id=WHITELIST_STREAM_ID,
                user_id="10001",
                nickname="老熟人甲",
                text="刚来群里看看",
                group_id=WHITELIST_GROUP_ID,
            )
        )
        items = [
            {"item_type": "SystemMessageItem", "parts": [{"type": "text", "text": "系统人设"}]},
            {"item_type": "UserMessageItem", "parts": [{"type": "text", "text": "老熟人甲: 刚来群里看看"}]},
            {"item_type": "UserMessageItem", "parts": [{"type": "text", "text": "当前时刻指令"}]},
        ]
        res = await plugin.inject_into_planner(session_id=WHITELIST_STREAM_ID, items=items, item_schema_version=1)
        new_items = res["modified_kwargs"]["items"]
        # 慢层插在 SystemMessageItem 之后（下标 1），动态层插在末尾指令项之前（下标 -2）
        assert len(new_items) == 5
        assert "<maiswitch-life-brief>" in new_items[1]["parts"][0]["text"]
        assert "手工粘土视频" in new_items[1]["parts"][0]["text"]
        assert "私下说过不想公开的小事" not in new_items[1]["parts"][0]["text"]

        dynamic_text = new_items[-2]["parts"][0]["text"]
        assert "<maiswitch-context>" in dynamic_text
        assert "老熟人甲" in dynamic_text
        assert "新灯塔" in dynamic_text
        assert new_items[-1]["parts"][0]["text"] == "当前时刻指令"

        # 确认调用参数携带 active_actors="qq:10001"，第二次命中 if_brief_etag 缓存
        assert client.context_requests[0]["active_actors"] == "qq:10001"
        res2 = await plugin.inject_into_planner(session_id=WHITELIST_STREAM_ID, items=items, item_schema_version=1)
        assert client.context_requests[1]["if_brief_etag"] == "etag-g1"
        assert "手工粘土视频" in res2["modified_kwargs"]["items"][1]["parts"][0]["text"]

        # 协议版本不符安全跳过
        bad_ver = await plugin.inject_into_planner(session_id=WHITELIST_STREAM_ID, items=items, item_schema_version=99)
        assert bad_ver == {"action": "continue"}

        # 非白名单群不注入
        non_wl = await plugin.inject_into_planner(
            session_id=NON_WHITELIST_STREAM_ID, items=items, item_schema_version=1
        )
        assert non_wl == {"action": "continue"}

        # 回复前置勾子保留现有 extra_prompt 并追加
        rep = await plugin.inject_into_replyer(session_id=WHITELIST_STREAM_ID, extra_prompt="保持俏皮语气")
        combined = rep["modified_kwargs"]["extra_prompt"]
        assert combined.startswith("保持俏皮语气") and "<maiswitch-reply-context>" in combined
        assert "今晚在 MC 建一座新灯塔" in combined

        # 离线场景退回本地已持久化缓存继续可用，绝不抛异常
        client.fail_network = True
        offline_res = await plugin.inject_into_planner(
            session_id=WHITELIST_STREAM_ID, items=items, item_schema_version=1
        )
        assert "手工粘土视频" in offline_res["modified_kwargs"]["items"][1]["parts"][0]["text"]
    finally:
        await plugin.on_unload()


async def test_go_live_delay_announcement_and_persistence() -> None:
    host = FakeHost()
    clock = ManualClock(start_ms=10_000_000)
    client = _StubSwitchClient()
    plugin = plugin_mod.MaiSwitchAdapterPlugin(
        client_factory=lambda _cfg: client, clock_ms=clock, start_background=False
    )
    plugin._ctx = host.make_context()
    plugin._plugin_config_instance = _make_config(notify_groups=[NOTIFY_GROUP_ID], delay_ms=300_000)
    await plugin.on_load()

    try:
        # 刚开播 60 秒（未满 5 分钟延迟门槛），不喊人
        client.context_by_vis["group"]["active_sessions"] = [
            {
                "checkout_id": "chk-live-7",
                "branch_id": "amaidesu",
                "external_session_id": "7",
                "title": "沿海造灯塔",
                "platform": "bilibili",
                "room_id": "9001",
                "segment_title": "闲聊开场",
                "started_at_ms": 10_000_000,
                "last_heartbeat_ms": 10_060_000,
            }
        ]
        clock.advance(60_000)
        await plugin.refresh_once()
        assert host.trigger_calls == []

        # 推进到 310 秒并刷新最新心跳，达标触发一次喊人
        clock.advance(250_000)
        client.context_by_vis["group"]["active_sessions"][0]["last_heartbeat_ms"] = clock.now_ms
        await plugin.refresh_once()
        assert len(host.trigger_calls) == 1
        call = host.trigger_calls[0]
        assert call["stream_id"] == NOTIFY_STREAM_ID
        assert "https://live.bilibili.com/9001" in call["intent"]

        # 重启插件实例再刷同一场次，因 go_live_announced.json 落盘，不会重复喊人
        await plugin.on_unload()
        plugin2 = plugin_mod.MaiSwitchAdapterPlugin(
            client_factory=lambda _cfg: client, clock_ms=clock, start_background=False
        )
        plugin2._ctx = host.make_context()
        plugin2._plugin_config_instance = _make_config(notify_groups=[NOTIFY_GROUP_ID], delay_ms=300_000)
        await plugin2.on_load()
        await plugin2.refresh_once()
        assert len(host.trigger_calls) == 1
        await plugin2.on_unload()
    finally:
        await plugin.on_unload()


async def test_recall_life_tool_and_real_http_loopback_server() -> None:
    # 1. 先起一个轻量标准库环回 HTTP 服务器验证 HttpSwitchClient 真实鉴权与 JSON 通信
    server_hits: List[str] = []

    class _LoopbackHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def _write_json(self, status: int, payload: Dict[str, Any]) -> None:
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:
            auth = self.headers.get("Authorization", "")
            if auth != "Bearer real-tok":
                self._write_json(401, {"detail": "bad token"})
                return
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            if parsed.path == "/api/v1/views/recall":
                query = qs.get("query", [""])[0]
                days = qs.get("days", ["7"])[0]
                limit = qs.get("limit", ["5"])[0]
                vis = qs.get("target_visibility", ["public"])[0]
                server_hits.append(f"{vis}:{query}:{days}:{limit}")
                self._write_json(200, {"ok": True, "text": f"[{vis}] 记得：{query}"})
                return
            if parsed.path == "/api/v1/views/context":
                vis = qs.get("target_visibility", ["public"])[0]
                self._write_json(200, {"brief_etag": f"e-{vis}", "stable_brief": "环回测试简报", "active_sessions": []})
                return
            self._write_json(404, {"detail": "not found"})

    server = ThreadingHTTPServer(("127.0.0.1", 0), _LoopbackHandler)
    port = int(server.server_address[1])
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    host = FakeHost()
    plugin = plugin_mod.MaiSwitchAdapterPlugin(start_background=False)
    plugin._ctx = host.make_context()
    plugin._plugin_config_instance = _make_config(
        server_url=f"http://127.0.0.1:{port}", token="real-tok", sync_private=True
    )
    await plugin.on_load()

    try:
        # 群聊调用强制走 group 可见性
        res_g = await plugin.handle_recall_life(
            query="灯塔", days=3, limit=4, group_id=WHITELIST_GROUP_ID, platform="qq"
        )
        assert res_g["success"] is True and "[group]" in res_g["content"]

        # 非白名单群号仅降级走 public
        res_p = await plugin.handle_recall_life(query="灯塔", group_id=NON_WHITELIST_GROUP_ID, platform="qq")
        assert res_p["success"] is True and "[public]" in res_p["content"]

        # 私聊上下文走 private
        res_priv = await plugin.handle_recall_life(query="悄悄话", is_private=True)
        assert res_priv["success"] is True and "[private]" in res_priv["content"]
        assert len(server_hits) == 3

        # 刷新走通真实 /api/v1/views/context
        await plugin.refresh_once()
        assert plugin._cache is not None and plugin._cache.view_of("group").stable_brief == "环回测试简报"
    finally:
        await plugin.on_unload()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    # 离线降级：关闭服务器后再调用 recall_life 返回温和失败结果而不抛异常
    await plugin.on_load()
    try:
        err_res = await plugin.handle_recall_life(query="灯塔")
        assert err_res["success"] is False and "连不上 MaiSwitch" in err_res["error"]
    finally:
        await plugin.on_unload()
