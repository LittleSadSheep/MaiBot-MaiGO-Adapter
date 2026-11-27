"""会话窗口归并、指令/水词过滤、高置信度自动预召回与精简回复注入专项测试。"""

from __future__ import annotations

from typing import Any, Dict, List

from tests.fake_host import (
    WHITELIST_GROUP_ID,
    WHITELIST_GROUP_NAME,
    WHITELIST_STREAM_ID,
    FakeHost,
    ManualClock,
    load_plugin_module,
)
from tests.test_maigo_adapter import _StubMaiGOClient, _inbound_msg, _make_config, _outbound_msg

plugin_mod = load_plugin_module()


async def test_speech_window_merging_and_low_signal_filtering() -> None:
    """验证多轮连续发言按窗口归并、指令回执与短水词被过滤、达轮次上限立即刷出。"""
    host = FakeHost()
    clock = ManualClock()
    client = _StubMaiGOClient()
    plugin = plugin_mod.MaiGOAdapterPlugin(client_factory=lambda _cfg: client, clock_ms=clock, start_background=False)
    plugin._ctx = host.make_context()
    cfg = _make_config()
    cfg.speech_buffer.flush_idle_ms = 90_000
    cfg.speech_buffer.max_turns = 3
    plugin._plugin_config_instance = cfg
    await plugin.on_load()

    try:
        await plugin.track_inbound_message(
            message=_inbound_msg(
                session_id=WHITELIST_STREAM_ID,
                user_id="10001",
                nickname="测试群友甲",
                text="昨晚建的晶体装配器好用吗",
                group_id=WHITELIST_GROUP_ID,
            )
        )

        # 1. 命令回执与短水词直接拦截，不进入缓冲也不触发上报
        for noise_id, noise_text, is_cmd in (
            ("m-perm", "你没有权限使用此命令", False),
            ("m-clear", "我暂时失忆了", False),
            ("m-cmd", "命令已执行成功，切回默认模式", True),
            ("m-short", "咋了", False),
            ("m-filler", "能啊", False),
        ):
            msg = _outbound_msg(session_id=WHITELIST_STREAM_ID, message_id=noise_id, text=noise_text)
            msg["is_command"] = is_cmd
            await plugin.report_outbound_speech(sent=True, message=msg)

        # 2. 同会话 2 轮有效回复留在缓冲区内等待合并，尚未立刻上报
        await plugin.report_outbound_speech(
            sent=True,
            message=_outbound_msg(
                session_id=WHITELIST_STREAM_ID,
                message_id="m-good-1",
                text="好用呀，昨天刚接上能源线就开始自动搓芯片了。",
            ),
        )
        await plugin.report_outbound_speech(
            sent=True,
            message=_outbound_msg(
                session_id=WHITELIST_STREAM_ID,
                message_id="m-good-2",
                text="就是红石线路还绕了一小圈，下次再整理一下布线。",
            ),
        )
        assert len(client.posted_batches) == 0

        # 3. 第 3 轮触达 max_turns=3 -> 立刻合并为单条窗口事件上报
        await plugin.report_outbound_speech(
            sent=True,
            message=_outbound_msg(
                session_id=WHITELIST_STREAM_ID,
                message_id="m-good-3",
                text="对了仓库那边也顺手扩了三排抽屉。",
            ),
        )
        await plugin.flush_pending_tasks()

        assert len(client.posted_batches) == 1
        merged_ev = client.posted_batches[0][0]
        assert merged_ev["idempotency_key"] == "maibot-win-m-good-1-m-good-3"
        assert merged_ev["metadata"]["window_turns"] == 3
        assert "自动搓芯片" in merged_ev["verbatim"]
        assert "抽屉" in merged_ev["verbatim"]
        assert "你没有权限" not in merged_ev["verbatim"]
        assert "暂时失忆" not in merged_ev["verbatim"]
    finally:
        await plugin.on_unload()


async def test_planner_auto_prerecall_routing_note_and_thin_replyer_injection() -> None:
    """验证规划前置工具分工提示、高置信度预召回注入，以及回复前置精简注入不堆砌慢层。"""
    host = FakeHost()
    clock = ManualClock()
    client = _StubMaiGOClient()
    client.recall_reply = "[2026-11-20 · 直播《下界探险》] 用床在下界炸出三块远古残骸"
    plugin = plugin_mod.MaiGOAdapterPlugin(client_factory=lambda _cfg: client, clock_ms=clock, start_background=False)
    plugin._ctx = host.make_context()
    plugin._plugin_config_instance = _make_config()
    await plugin.on_load()

    try:
        await plugin.track_inbound_message(
            message=_inbound_msg(
                session_id=WHITELIST_STREAM_ID,
                user_id="10001",
                nickname="测试群友甲",
                text="你上次下界挖远古残骸挖到几块呀",
                group_id=WHITELIST_GROUP_ID,
                group_name=WHITELIST_GROUP_NAME,
            )
        )
        items: List[Dict[str, Any]] = [
            {"item_type": "SystemMessageItem", "parts": [{"type": "text", "text": "系统人设"}]},
            {"item_type": "UserMessageItem", "parts": [{"type": "text", "text": "当前时刻指令"}]},
        ]
        res = await plugin.inject_into_planner(session_id=WHITELIST_STREAM_ID, items=items, item_schema_version=1)
        new_items = res["modified_kwargs"]["items"]

        # 慢层稳定位固定包含 recall_life 与 query_memory 分工提示
        stable_text = new_items[1]["parts"][0]["text"]
        assert "recall_life" in stable_text and "query_memory" in stable_text

        # 自动预召回以 days=0 / high_confidence_only=True / branch_id=maibot 触发并注入动态块
        assert len(client.recall_calls) == 1
        call = client.recall_calls[0]
        assert call["days"] == 0 and call["high_confidence_only"] is True and call["branch_id"] == "maibot"
        dynamic_text = new_items[-2]["parts"][0]["text"]
        assert "【顺手想起来的跨端细节】" in dynamic_text
        assert "远古残骸" in dynamic_text

        # 回复前置仅精简注入动态近况、待办约定与本轮预召回，不再重复堆砌整段慢层 stable_brief
        rep = await plugin.inject_into_replyer(session_id=WHITELIST_STREAM_ID, extra_prompt="保持自然语气")
        rep_prompt = rep["modified_kwargs"]["extra_prompt"]
        assert "远古残骸" in rep_prompt
        assert "今晚在 MC 建一座新灯塔" in rep_prompt
        assert "在B站刷到手工粘土视频" not in rep_prompt

        # 工具调用 recall_life 支持 days=0 全量历史并携带 branch_id
        tool_res = await plugin.handle_recall_life(
            query="远古残骸", days=0, limit=3, group_id=WHITELIST_GROUP_ID, platform="qq"
        )
        assert tool_res["success"] is True
        assert client.recall_calls[-1]["days"] == 0
        assert client.recall_calls[-1]["branch_id"] == "maibot"
    finally:
        await plugin.on_unload()
