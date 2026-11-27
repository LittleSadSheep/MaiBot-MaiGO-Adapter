"""MaiBot-MaiGO-Adapter 主插件：连接 MaiBot 与 MaiGO 跨端记忆生活流中心。"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional
import uuid

from maibot_sdk import HookHandler, MaiBotPlugin, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType

from .backfill import HistoryBackfiller
from .config import MaiGOAdapterConfig
from .context_cache import (
    ContextCacheStore,
    SessionTracker,
    extract_plain_text,
    format_dynamic_block,
    insert_dynamic_context_item,
    insert_stable_brief_item,
)
from .go_live import GoLiveAnnouncer, GoLiveSettings
from .maigo_client import HttpMaiGOClient, MaiGOClient, MaiGOClientError
from .speech_buffer import SpeechWindowBuffer, is_pre_recall_candidate_query

_CONTEXT_ITEM_SCHEMA_VERSION = 1
_PRE_RECALL_CACHE_TTL_MS = 60_000


def _default_now_ms() -> int:
    return int(time.time() * 1000)


class MaiGOAdapterPlugin(MaiBotPlugin):
    """零侵入对接 MaiGO 的 MaiBot 轻量适配器插件。

    职责边界：
    1. 入站监听 ``chat.receive.after_process`` 追踪会话作用域、近期发言人与当前提问正文；
    2. 出站监听 ``send_service.after_send``，经窗口归并缓冲器过滤命令回执与水词后，
       将白名单群（``group``）与私聊（``private``）的高信噪比对话段异步上报给 ``POST /api/v1/events``；
    3. 规划前置注入带工具分工说明的慢层简报、高置信度短超时自动预召回与快层动态上下文；
       回复前置改走精简注入（动态近况 + 最多 3 条约定 + 本轮预召回），避免重复堆砌慢层；
    4. 定时刷新本地缓存，监听其他端稳定开播并在通知群延迟喊人，暴露 ``recall_life`` 工具供深度翻查。
    """

    config_model = MaiGOAdapterConfig

    def __init__(
        self,
        *,
        client_factory: Optional[Callable[[MaiGOAdapterConfig], MaiGOClient]] = None,
        clock_ms: Callable[[], int] = _default_now_ms,
        start_background: bool = True,
    ) -> None:
        super().__init__()
        self._client_factory = client_factory
        self._clock_ms = clock_ms
        self._start_background = start_background
        self._client: Optional[MaiGOClient] = None
        self._tracker: Optional[SessionTracker] = None
        self._cache: Optional[ContextCacheStore] = None
        self._go_live: Optional[GoLiveAnnouncer] = None
        self._backfiller: Optional[HistoryBackfiller] = None
        self._speech_buffer: Optional[SpeechWindowBuffer] = None
        self._last_pre_recall: Dict[str, tuple[int, str]] = {}
        self._poll_task: Optional[asyncio.Task[None]] = None
        self._backfill_task: Optional[asyncio.Task[int]] = None
        self._pending_tasks: set[asyncio.Task[Any]] = set()
        self._schema_mismatch_logged = False

    def _build_client(self, cfg: MaiGOAdapterConfig) -> MaiGOClient:
        if self._client_factory is not None:
            return self._client_factory(cfg)
        return HttpMaiGOClient(
            base_url=cfg.server.url,
            token=cfg.server.token,
            timeout_ms=cfg.server.request_timeout_ms,
        )

    def _go_live_settings(self, cfg: MaiGOAdapterConfig) -> GoLiveSettings:
        groups = tuple(dict.fromkeys(item.strip() for item in cfg.go_live.group_ids if item.strip()))
        return GoLiveSettings(
            group_ids=groups,
            platform=cfg.scope.platform.strip() or "qq",
            self_branch_id=(cfg.server.branch_id or "maibot").strip().lower(),
            delay_ms=cfg.go_live.delay_ms,
            stale_after_ms=cfg.go_live.stale_after_ms,
            room_url=cfg.go_live.room_url.strip(),
        )

    async def on_load(self) -> None:
        """初始化 HTTP 客户端、作用域追踪器、持久化缓存并按需开启后台同步循环。"""
        cfg = self.config
        self._client = self._build_client(cfg)
        self._tracker = SessionTracker(
            self.ctx,
            platform=cfg.scope.platform,
            group_ids=cfg.scope.group_ids,
            sync_private=cfg.scope.sync_private,
            logger=self.ctx.logger,
            clock_ms=self._clock_ms,
        )
        self._cache = ContextCacheStore(
            self.ctx.paths.data_dir / "maigo_cache.json",
            logger=self.ctx.logger,
            clock_ms=self._clock_ms,
        )
        self._cache.load()
        self._go_live = GoLiveAnnouncer(
            self.ctx,
            self._go_live_settings(cfg),
            state_path=self.ctx.paths.data_dir / "go_live_announced.json",
            logger=self.ctx.logger,
            clock_ms=self._clock_ms,
        )
        self._go_live.load()
        self._backfiller = HistoryBackfiller(
            data_dir=self.ctx.paths.data_dir,
            client=self._client,
            logger=self.ctx.logger,
            branch_id=(cfg.server.branch_id or "maibot").strip().lower(),
            platform=cfg.scope.platform,
            group_ids=cfg.scope.group_ids,
            sync_private=cfg.scope.sync_private,
            days=cfg.backfill.days,
            min_chars=cfg.backfill.min_chars,
            batch_size=cfg.backfill.batch_size,
            batch_interval_ms=cfg.backfill.batch_interval_ms,
            db_path=cfg.backfill.db_path,
            clock_ms=self._clock_ms,
        )
        self._speech_buffer = SpeechWindowBuffer(
            on_flush=self._send_events_safe,
            branch_id=(cfg.server.branch_id or "maibot").strip().lower(),
            enabled=cfg.speech_buffer.enabled,
            flush_idle_ms=cfg.speech_buffer.flush_idle_ms,
            max_turns=cfg.speech_buffer.max_turns,
            min_chars=cfg.speech_buffer.min_chars,
            clock_ms=self._clock_ms,
        )
        if self._start_background and cfg.plugin.enabled and cfg.server.url.strip():
            self._poll_task = asyncio.create_task(self._poll_loop(), name="maigo_adapter.poll")

    async def on_unload(self) -> None:
        """停后台轮询、刷出窗口归并缓冲、等待收尾上报任务并关闭连接。"""
        for bg in (self._poll_task, self._backfill_task):
            if bg is not None and not bg.done():
                bg.cancel()
                try:
                    await bg
                except asyncio.CancelledError:
                    pass
        self._poll_task = None
        self._backfill_task = None
        await self.flush_pending_tasks()
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def on_config_update(self, scope: str, config_data: Dict[str, Any], version: str) -> None:
        """响应配置变更：更新白名单、喊人配置、窗口缓冲与服务端连接参数。"""
        del config_data, version
        if scope != "self":
            return
        cfg = self.config
        if self._tracker is not None:
            self._tracker.update_settings(
                platform=cfg.scope.platform,
                group_ids=cfg.scope.group_ids,
                sync_private=cfg.scope.sync_private,
            )
        if self._go_live is not None:
            self._go_live.update_settings(self._go_live_settings(cfg))
        if self._client is not None:
            await self._client.close()
        self._client = self._build_client(cfg)
        if self._speech_buffer is not None:
            self._speech_buffer.update_settings(
                branch_id=(cfg.server.branch_id or "maibot").strip().lower(),
                enabled=cfg.speech_buffer.enabled,
                flush_idle_ms=cfg.speech_buffer.flush_idle_ms,
                max_turns=cfg.speech_buffer.max_turns,
                min_chars=cfg.speech_buffer.min_chars,
            )
        if self._backfiller is not None:
            self._backfiller.update_settings(
                client=self._client,
                branch_id=(cfg.server.branch_id or "maibot").strip().lower(),
                platform=cfg.scope.platform,
                group_ids=cfg.scope.group_ids,
                sync_private=cfg.scope.sync_private,
                days=cfg.backfill.days,
                min_chars=cfg.backfill.min_chars,
                batch_size=cfg.backfill.batch_size,
                batch_interval_ms=cfg.backfill.batch_interval_ms,
                db_path=cfg.backfill.db_path,
            )

    async def flush_pending_tasks(self) -> None:
        """立即刷出会话窗口缓冲区并等待在途异步事件上报任务全部结束（卸载与确定性测试使用）。"""
        if self._speech_buffer is not None:
            await self._speech_buffer.flush_all()
        tasks = [t for t in self._pending_tasks if not t.done()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._pending_tasks.clear()

    def _spawn_background(self, coro: Any, *, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        self._pending_tasks.add(task)
        task.add_done_callback(self._on_bg_done)

    def _on_bg_done(self, task: asyncio.Task[Any]) -> None:
        self._pending_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self.ctx.logger.warning(f"MaiGO 插件后台任务 {task.get_name()} 失败: {exc}")

    # ------------------------------------------------------------------
    # 入站追踪与出站窗口归并采集
    # ------------------------------------------------------------------

    @HookHandler(
        "chat.receive.after_process",
        description="轻量追踪互通群/私聊会话类型、近期发言人与当前提问，供预召回与跨端名片使用",
    )
    async def track_inbound_message(self, **kwargs: Any) -> Dict[str, Any]:
        message = kwargs.get("message")
        if self._tracker is not None and isinstance(message, dict) and self.config.plugin.enabled:
            self._tracker.record_inbound(message)
        return {"action": "continue"}

    @HookHandler(
        "send_service.after_send",
        description="麦麦在白名单互通群或私聊成功回复后，经噪声过滤与会话窗口归并异步上报至 MaiGO",
    )
    async def report_outbound_speech(self, **kwargs: Any) -> Dict[str, Any]:
        if not self.config.plugin.enabled or not kwargs.get("sent"):
            return {"action": "continue"}
        message = kwargs.get("message")
        if not isinstance(message, dict) or bool(message.get("is_notify", False)):
            return {"action": "continue"}
        if self._tracker is None or self._client is None or self._speech_buffer is None:
            return {"action": "continue"}

        session_id = str(message.get("session_id") or "").strip()
        vis = await self._tracker.resolve_visibility(session_id)
        if vis is None:
            return {"action": "continue"}

        verbatim = extract_plain_text(message)
        if not verbatim:
            return {"action": "continue"}

        msg_id = str(message.get("message_id") or "").strip() or uuid.uuid4().hex[:12]
        actors = self._tracker.recent_outbound_actors(session_id)
        inbound_hint = self._tracker.last_inbound_hint(session_id)
        scope_label = "私聊" if vis == "private" else f"群「{self._tracker.group_label(session_id)}」"
        # 入队窗口缓冲器：过滤命令回执/水词，达到轮次上限立即弹出，否则按空闲窗口合并后自动刷出
        ready_events = self._speech_buffer.enqueue(
            session_id=session_id,
            visibility=vis,
            scope_label=scope_label,
            msg_id=msg_id,
            verbatim=verbatim,
            inbound_hint=inbound_hint,
            actors=actors,
            is_command=bool(message.get("is_command", False)),
            is_notify=bool(message.get("is_notify", False)),
        )
        if ready_events:
            self._spawn_background(self._send_events_safe(ready_events), name="maigo_adapter.report_send")
        return {"action": "continue"}

    async def _send_events_safe(self, events: List[Dict[str, Any]]) -> None:
        if self._client is None:
            return
        try:
            await self._client.post_events(events)
        except MaiGOClientError as exc:
            self.ctx.logger.warning(f"出站发言上报 MaiGO 失败（本地不受影响）: {exc}")

    # ------------------------------------------------------------------
    # 规划与回复双钩子注入
    # ------------------------------------------------------------------

    @HookHandler(
        "maisaka.planner.before_request",
        description="规划前置：注入稳定位慢层简报（含工具分工提示）与末尾快层动态、熟人名片及高置信度自动预召回",
    )
    async def inject_into_planner(self, **kwargs: Any) -> Dict[str, Any]:
        if not self.config.plugin.enabled or not self.config.injection.inject_planner:
            return {"action": "continue"}
        if self._tracker is None or self._cache is None:
            return {"action": "continue"}

        session_id = str(kwargs.get("session_id") or "").strip()
        vis = await self._tracker.resolve_visibility(session_id)
        if vis is None:
            return {"action": "continue"}

        items = kwargs.get("items")
        if not isinstance(items, list):
            return {"action": "continue"}
        if kwargs.get("item_schema_version") != _CONTEXT_ITEM_SCHEMA_VERSION:
            if not self._schema_mismatch_logged:
                self._schema_mismatch_logged = True
                self.ctx.logger.warning(
                    f"MaiBot 上下文协议版本为 {kwargs.get('item_schema_version')!r}，本插件预期 {_CONTEXT_ITEM_SCHEMA_VERSION}，已跳过注入"
                )
            return {"action": "continue"}

        branch_id = (self.config.server.branch_id or "maibot").strip().lower()
        cached = self._cache.view_of(vis)
        active_actors = (
            self._tracker.recent_actors_param(session_id) if self.config.injection.include_actor_cards else ""
        )
        actor_cards: List[Dict[str, Any]] = []
        pre_recalled = ""
        if self._client is not None and self.config.server.url.strip():
            try:
                raw = await self._client.get_context(
                    branch_id=branch_id,
                    target_visibility=vis,
                    if_brief_etag=cached.brief_etag,
                    active_actors=active_actors,
                    include_topics=self.config.injection.include_topics,
                )
                cached = self._cache.apply_context_response(vis, raw)
                actor_cards = [dict(card) for card in (raw.get("actor_cards") or ()) if isinstance(card, dict)]
            except MaiGOClientError as exc:
                self.ctx.logger.warning(f"拉取 MaiGO 实时上下文失败，改用本地持久化缓存: {exc}")

            # 双保险自动预召回：仅对有效提问以短超时查强命中跨端细节，未命中或超时静默跳过不拖慢主链
            query_text = self._tracker.last_inbound_query(session_id)
            if self.config.injection.pre_recall_enabled and is_pre_recall_candidate_query(query_text):
                try:
                    pre_recalled = (
                        await self._client.recall_life(
                            query=query_text[:120],
                            days=0,
                            limit=3,
                            target_visibility=vis,
                            branch_id=branch_id,
                            high_confidence_only=True,
                            timeout_ms=self.config.injection.pre_recall_timeout_ms,
                        )
                    ).strip()
                except MaiGOClientError as exc:
                    self.ctx.logger.debug(f"规划前置自动预召回跳过（不阻塞主链）: {exc}")

        if pre_recalled:
            self._last_pre_recall[session_id] = (self._clock_ms(), pre_recalled)
        else:
            self._last_pre_recall.pop(session_id, None)

        new_items = list(items)
        if cached.stable_brief.strip():
            new_items = insert_stable_brief_item(
                new_items,
                cached.stable_brief,
                item_id=f"maigo-stable-brief-{vis}",
            )
        dynamic_text = format_dynamic_block(
            live_context=cached.live_context,
            actor_cards=actor_cards if self.config.injection.include_actor_cards else (),
            fresh_topics=cached.fresh_topics if self.config.injection.include_topics else (),
            open_promises=cached.open_promises,
            pre_recalled=pre_recalled,
        )
        if dynamic_text:
            new_items = insert_dynamic_context_item(
                new_items,
                dynamic_text,
                item_id=f"maigo-dynamic-context-{vis}",
            )
        return {"action": "continue", "modified_kwargs": {**kwargs, "items": new_items}}

    @HookHandler(
        "maisaka.replyer.before_request",
        description="回复前置：精简注入动态近况、待兑现约定与本轮预召回细节，不重复堆砌长篇慢层",
    )
    async def inject_into_replyer(self, **kwargs: Any) -> Dict[str, Any]:
        if not self.config.plugin.enabled or not self.config.injection.inject_replyer:
            return {"action": "continue"}
        if self._tracker is None or self._cache is None:
            return {"action": "continue"}

        session_id = str(kwargs.get("session_id") or "").strip()
        vis = await self._tracker.resolve_visibility(session_id)
        if vis is None:
            return {"action": "continue"}

        view = self._cache.view_of(vis)
        max_live = max(100, int(self.config.injection.replyer_max_chars))
        snippets: List[str] = []
        if view.live_context.strip():
            live_short = view.live_context.strip()
            if len(live_short) > max_live:
                live_short = live_short[: max_live - 1].rstrip() + "…"
            snippets.append(f"跨端动态：{live_short}")
        if view.open_promises:
            promise_titles = "；".join(
                str(p.get("content") or "").strip() for p in view.open_promises[:3] if p.get("content")
            )
            if promise_titles:
                snippets.append(f"待兑现约定：{promise_titles}")

        # 若本轮规划前置命中了高置信度跨端细节（60 秒有效期内），同步透传给回复器避免细节丢漏
        cached_recall = self._last_pre_recall.get(session_id)
        if cached_recall is not None:
            ts_ms, recalled_text = cached_recall
            if self._clock_ms() - ts_ms <= _PRE_RECALL_CACHE_TTL_MS and recalled_text:
                snippets.append(f"相关跨端回忆：\n{recalled_text}")

        if not snippets:
            return {"action": "continue"}

        added = "<maigo-reply-context>\n" + "\n".join(snippets) + "\n</maigo-reply-context>"
        existing = str(kwargs.get("extra_prompt") or "").strip()
        combined = f"{existing}\n\n{added}" if existing else added
        return {"action": "continue", "modified_kwargs": {**kwargs, "extra_prompt": combined}}

    # ------------------------------------------------------------------
    # 工具与后台轮询
    # ------------------------------------------------------------------

    @Tool(
        "recall_life",
        description=(
            "深度翻查你在各个地方（历次直播、MC 存档进展、B 站见闻、QQ 群、跨端熟人）的原件细节、观点与待办约定。"
            "有人问起往期直播细节、游戏里造过什么或之前刷到过什么内容时优先调用；需要查超过 30 天的历史可传 days=0。"
        ),
        parameters=[
            ToolParameterInfo(
                name="query",
                param_type=ToolParamType.STRING,
                description="想要回想的关键词或主题（例如「晶体装配器」「下界 远古残骸」）",
                required=True,
            ),
            ToolParameterInfo(
                name="days",
                param_type=ToolParamType.INTEGER,
                description="往前检索几天，默认 30；传 0 表示不限天数翻查全部历史",
                required=False,
            ),
            ToolParameterInfo(
                name="limit",
                param_type=ToolParamType.INTEGER,
                description="最多返回几条结果，默认 5",
                required=False,
            ),
        ],
    )
    async def handle_recall_life(self, query: Any = "", days: Any = 30, limit: Any = 5, **kwargs: Any) -> Dict[str, Any]:
        """按当前对话会话可见性调用 MaiGO 检索跨端原件书库与历史详情。"""
        if not self.config.plugin.enabled or self._client is None:
            return {"success": False, "error": "MaiGO 跨端同步未启用"}
        cleaned_query = str(query or "").strip()
        if not cleaned_query:
            return {"success": False, "error": "请提供想要检索的关键词"}
        try:
            raw_days = 30 if days is None else int(days)
            days_int = max(0, min(365, raw_days))
            limit_int = max(1, min(20, int(limit or 5)))
        except (TypeError, ValueError):
            return {"success": False, "error": "days 与 limit 必须是整数"}

        group_id = str(kwargs.get("group_id") or "").strip()
        platform = str(kwargs.get("platform") or "").strip()
        target_vis = "public"
        if self._tracker is not None:
            if group_id:
                target_vis = "group" if self._tracker.allows_group(group_id, platform) else "public"
            elif kwargs.get("is_private") is True and self.config.scope.sync_private:
                target_vis = "private"
            elif kwargs.get("stream_id"):
                resolved = await self._tracker.resolve_visibility(str(kwargs["stream_id"]))
                if resolved in ("group", "private"):
                    target_vis = resolved

        branch_id = (self.config.server.branch_id or "maibot").strip().lower()
        try:
            text = await self._client.recall_life(
                query=cleaned_query,
                days=days_int,
                limit=limit_int,
                target_visibility=target_vis,
                branch_id=branch_id,
            )
            return {"success": True, "content": text}
        except MaiGOClientError as exc:
            self.ctx.logger.warning(f"recall_life 工具查询 MaiGO 失败: {exc}")
            return {"success": False, "error": f"暂时连不上 MaiGO：{exc}"}

    async def refresh_once(self) -> None:
        """执行一轮慢层与在播状态刷新，并检查是否满足跨端开播喊人条件。"""
        if not self.config.plugin.enabled or self._client is None or self._cache is None:
            return
        if not self.config.server.url.strip():
            return
        branch_id = (self.config.server.branch_id or "maibot").strip().lower()
        vis_list = ["group"]
        if self.config.scope.sync_private:
            vis_list.append("private")
        merged_sessions: Dict[str, Dict[str, Any]] = {}
        any_succeeded = False
        for vis in vis_list:
            prev = self._cache.view_of(vis)
            try:
                raw = await self._client.get_context(
                    branch_id=branch_id,
                    target_visibility=vis,
                    if_brief_etag=prev.brief_etag,
                    include_topics=self.config.injection.include_topics,
                )
                self._cache.apply_context_response(vis, raw, update_active_sessions=False)
                any_succeeded = True
                for item in raw.get("active_sessions") or ():
                    if isinstance(item, dict) and item.get("checkout_id"):
                        merged_sessions[str(item["checkout_id"])] = dict(item)
            except MaiGOClientError as exc:
                self.ctx.logger.warning(f"后台刷新 MaiGO {vis} 上下文缓存失败（保留本地缓存）: {exc}")
        if any_succeeded:
            self._cache.set_active_sessions(list(merged_sessions.values()))
            self._maybe_start_backfill()
        if self._go_live is not None:
            await self._go_live.observe_and_tick(self._cache.active_sessions)

    async def run_history_backfill_once(self) -> int:
        """执行一轮历史对话回填（供测试确定性调用与人工触发）。"""
        if not self.config.plugin.enabled or not self.config.backfill.enabled or self._backfiller is None:
            return 0
        return await self._backfiller.run_once()

    def _maybe_start_backfill(self) -> None:
        if not self.config.backfill.enabled or self._backfiller is None:
            return
        if self._backfill_task is not None and not self._backfill_task.done():
            return
        if not self._backfiller.has_pending_scopes():
            return
        self._backfill_task = asyncio.create_task(self._backfiller.run_once(), name="maigo_adapter.backfill")

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self.refresh_once()
            except Exception as exc:
                self.ctx.logger.error(f"MaiGO 插件后台轮询异常: {exc}", exc_info=True)
            await asyncio.sleep(max(1.0, self.config.server.poll_interval_ms / 1000))


def create_plugin() -> MaiGOAdapterPlugin:
    """MaiBot 插件加载入口工厂。"""
    return MaiGOAdapterPlugin()
