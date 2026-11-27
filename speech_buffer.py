"""出站发言会话窗口归并缓冲器：过滤指令回执与无意义水词，将连续多轮回复合并上报。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import re
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

_LOW_SIGNAL_SYSTEM_PHRASES: Tuple[str, ...] = (
    "你没有权限使用此命令",
    "我暂时失忆了",
    "已清除上下文",
    "上下文已清空",
    "权限不足",
    "指令格式错误",
    "未知命令",
)
_TRIVIAL_FILLERS: frozenset[str] = frozenset(
    {
        "好的",
        "好呀",
        "好哒",
        "好哦",
        "嗯嗯",
        "嗯呐",
        "咋了",
        "怎么了",
        "能啊",
        "行吧",
        "行呀",
        "哈哈",
        "哈哈哈",
        "哈哈哈哈",
        "是的",
        "对呀",
        "对的",
        "收到",
        "明白",
        "没事",
        "ok",
        "okay",
        "yes",
        "no",
    }
)
_PUNCT_AND_SPACE_RE = re.compile(r"[\s\W_]+", re.UNICODE)


def _default_now_ms() -> int:
    return int(time.time() * 1000)


def is_low_signal_outbound(
    text: str,
    *,
    is_command: bool = False,
    is_notify: bool = False,
    min_chars: int = 4,
) -> bool:
    """判断出站回复是否属于命令回执、系统提示或过短水词（此类内容不应进入跨端时间线）。"""
    if is_command or is_notify:
        return True
    cleaned = " ".join((text or "").split()).strip()
    if not cleaned:
        return True
    if cleaned.startswith(("/", "[系统]", "【系统】", "⚠️", "❌")):
        return True
    if any(phrase in cleaned for phrase in _LOW_SIGNAL_SYSTEM_PHRASES):
        return True
    core = _PUNCT_AND_SPACE_RE.sub("", cleaned).lower()
    if len(core) < max(1, int(min_chars)):
        return True
    return core in _TRIVIAL_FILLERS


def is_pre_recall_candidate_query(text: str) -> bool:
    """判断入站消息是否值得触发自动跨端预召回（过滤指令与极短闲聊水词）。"""
    cleaned = " ".join((text or "").split()).strip()
    if not cleaned or cleaned.startswith(("/", "!", "！")):
        return False
    core = _PUNCT_AND_SPACE_RE.sub("", cleaned).lower()
    if len(core) < 4:
        return False
    return core not in _TRIVIAL_FILLERS


@dataclass
class _BufferedTurn:
    occurred_at_ms: int
    msg_id: str
    inbound_hint: str
    verbatim: str
    actors: List[Dict[str, str]]


@dataclass
class _SessionBucket:
    session_id: str
    visibility: str
    scope_label: str
    turns: List[_BufferedTurn] = field(default_factory=list)


class SpeechWindowBuffer:
    """按 ``(session_id, visibility)`` 聚合出站发言，达到空闲窗口或轮次上限时合并成高信噪比事件。

    触发条件：出站发送钩子投递经清洗后的本人回复，或在插件卸载/测试收口时主动刷出；
    执行顺序：先过滤低信噪比回执与水词 -> 按会话分桶暂存并刷新空闲定时器 -> 达轮次上限
    或空闲到期时合并成单条事件经 ``on_flush`` 上报给 MaiGO。
    """

    def __init__(
        self,
        *,
        on_flush: Callable[[List[Dict[str, Any]]], Awaitable[None]],
        branch_id: str = "maibot",
        enabled: bool = True,
        flush_idle_ms: int = 90_000,
        max_turns: int = 6,
        min_chars: int = 4,
        clock_ms: Callable[[], int] = _default_now_ms,
    ) -> None:
        self._on_flush = on_flush
        self._branch_id = (branch_id or "maibot").strip().lower() or "maibot"
        self._enabled = bool(enabled)
        self._flush_idle_ms = max(0, int(flush_idle_ms))
        self._max_turns = max(1, int(max_turns))
        self._min_chars = max(1, int(min_chars))
        self._clock_ms = clock_ms
        self._buckets: Dict[Tuple[str, str], _SessionBucket] = {}
        self._timers: Dict[Tuple[str, str], asyncio.Task[None]] = {}

    def update_settings(
        self,
        *,
        branch_id: str,
        enabled: bool,
        flush_idle_ms: int,
        max_turns: int,
        min_chars: int,
    ) -> None:
        """热更新窗口聚合参数。"""
        self._branch_id = (branch_id or "maibot").strip().lower() or "maibot"
        self._enabled = bool(enabled)
        self._flush_idle_ms = max(0, int(flush_idle_ms))
        self._max_turns = max(1, int(max_turns))
        self._min_chars = max(1, int(min_chars))

    def enqueue(
        self,
        *,
        session_id: str,
        visibility: str,
        scope_label: str,
        msg_id: str,
        verbatim: str,
        inbound_hint: str,
        actors: Sequence[Dict[str, str]],
        is_command: bool = False,
        is_notify: bool = False,
    ) -> Optional[List[Dict[str, Any]]]:
        """登记一轮出站发言。

        若命中低信噪比过滤返回 None；若未开启窗口缓冲或达到轮次上限，直接弹出待上报事件列表；
        否则留在缓冲区并刷新空闲定时器，返回 None 等待后续定时或显式刷出。
        """
        if is_low_signal_outbound(
            verbatim,
            is_command=is_command,
            is_notify=is_notify,
            min_chars=self._min_chars,
        ):
            return None

        key = (session_id, visibility)
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = _SessionBucket(session_id=session_id, visibility=visibility, scope_label=scope_label)
            self._buckets[key] = bucket
        bucket.turns.append(
            _BufferedTurn(
                occurred_at_ms=self._clock_ms(),
                msg_id=msg_id,
                inbound_hint=inbound_hint,
                verbatim=verbatim,
                actors=[dict(a) for a in actors],
            )
        )

        if not self._enabled or self._flush_idle_ms <= 0 or len(bucket.turns) >= self._max_turns:
            self._cancel_timer(key)
            popped = self._buckets.pop(key, None)
            return [self._build_event(popped)] if popped is not None and popped.turns else None

        self._reschedule_timer(key)
        return None

    def pop_all_events(self) -> List[Dict[str, Any]]:
        """立即取消全部空闲定时器并弹出所有待上报合并事件。"""
        for key in list(self._timers.keys()):
            self._cancel_timer(key)
        events: List[Dict[str, Any]] = []
        for key in list(self._buckets.keys()):
            bucket = self._buckets.pop(key, None)
            if bucket is not None and bucket.turns:
                events.append(self._build_event(bucket))
        return events

    async def flush_all(self) -> int:
        """立即将缓冲区内所有未到期对话窗口刷出并异步上报。"""
        events = self.pop_all_events()
        if not events:
            return 0
        await self._on_flush(events)
        return len(events)

    def _cancel_timer(self, key: Tuple[str, str]) -> None:
        task = self._timers.pop(key, None)
        if task is not None and not task.done():
            task.cancel()

    def _reschedule_timer(self, key: Tuple[str, str]) -> None:
        self._cancel_timer(key)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        delay_s = self._flush_idle_ms / 1000.0
        self._timers[key] = asyncio.create_task(
            self._idle_flush_task(key, delay_s),
            name=f"maigo_adapter.speech_window.{key[0]}",
        )

    async def _idle_flush_task(self, key: Tuple[str, str], delay_s: float) -> None:
        try:
            await asyncio.sleep(delay_s)
        except asyncio.CancelledError:
            return
        self._timers.pop(key, None)
        bucket = self._buckets.pop(key, None)
        if bucket is not None and bucket.turns:
            await self._on_flush([self._build_event(bucket)])

    def _build_event(self, bucket: _SessionBucket) -> Dict[str, Any]:
        turns = bucket.turns
        first = turns[0]
        last = turns[-1]
        merged_actors: List[Dict[str, str]] = []
        seen_actor_keys: set[Tuple[str, str]] = set()
        for turn in turns:
            for actor in turn.actors:
                akey = (str(actor.get("platform") or ""), str(actor.get("external_id") or ""))
                if akey[0] and akey[1] and akey not in seen_actor_keys:
                    seen_actor_keys.add(akey)
                    merged_actors.append(actor)

        if len(turns) == 1:
            if first.inbound_hint:
                summary = f"在{bucket.scope_label}回应（{first.inbound_hint}）：{first.verbatim[:160]}"
            else:
                summary = f"在{bucket.scope_label}说：{first.verbatim[:180]}"
            return {
                "idempotency_key": f"maibot-send-{first.msg_id}",
                "branch_id": self._branch_id,
                "occurred_at_ms": last.occurred_at_ms,
                "kind": "speech",
                "actor_role": "self",
                "visibility": bucket.visibility,
                "context_scope": f"{bucket.visibility}:{bucket.session_id}",
                "actors": merged_actors[:5],
                "summary": summary[:400],
                "verbatim": first.verbatim[:4000],
            }

        snippets: List[str] = []
        verbatim_lines: List[str] = []
        for turn in turns:
            snippets.append(turn.verbatim[:70])
            if turn.inbound_hint:
                verbatim_lines.append(f"[{turn.inbound_hint}] -> {turn.verbatim}")
            else:
                verbatim_lines.append(turn.verbatim)
        summary = f"在{bucket.scope_label}聊了一段（{len(turns)}轮）：" + " / ".join(snippets)
        return {
            "idempotency_key": f"maibot-win-{first.msg_id}-{last.msg_id}",
            "branch_id": self._branch_id,
            "occurred_at_ms": last.occurred_at_ms,
            "kind": "speech",
            "actor_role": "self",
            "visibility": bucket.visibility,
            "context_scope": f"{bucket.visibility}:{bucket.session_id}",
            "actors": merged_actors[:5],
            "summary": summary[:400],
            "verbatim": "\n".join(verbatim_lines)[:4000],
            "metadata": {"merged_window": True, "window_turns": len(turns)},
        }
