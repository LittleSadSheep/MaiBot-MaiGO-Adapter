"""会话作用域追踪、慢快双层持久化缓存与 MaiBot Context Item 注入助手。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

_CONTEXT_STABLE_ITEM_TIMESTAMP = "2026-01-01T00:00:00"
_MAX_RECENT_ACTORS_PER_SESSION = 8
_RESOLVE_RETRY_MS = 60_000
_FORGET_KEYWORDS = ("忘了我", "忘掉我")


def _default_now_ms() -> int:
    return int(time.time() * 1000)


def extract_plain_text(message: Dict[str, Any]) -> str:
    """从序列化的 SessionMessage 字典里提取纯文本正文。"""
    processed = message.get("processed_plain_text")
    if isinstance(processed, str) and processed.strip():
        return processed.strip()
    raw = message.get("raw_message")
    if isinstance(raw, list):
        parts: List[str] = []
        for comp in raw:
            if isinstance(comp, dict) and comp.get("type") in ("text", "plain"):
                val = str(comp.get("data") or comp.get("text") or "").strip()
                if val:
                    parts.append(val)
        return " ".join(parts).strip()
    if isinstance(raw, dict):
        components = raw.get("components")
        if isinstance(components, list):
            parts = []
            for comp in components:
                if isinstance(comp, dict) and str(comp.get("type") or "") in ("text", "plain", "TextComponent"):
                    val = str(comp.get("text") or comp.get("data") or "").strip()
                    if val:
                        parts.append(val)
            return " ".join(parts).strip()
    return ""


@dataclass(frozen=True)
class TrackedActor:
    """会话中近期活跃的外部对象快照。"""

    platform: str
    external_id: str
    display_name: str
    last_seen_ms: int
    last_text: str = ""
    forgotten: bool = False


class SessionTracker:
    """维护 QQ 会话到目标可见性（白名单群=group / 私聊=private）的映射与近期发言人。

    触发条件：入站 Hook 记录消息元信息，规划/发送/检索钩子按 ``session_id`` 查询可见性；
    执行顺序：先查已缓存映射与消息附带的 ``group_info`` -> 未缓存群则带冷却地通过 SDK
    ``chat.get_stream_by_group_id`` 解析白名单群对应的 ``stream_id``。
    """

    def __init__(
        self,
        ctx: Any,
        *,
        platform: str,
        group_ids: Sequence[str],
        sync_private: bool = True,
        logger: Optional[logging.Logger] = None,
        clock_ms: Callable[[], int] = _default_now_ms,
        retry_ms: int = _RESOLVE_RETRY_MS,
    ) -> None:
        self._ctx = ctx
        self._logger = logger or logging.getLogger("plugin.maigo_adapter")
        self._clock_ms = clock_ms
        self._retry_ms = retry_ms
        self._platform = "qq"
        self._group_ids: Tuple[str, ...] = ()
        self._sync_private = bool(sync_private)
        self._whitelist_streams: Dict[str, str] = {}
        self._group_names: Dict[str, str] = {}
        self._last_try_ms: Dict[str, int] = {}
        self._session_kinds: Dict[str, str] = {}
        self._session_groups: Dict[str, str] = {}
        self._recent_actors: Dict[str, List[TrackedActor]] = {}
        self._last_inbound_summary: Dict[str, str] = {}
        self._last_inbound_raw: Dict[str, str] = {}
        self.update_settings(platform=platform, group_ids=group_ids, sync_private=sync_private)

    def update_settings(self, *, platform: str, group_ids: Sequence[str], sync_private: bool) -> None:
        """配置热更新时重置白名单会话缓存，确保变更立即生效。"""
        self._platform = (platform or "qq").strip() or "qq"
        self._group_ids = tuple(dict.fromkeys(item.strip() for item in group_ids if item.strip()))
        self._sync_private = bool(sync_private)
        self._whitelist_streams.clear()
        self._group_names.clear()
        self._last_try_ms.clear()

    def record_inbound(self, message: Dict[str, Any]) -> None:
        """记录入站会话类别（群/私聊）及近期发言人，供出站上下文与跨端名片查询使用。"""
        session_id = str(message.get("session_id") or "").strip()
        if not session_id:
            return
        msg_info = message.get("message_info")
        if not isinstance(msg_info, dict):
            return
        platform = str(message.get("platform") or self._platform).strip().lower() or self._platform
        group_info = msg_info.get("group_info")
        if isinstance(group_info, dict) and group_info.get("group_id"):
            gid = str(group_info["group_id"]).strip()
            self._session_kinds[session_id] = "group"
            self._session_groups[session_id] = gid
            if gid in self._group_ids:
                self._whitelist_streams[session_id] = gid
                gname = str(group_info.get("group_name") or "").strip()
                if gname:
                    self._group_names[gid] = gname
        else:
            self._session_kinds[session_id] = "private"

        user_info = msg_info.get("user_info")
        if not isinstance(user_info, dict):
            return
        uid = str(user_info.get("user_id") or "").strip()
        if not uid:
            return
        display_name = str(user_info.get("user_cardname") or user_info.get("user_nickname") or "").strip()
        text = extract_plain_text(message)
        forgotten = any(kw in text for kw in _FORGET_KEYWORDS)
        now = self._clock_ms()
        if text and not forgotten:
            self._last_inbound_summary[session_id] = f"{display_name or '群友'}说「{text[:80]}」"
            self._last_inbound_raw[session_id] = text[:160]
        elif forgotten:
            self._last_inbound_raw.pop(session_id, None)
        bucket = self._recent_actors.setdefault(session_id, [])
        bucket = [a for a in bucket if not (a.platform == platform and a.external_id == uid)]
        bucket.insert(
            0,
            TrackedActor(
                platform=platform,
                external_id=uid,
                display_name=display_name,
                last_seen_ms=now,
                last_text=text[:120],
                forgotten=forgotten,
            ),
        )
        self._recent_actors[session_id] = bucket[:_MAX_RECENT_ACTORS_PER_SESSION]

    async def resolve_visibility(self, session_id: str) -> Optional[str]:
        """根据会话 ID 裁决跨端可见性档位（group / private / None 不互通）。"""
        sid = str(session_id or "").strip()
        if not sid:
            return None
        if sid in self._whitelist_streams:
            return "group"
        kind = self._session_kinds.get(sid)
        if kind == "group":
            gid = self._session_groups.get(sid, "")
            return "group" if gid in self._group_ids else None
        if kind == "private":
            return "private" if self._sync_private else None
        await self._resolve_missing_groups()
        if sid in self._whitelist_streams:
            return "group"
        return None

    def allows_group(self, group_id: str, platform: str = "") -> bool:
        """按群号校验是否属于白名单互通群。"""
        if platform and platform.strip().lower() != self._platform.lower():
            return False
        return str(group_id or "").strip() in self._group_ids

    def group_label(self, session_id: str) -> str:
        """返回白名单群的人类可读名称（若未知则回落群号）。"""
        gid = self._whitelist_streams.get(session_id) or self._session_groups.get(session_id, "")
        return self._group_names.get(gid) or gid or session_id

    def recent_actors_param(self, session_id: str, *, limit: int = 4) -> str:
        """构造传给 ``GET /api/v1/views/context`` 的 ``active_actors`` 查询参数。"""
        actors = [a for a in self._recent_actors.get(session_id, ()) if not a.forgotten][:limit]
        return ",".join(f"{a.platform}:{a.external_id}" for a in actors)

    def recent_outbound_actors(self, session_id: str, *, limit: int = 3) -> List[Dict[str, str]]:
        """构造出站事件信封携带的近期非遗忘交互对象列表。"""
        actors = [a for a in self._recent_actors.get(session_id, ()) if not a.forgotten][:limit]
        return [
            {
                "platform": a.platform,
                "external_id": a.external_id,
                "display_name": a.display_name,
            }
            for a in actors
        ]

    def last_inbound_hint(self, session_id: str) -> str:
        """返回该会话最近一句有效入站摘要（用于出站事件上下文概括）。"""
        return self._last_inbound_summary.get(session_id, "")

    def last_inbound_query(self, session_id: str) -> str:
        """返回该会话最近一句未遗忘入站纯文本（用于规划前置高置信度跨端预召回）。"""
        return self._last_inbound_raw.get(session_id, "")

    async def _resolve_missing_groups(self) -> None:
        now = self._clock_ms()
        known_groups = set(self._whitelist_streams.values())
        for gid in self._group_ids:
            if gid in known_groups or now - self._last_try_ms.get(gid, -self._retry_ms) < self._retry_ms:
                continue
            self._last_try_ms[gid] = now
            try:
                raw = await self._ctx.chat.get_stream_by_group_id(gid, platform=self._platform)
            except Exception as exc:
                self._logger.warning(f"查询白名单群 {gid} 会话 ID 失败（稍后重试）: {exc}")
                continue
            stream = raw.get("stream") if isinstance(raw, dict) and isinstance(raw.get("stream"), dict) else raw
            if isinstance(stream, dict) and stream.get("stream_id"):
                sid = str(stream["stream_id"])
                self._whitelist_streams[sid] = gid
                self._session_kinds[sid] = "group"
                self._session_groups[sid] = gid
                if stream.get("group_name"):
                    self._group_names[gid] = str(stream["group_name"])


@dataclass
class CachedVisibilityView:
    """按可见性分桶存盘的 MaiGO 双层视图快照。"""

    brief_etag: str = ""
    stable_brief: str = ""
    live_context: str = ""
    open_promises: List[Dict[str, Any]] = None  # type: ignore[assignment]
    fresh_topics: List[Dict[str, Any]] = None  # type: ignore[assignment]
    save_brief: str = ""
    updated_at_ms: int = 0

    def __post_init__(self) -> None:
        if self.open_promises is None:
            self.open_promises = []
        if self.fresh_topics is None:
            self.fresh_topics = []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "brief_etag": self.brief_etag,
            "stable_brief": self.stable_brief,
            "live_context": self.live_context,
            "open_promises": list(self.open_promises),
            "fresh_topics": list(self.fresh_topics),
            "save_brief": self.save_brief,
            "updated_at_ms": self.updated_at_ms,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CachedVisibilityView":
        return cls(
            brief_etag=str(data.get("brief_etag") or ""),
            stable_brief=str(data.get("stable_brief") or ""),
            live_context=str(data.get("live_context") or ""),
            open_promises=list(data.get("open_promises") or []),
            fresh_topics=list(data.get("fresh_topics") or []),
            save_brief=str(data.get("save_brief") or ""),
            updated_at_ms=int(data.get("updated_at_ms") or 0),
        )


class ContextCacheStore:
    """慢层简报与跨端活跃状态的本地持久化缓存（支持离线降级与 ETag 命中）。"""

    def __init__(
        self,
        cache_path: Optional[Path],
        *,
        logger: Optional[logging.Logger] = None,
        clock_ms: Callable[[], int] = _default_now_ms,
    ) -> None:
        self._path = cache_path
        self._logger = logger or logging.getLogger("plugin.maigo_adapter")
        self._clock_ms = clock_ms
        self._views: Dict[str, CachedVisibilityView] = {}
        self.active_sessions: List[Dict[str, Any]] = []

    def view_of(self, visibility: str) -> CachedVisibilityView:
        """返回指定可见性分桶的只读或当前缓存视图（默认空视图）。"""
        return self._views.get(visibility, CachedVisibilityView())

    def apply_context_response(
        self,
        visibility: str,
        payload: Dict[str, Any],
        *,
        update_active_sessions: bool = True,
    ) -> CachedVisibilityView:
        """按 ``if_brief_etag`` 协议合并服务端响应并落盘，保持未更新慢层字节不动。"""
        prev = self._views.get(visibility, CachedVisibilityView())
        new_etag = str(payload.get("brief_etag") or prev.brief_etag)
        unchanged = bool(payload.get("stable_unchanged", False)) and payload.get("stable_brief") is None
        stable = prev.stable_brief if unchanged else str(payload.get("stable_brief") or "")
        view = CachedVisibilityView(
            brief_etag=new_etag,
            stable_brief=stable,
            live_context=str(payload.get("live_context") or ""),
            open_promises=[dict(item) for item in (payload.get("open_promises") or ()) if isinstance(item, dict)],
            fresh_topics=[dict(item) for item in (payload.get("fresh_topics") or ()) if isinstance(item, dict)],
            save_brief=str(payload.get("save_brief") or ""),
            updated_at_ms=self._clock_ms(),
        )
        self._views[visibility] = view
        if update_active_sessions and "active_sessions" in payload and isinstance(payload.get("active_sessions"), list):
            self.active_sessions = [dict(item) for item in payload["active_sessions"] if isinstance(item, dict)]
        self.save()
        return view

    def set_active_sessions(self, sessions: Sequence[Dict[str, Any]]) -> None:
        """更新本轮多视图汇总后的活跃会话列表并落盘。"""
        self.active_sessions = [dict(item) for item in sessions if isinstance(item, dict)]
        self.save()

    def load(self) -> None:
        """从本地缓存文件加载慢层快照；损坏时仅告警并从空缓存起步。"""
        if self._path is None or not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception as exc:
            self._logger.error(f"读取 MaiGO 本地缓存失败（{self._path}），从空状态起步: {exc}")
            self._views = {}
            self.active_sessions = []
            return
        views_raw = raw.get("views") if isinstance(raw, dict) else {}
        if isinstance(views_raw, dict):
            self._views = {
                str(vis): CachedVisibilityView.from_dict(val) for vis, val in views_raw.items() if isinstance(val, dict)
            }
        sessions_raw = raw.get("active_sessions") if isinstance(raw, dict) else []
        if isinstance(sessions_raw, list):
            self.active_sessions = [dict(s) for s in sessions_raw if isinstance(s, dict)]

    def save(self) -> None:
        """原子写盘持久化缓存。"""
        if self._path is None:
            return
        payload = {
            "views": {vis: view.to_dict() for vis, view in self._views.items()},
            "active_sessions": list(self.active_sessions),
        }
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, self._path)
        except OSError as exc:
            self._logger.error(f"写入 MaiGO 本地缓存失败（{self._path}）: {exc}")


def build_context_item(text: str, *, item_id: str, stable: bool) -> Dict[str, Any]:
    """构造 MaiBot Schema v1 用户消息注入条目；慢层固定时间戳保前缀缓存。"""
    ts = _CONTEXT_STABLE_ITEM_TIMESTAMP if stable else datetime.now(tz=timezone.utc).replace(tzinfo=None).isoformat()
    return {
        "item_type": "UserMessageItem",
        "meta": {
            "item_id": item_id,
            "logical_turn_id": None,
            "timestamp": ts,
        },
        "parts": [{"type": "text", "text": text}],
    }


def insert_stable_brief_item(items: List[Any], text: str, *, item_id: str = "maigo-stable-brief") -> List[Any]:
    """将慢层今日生活与近期立场插在 SystemMessageItem 之后、对话历史之前以最大化前缀缓存命中。"""
    if not text.strip():
        return list(items)
    new_items = list(items)
    pos = 0
    while pos < len(new_items) and isinstance(new_items[pos], dict):
        if new_items[pos].get("item_type") != "SystemMessageItem":
            break
        pos += 1
    wrapped = (
        "<maigo-life-brief>\n"
        "以下是关于你自己的跨端生活背景与长期记忆（今天经历与近期表态）：\n"
        f"{text.strip()}\n\n"
        "【记忆检索分工】\n"
        "- 本地 query_memory 仅覆盖当前群聊记录，不包含你的直播、MC 存档或 B 站经历；\n"
        "- 凡涉及往期直播细节、游戏存档进展、B 站见闻或上方未展开的历史事项，"
        "请直接调用已在可用工具列表中的 recall_life 工具（查全部历史传 days=0，无需再用 tool_search 搜索它）查证后再答，切勿靠猜。\n"
        "</maigo-life-brief>"
    )
    new_items.insert(pos, build_context_item(wrapped, item_id=item_id, stable=True))
    return new_items


def insert_dynamic_context_item(items: List[Any], text: str, *, item_id: str = "maigo-dynamic-context") -> List[Any]:
    """将快层实时态势、熟人名片与鲜活话头插在请求末尾指令项之前。"""
    if not text.strip():
        return list(items)
    new_items = list(items)
    pos = max(len(new_items) - 1, 0) if new_items else 0
    new_items.insert(pos, build_context_item(text.strip(), item_id=item_id, stable=False))
    return new_items


def format_dynamic_block(
    *,
    live_context: str = "",
    actor_cards: Sequence[Dict[str, Any]] = (),
    fresh_topics: Sequence[Dict[str, Any]] = (),
    open_promises: Sequence[Dict[str, Any]] = (),
    pre_recalled: str = "",
) -> str:
    """将快层动态信息与高置信度预召回细节格式化为防串话、防当众开盒的参考块。"""
    sections: List[str] = []
    if live_context.strip():
        sections.append(f"当前动态与刚发生的跨端事件：\n{live_context.strip()}")
    if pre_recalled.strip():
        sections.append(
            "【顺手想起来的跨端细节】（已核实历史记录，答复时可直接自然引用，勿与上面内容矛盾）：\n"
            f"{pre_recalled.strip()}"
        )
    if actor_cards:
        lines = ["在场可识别的跨端熟人（仅基于可公开关联，不要当众点破对方其他平台账号或未公开隐私）："]
        for card in actor_cards:
            hint = str(card.get("hint") or "").strip()
            name = str(card.get("display_name") or "熟人").strip()
            if hint:
                lines.append(f"- {name}：{hint}")
        if len(lines) > 1:
            sections.append("\n".join(lines))
    if fresh_topics:
        tlines = ["近期可顺手聊聊的新鲜见闻："]
        for topic in fresh_topics[:4]:
            title = str(topic.get("title") or "").strip()
            summary = str(topic.get("summary") or "").strip()
            if title:
                tlines.append(f"- {title}" + (f"（{summary}）" if summary else ""))
        if len(tlines) > 1:
            sections.append("\n".join(tlines))
    if open_promises:
        plines = ["还未兑现的跨端约定（没完成前别说成已做完）："]
        for prom in open_promises[:5]:
            content = str(prom.get("content") or "").strip()
            if content:
                plines.append(f"- [约#{prom.get('id')}] {content}")
        if len(plines) > 1:
            sections.append("\n".join(plines))
    if not sections:
        return ""
    body = "\n\n".join(sections)
    return (
        "<maigo-context>\n"
        "关于你自己的跨端事实（被问到最近在干嘛、直播近况或对应经历时照此如实衔接，勿另编细节）：\n"
        f"{body}\n"
        "</maigo-context>"
    )
