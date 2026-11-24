"""跨端开播提示：基于 MaiSwitch 在播状态，延迟确认正式场稳定后唤醒 QQ 麦麦进群喊人。"""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

_KEEP_ANNOUNCED = 50
_PLATFORM_NAMES = {"bilibili": "B站", "douyin": "抖音", "youtube": "YouTube", "twitch": "Twitch"}


def _default_now_ms() -> int:
    return int(time.time() * 1000)


def build_room_url(platform: str, room_id: str, override: str = "") -> str:
    """构造直播间访问地址：优先采用人工覆盖配置，否则按 B 站规则拼接。"""
    if override.strip():
        return override.strip()
    if platform.strip().lower() == "bilibili" and room_id.strip():
        return f"https://live.bilibili.com/{room_id.strip()}"
    return ""


def _platform_label(platform: str) -> str:
    return _PLATFORM_NAMES.get(platform.strip().lower(), platform.strip())


@dataclass(frozen=True)
class GoLiveSettings:
    """开播延迟喊人运行参数。"""

    group_ids: Tuple[str, ...] = ()
    platform: str = "qq"
    self_branch_id: str = "maibot"
    delay_ms: int = 300_000
    stale_after_ms: int = 180_000
    room_url: str = ""


class GoLiveAnnouncer:
    """跟踪来自其他分支（如 Amaidesu）的在播场次，满足稳定时长后托 QQ 麦麦喊人。

    触发条件：后台定时轮询收到 MaiSwitch ``active_sessions`` 更新时 ``observe_and_tick``；
    执行顺序：过滤本分支场次 -> 按 ``checkout_id`` 检查历史已喊落盘 -> 核对心跳未过期且
    已稳定超过 ``delay_ms`` -> 先记入落盘集合防重发 -> 逐群调用 ``maisaka.proactive.trigger``。
    """

    def __init__(
        self,
        ctx: Any,
        settings: GoLiveSettings,
        *,
        state_path: Optional[Path],
        logger: Optional[logging.Logger] = None,
        clock_ms: Callable[[], int] = _default_now_ms,
    ) -> None:
        self._ctx = ctx
        self._settings = settings
        self._state_path = state_path
        self._logger = logger or logging.getLogger("plugin.maiswitch_adapter")
        self._clock_ms = clock_ms
        self._announced: List[str] = []

    def update_settings(self, settings: GoLiveSettings) -> None:
        self._settings = settings

    def load(self) -> None:
        """从插件数据目录读回历史已喊场次编号，避免进程重启后同场重复喊人。"""
        if self._state_path is None or not self._state_path.exists():
            return
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
            self._announced = [str(item) for item in (raw.get("announced") or [])]
        except Exception as exc:
            self._logger.error(f"读取开播喊人记录失败（{self._state_path}），从空记录开始: {exc}")
            self._announced = []

    async def observe_and_tick(self, active_sessions: Sequence[Dict[str, Any]]) -> List[str]:
        """检查在播列表并对达标且未喊过的外部场次触发延迟开播通知，返回本次触发的场次标识。"""
        settings = self._settings
        if not settings.group_ids:
            return []
        now = self._clock_ms()
        triggered: List[str] = []
        for sess in active_sessions:
            if not isinstance(sess, dict):
                continue
            branch = str(sess.get("branch_id") or "").strip().lower()
            if not branch or branch == settings.self_branch_id:
                continue
            checkout_id = str(sess.get("checkout_id") or "").strip()
            if not checkout_id or checkout_id in self._announced:
                continue
            started_ms = int(sess.get("started_at_ms") or 0)
            heartbeat_ms = int(sess.get("last_heartbeat_ms") or started_ms)
            if started_ms <= 0:
                continue
            if now - heartbeat_ms > settings.stale_after_ms:
                continue
            elapsed_ms = now - started_ms
            if elapsed_ms < settings.delay_ms:
                continue

            # 先落盘防重入，避免发通知过程中失败重入刷屏
            self._remember(checkout_id)
            prompt = self._build_request_text(sess, elapsed_ms)
            for gid in settings.group_ids:
                try:
                    await self._notify_group(gid, prompt, checkout_id=checkout_id)
                except Exception as exc:
                    self._logger.error(f"跨端开播喊人在群 {gid} 执行异常: {exc}", exc_info=True)
            triggered.append(checkout_id)
        return triggered

    def _build_request_text(self, sess: Dict[str, Any], elapsed_ms: int) -> str:
        title = str(sess.get("title") or "").strip()
        title_part = f"《{title}》" if title else ""
        plat = _platform_label(str(sess.get("platform") or ""))
        where = f"在{plat}" if plat else ""
        url = build_room_url(str(sess.get("platform") or ""), str(sess.get("room_id") or ""), self._settings.room_url)
        link = f"，直播间 {url}" if url else ""
        minutes = max(1, round(elapsed_ms / 60_000))
        return (
            f"直播那边的你{where}开播了{title_part}{link}，已经稳定播了约 {minutes} 分钟。"
            "用你自己的语气在群里喊大家来看直播。\n这件事请一定在群里说到。"
        )

    async def _notify_group(self, group_id: str, request: str, *, checkout_id: str) -> None:
        raw = await self._ctx.chat.get_stream_by_group_id(group_id, platform=self._settings.platform)
        stream = raw.get("stream") if isinstance(raw, dict) and isinstance(raw.get("stream"), dict) else raw
        if not isinstance(stream, dict) or not stream.get("stream_id"):
            self._logger.error(f"跨端开播喊人找不到群 {group_id} 的会话信息: {raw}")
            return
        res = await self._ctx.maisaka.proactive.trigger(
            str(stream["stream_id"]),
            request,
            reason="maiswitch_go_live",
            priority="高",
            metadata={"source": "maiswitch", "checkout_id": checkout_id},
        )
        if isinstance(res, dict) and res.get("success") is True:
            self._logger.info(f"已触发 QQ 麦麦到群 {group_id} 喊人看直播（checkout={checkout_id}）")
        else:
            self._logger.warning(f"跨端开播喊人未被宿主受理（群 {group_id}）: {res}")

    def _remember(self, checkout_id: str) -> None:
        self._announced.append(checkout_id)
        del self._announced[:-_KEEP_ANNOUNCED]
        if self._state_path is None:
            return
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"announced": self._announced}, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, self._state_path)
        except OSError as exc:
            self._logger.error(f"保存开播喊人记录失败（{self._state_path}）: {exc}")
