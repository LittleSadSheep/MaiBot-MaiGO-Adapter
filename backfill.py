"""MaiBot-MaiGO-Adapter 首次连通后台低速历史回填器。"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from .maigo_client import MaiGOClient, MaiGOClientError
from .speech_buffer import is_low_signal_outbound

_SPACE_RE = re.compile(r"\s+")


def _clean_text(raw: Any, *, max_len: int = 1200) -> str:
    compact = _SPACE_RE.sub(" ", str(raw or "")).strip()
    return compact if len(compact) <= max_len else compact[: max_len - 1].rstrip() + "…"


def _to_ms(raw_ts: Any, fallback_ms: int) -> int:
    if raw_ts is None:
        return fallback_ms
    try:
        val = float(raw_ts)
    except (TypeError, ValueError):
        return fallback_ms
    if val <= 0:
        return fallback_ms
    return int(val * 1000) if val < 1e11 else int(val)


def _table_columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    return {str(r["name"]) for r in conn.execute(f"PRAGMA table_info([{table}])").fetchall()}


def _row_val(row: sqlite3.Row, cols: Set[str], key: str, default: Any = None) -> Any:
    return row[key] if key in cols else default


class HistoryBackfiller:
    """将 MaiBot 本地历史库中的高信噪比本人回复低速回填到 MaiGO。"""

    def __init__(
        self,
        *,
        data_dir: Path,
        client: MaiGOClient,
        logger: Any,
        branch_id: str = "maibot",
        platform: str = "qq",
        group_ids: Sequence[str] = (),
        sync_private: bool = True,
        days: int = 30,
        min_chars: int = 6,
        batch_size: int = 20,
        batch_interval_ms: int = 1_000,
        db_path: str = "",
        clock_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    ) -> None:
        self._data_dir = Path(data_dir)
        self._state_path = self._data_dir / "maigo_backfill_state.json"
        self._client = client
        self._logger = logger
        self._branch_id = (branch_id or "maibot").strip().lower() or "maibot"
        self._platform = (platform or "qq").strip().lower() or "qq"
        self._group_ids = [str(g).strip() for g in group_ids if str(g).strip()]
        self._sync_private = bool(sync_private)
        self._days = max(0, int(days))
        self._min_chars = max(1, int(min_chars))
        self._batch_size = max(1, int(batch_size))
        self._batch_interval_s = max(0.0, int(batch_interval_ms) / 1000.0)
        self._db_path = db_path.strip()
        self._clock_ms = clock_ms
        self._run_lock = asyncio.Lock()

    def update_settings(
        self,
        *,
        client: Optional[MaiGOClient] = None,
        branch_id: str,
        platform: str,
        group_ids: Sequence[str],
        sync_private: bool,
        days: int,
        min_chars: int,
        batch_size: int,
        batch_interval_ms: int,
        db_path: str,
    ) -> None:
        if client is not None:
            self._client = client
        self._branch_id = (branch_id or "maibot").strip().lower() or "maibot"
        self._platform = (platform or "qq").strip().lower() or "qq"
        self._group_ids = [str(g).strip() for g in group_ids if str(g).strip()]
        self._sync_private = bool(sync_private)
        self._days = max(0, int(days))
        self._min_chars = max(1, int(min_chars))
        self._batch_size = max(1, int(batch_size))
        self._batch_interval_s = max(0.0, int(batch_interval_ms) / 1000.0)
        self._db_path = db_path.strip()

    def _target_scopes(self) -> List[str]:
        scopes = [f"group:{gid}" for gid in dict.fromkeys(self._group_ids)]
        if self._sync_private:
            scopes.append("private")
        return scopes

    def _read_completed_scopes(self) -> Set[str]:
        if not self._state_path.is_file():
            return set()
        try:
            data = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return set()
        if not isinstance(data, dict):
            return set()
        return {str(s).strip() for s in (data.get("completed_scopes") or ()) if str(s).strip()}

    def _write_completed_scopes(self, new_scopes: Sequence[str]) -> None:
        completed = self._read_completed_scopes()
        completed.update(str(s).strip() for s in new_scopes if str(s).strip())
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "completed_scopes": sorted(completed),
            "updated_at_ms": int(self._clock_ms()),
        }
        tmp = self._state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self._state_path)

    def has_pending_scopes(self) -> bool:
        """判断是否仍有未完成过历史回填的目标群聊或私聊作用域。"""
        completed = self._read_completed_scopes()
        return any(scope not in completed for scope in self._target_scopes())

    def _locate_db_path(self) -> Optional[Path]:
        if self._db_path:
            explicit = Path(self._db_path)
            return explicit if explicit.is_file() else None
        base = self._data_dir.resolve()
        for ancestor in (base, *base.parents[:4]):
            for rel in ("data/MaiBot.db", "MaiBot.db"):
                cand = ancestor / rel
                if cand.is_file():
                    return cand
        return None

    def _collect_pending_sync(self) -> Tuple[List[Dict[str, Any]], List[str]]:
        completed = self._read_completed_scopes()
        pending_scopes = [s for s in self._target_scopes() if s not in completed]
        if not pending_scopes:
            return [], []

        db_file = self._locate_db_path()
        if db_file is None:
            return [], []

        pending_set = set(pending_scopes)
        now_ms = int(self._clock_ms())
        cutoff_ms = now_ms - self._days * 86_400_000 if self._days > 0 else 0

        conn = sqlite3.connect(f"file:{db_file.resolve().as_posix()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        events: List[Dict[str, Any]] = []
        try:
            tables = {str(r["name"]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            known_bots: Set[str] = set()
            if "bot_platform_accounts" in tables:
                for row in conn.execute("SELECT platform, account_id FROM bot_platform_accounts").fetchall():
                    if str(row["platform"] or "").strip().lower() == self._platform and row["account_id"]:
                        known_bots.add(str(row["account_id"]).strip())
            if not known_bots:
                return [], []

            msg_table = "mai_messages" if "mai_messages" in tables else ("messages" if "messages" in tables else "")
            if not msg_table:
                return [], []
            mcols = _table_columns(conn, msg_table)

            for row in conn.execute(f"SELECT * FROM [{msg_table}]").fetchall():
                if str(_row_val(row, mcols, "platform") or "").strip().lower() != self._platform:
                    continue
                if any(bool(_row_val(row, mcols, k, False)) for k in ("is_emoji", "is_picture", "is_command", "is_notify")):
                    continue
                uid = str(_row_val(row, mcols, "user_id") or "").strip()
                if uid not in known_bots:
                    continue

                gid = str(_row_val(row, mcols, "group_id") or "").strip()
                raw_scope = str(_row_val(row, mcols, "scope") or "").strip().lower()
                if gid and raw_scope != "private":
                    scope_key = f"group:{gid}"
                    vis = "group"
                    gname = _clean_text(_row_val(row, mcols, "group_name"), max_len=36)
                    scope_label = f"群「{gname}」" if gname else "互通群聊"
                else:
                    scope_key = "private"
                    vis = "private"
                    scope_label = "私聊"

                if scope_key not in pending_set:
                    continue

                text = _clean_text(
                    _row_val(row, mcols, "processed_plain_text") or _row_val(row, mcols, "raw_content"), max_len=1200
                )
                if len(text) < self._min_chars or is_low_signal_outbound(text, min_chars=self._min_chars):
                    continue
                occ_ms = _to_ms(_row_val(row, mcols, "timestamp") or _row_val(row, mcols, "received_at"), now_ms)
                if cutoff_ms > 0 and occ_ms < cutoff_ms:
                    continue

                msg_key = str(_row_val(row, mcols, "message_id") or _row_val(row, mcols, "id") or occ_ms).strip()
                sess_id = str(_row_val(row, mcols, "session_id") or gid or uid).strip()
                events.append(
                    {
                        "idempotency_key": f"backfill-maibot-msg-{msg_key}",
                        "branch_id": self._branch_id,
                        "occurred_at_ms": occ_ms,
                        "kind": "speech",
                        "actor_role": "self",
                        "visibility": vis,
                        "context_scope": f"{vis}:{sess_id}",
                        "actors": [],
                        "summary": _clean_text(f"在{scope_label}说：{text}", max_len=240),
                        "verbatim": text,
                        "reaction": "",
                        "save_id": None,
                        "metadata": {"backfill": True},
                    }
                )
        finally:
            conn.close()

        events.sort(key=lambda ev: int(ev["occurred_at_ms"]))
        return events, pending_scopes

    async def run_once(self) -> int:
        """执行一轮增量作用域历史回填；网络异常时保留未完成作用域以便下轮重试。"""
        async with self._run_lock:
            events, target_scopes = await asyncio.to_thread(self._collect_pending_sync)
            if not target_scopes:
                return 0

            sent = 0
            for idx in range(0, len(events), self._batch_size):
                chunk = events[idx : idx + self._batch_size]
                try:
                    await self._client.post_events(chunk)
                    sent += len(chunk)
                except MaiGOClientError as exc:
                    self._logger.warning(f"MaiGO 历史回填暂停（已送 {sent}/{len(events)} 条，留待后续重试）: {exc}")
                    return sent
                if idx + self._batch_size < len(events) and self._batch_interval_s > 0:
                    await asyncio.sleep(self._batch_interval_s)

            await asyncio.to_thread(self._write_completed_scopes, target_scopes)
            if sent > 0:
                self._logger.info(f"MaiGO 历史对话回填完成: 补推 {sent} 条本人回复（覆盖 {len(target_scopes)} 个作用域）")
            return sent
