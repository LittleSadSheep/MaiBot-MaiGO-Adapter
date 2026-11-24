"""MaiBot-MaiGO-Adapter 测试用假 MaiBot 宿主。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple

from maibot_sdk import PluginContext, PluginPaths

PLUGIN_DIR = Path(__file__).resolve().parents[1]
PACKAGE_NAME = "maigo_adapter_under_test"

WHITELIST_GROUP_ID = "10001"
WHITELIST_STREAM_ID = "stream-group-10001"
WHITELIST_GROUP_NAME = "测试互通群"
NON_WHITELIST_GROUP_ID = "99999"
NON_WHITELIST_STREAM_ID = "stream-group-99999"
PRIVATE_STREAM_ID = "stream-private-10001"
NOTIFY_GROUP_ID = "88888"
NOTIFY_STREAM_ID = "stream-notify-88888"


def load_plugin_module() -> ModuleType:
    """按 MaiBot 插件包形式加载当前目录的 plugin.py，确保相对导入生效。"""
    if PACKAGE_NAME in sys.modules:
        return sys.modules[PACKAGE_NAME]
    spec = importlib.util.spec_from_file_location(
        PACKAGE_NAME,
        PLUGIN_DIR / "plugin.py",
        submodule_search_locations=[str(PLUGIN_DIR)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[PACKAGE_NAME] = module
    spec.loader.exec_module(module)
    return module


class FakeHost:
    """实现插件依赖的最小真实 SDK RPC 宿主桩。"""

    def __init__(self) -> None:
        self.calls: List[Tuple[str, Dict[str, Any]]] = []
        self.streams: Dict[str, Dict[str, Any]] = {
            WHITELIST_GROUP_ID: {
                "stream_id": WHITELIST_STREAM_ID,
                "group_id": WHITELIST_GROUP_ID,
                "group_name": WHITELIST_GROUP_NAME,
            },
            NON_WHITELIST_GROUP_ID: {
                "stream_id": NON_WHITELIST_STREAM_ID,
                "group_id": NON_WHITELIST_GROUP_ID,
                "group_name": "其他非互通群",
            },
            NOTIFY_GROUP_ID: {
                "stream_id": NOTIFY_STREAM_ID,
                "group_id": NOTIFY_GROUP_ID,
                "group_name": "开播通知群",
            },
        }
        self.trigger_calls: List[Dict[str, Any]] = []
        self.config: Dict[str, Any] = {"bot.nickname": "麦麦"}
        self.data_dir = Path(tempfile.mkdtemp(prefix="maigo-adapter-test-"))

    def make_context(self, plugin_id: str = "maigo.maibot-adapter") -> PluginContext:
        paths = PluginPaths(data_dir=self.data_dir, runtime_dir=self.data_dir / "runtime")
        return PluginContext(plugin_id, rpc_call=self.rpc, paths=paths)

    async def rpc(
        self,
        method: str,
        plugin_id: str,
        payload: Optional[Dict[str, Any]],
        timeout_ms: Optional[int] = None,
    ) -> Any:
        del plugin_id, timeout_ms
        assert payload is not None
        assert method == "cap.call"
        capability = str(payload["capability"])
        args = dict(payload["args"])
        self.calls.append((capability, args))
        if capability == "chat.get_stream_by_group_id":
            return {"success": True, "stream": self.streams.get(str(args["group_id"]))}
        if capability == "maisaka.proactive.trigger":
            self.trigger_calls.append(dict(args))
            return {"success": True, "queued": True, "stream_id": args.get("stream_id")}
        if capability == "config.get":
            return {"success": True, "value": self.config.get(args["key"], args.get("default"))}
        raise AssertionError(f"未预期能力调用: {capability}")


class ManualClock:
    """确定性可拨动毫秒时钟。"""

    def __init__(self, start_ms: int = 1_800_000_000_000) -> None:
        self.now_ms = start_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, delta_ms: int) -> None:
        self.now_ms += delta_ms
