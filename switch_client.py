"""MaiSwitch HTTP REST 客户端协议与 httpx 异步实现。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol

import httpx


class SwitchClientError(RuntimeError):
    """调用 MaiSwitch 服务端发生的网络或状态码异常。"""


class SwitchClient(Protocol):
    """MaiSwitch 同步插件依赖的异步接口端口。"""

    async def post_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]: ...

    async def get_context(
        self,
        *,
        branch_id: str,
        target_visibility: str,
        if_brief_etag: str = "",
        active_actors: str = "",
        save_id: str = "",
        include_topics: bool = True,
    ) -> Dict[str, Any]: ...

    async def consume_topic(self, topic_id: int, *, branch_id: str) -> Dict[str, Any]: ...

    async def recall_life(
        self,
        *,
        query: str,
        days: int = 7,
        limit: int = 5,
        target_visibility: str = "public",
    ) -> str: ...

    async def close(self) -> None: ...


class HttpSwitchClient:
    """基于 httpx.AsyncClient 的 MaiSwitch REST JSON 客户端。

    触发条件：插件后台刷新循环、发送完成上报钩子、规划前置注入与回想工具调用时触发；
    执行顺序：按需惰性构建带超时的 AsyncClient（携可选 Bearer 令牌）-> 发起 HTTP 请求
    -> 非 2xx 响应统一包装为 SwitchClientError，供调用方降级放行并记录上下文日志。
    """

    def __init__(self, *, base_url: str, token: str = "", timeout_ms: int = 5_000) -> None:
        self._base_url = base_url.strip().rstrip("/")
        cleaned_token = token.strip()
        self._headers: Dict[str, str] = {"Authorization": f"Bearer {cleaned_token}"} if cleaned_token else {}
        self._timeout_s = max(0.5, timeout_ms / 1000)
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers=self._headers,
                timeout=self._timeout_s,
            )
        return self._client

    async def post_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """批量或单批上报 MaiBot 的入站/出站经历事件。"""
        if not self._base_url:
            raise SwitchClientError("MaiSwitch 服务端地址未配置")
        client = self._get_client()
        try:
            resp = await client.post("/api/v1/events", json={"events": events})
            resp.raise_for_status()
            data = resp.json()
            return dict(data) if isinstance(data, dict) else {}
        except httpx.HTTPError as exc:
            raise SwitchClientError(f"上报事件到 MaiSwitch 失败: {exc}") from exc

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
        """拉取指定可见性档位的慢层与快层双层上下文视图。"""
        if not self._base_url:
            raise SwitchClientError("MaiSwitch 服务端地址未配置")
        params: Dict[str, Any] = {
            "branch_id": branch_id,
            "target_visibility": target_visibility,
            "include_topics": str(bool(include_topics)).lower(),
        }
        if if_brief_etag:
            params["if_brief_etag"] = if_brief_etag
        if active_actors:
            params["active_actors"] = active_actors
        if save_id:
            params["save_id"] = save_id
        client = self._get_client()
        try:
            resp = await client.get("/api/v1/views/context", params=params)
            resp.raise_for_status()
            data = resp.json()
            return dict(data) if isinstance(data, dict) else {}
        except httpx.HTTPError as exc:
            raise SwitchClientError(f"拉取 MaiSwitch 上下文视图失败: {exc}") from exc

    async def consume_topic(self, topic_id: int, *, branch_id: str) -> Dict[str, Any]:
        """标记某条跨端话头已在当前分支提及，避免其他分支冷场时重复照念。"""
        if not self._base_url:
            raise SwitchClientError("MaiSwitch 服务端地址未配置")
        client = self._get_client()
        try:
            resp = await client.post(f"/api/v1/topics/{int(topic_id)}/consume", json={"branch_id": branch_id})
            resp.raise_for_status()
            data = resp.json()
            return dict(data) if isinstance(data, dict) else {}
        except httpx.HTTPError as exc:
            raise SwitchClientError(f"标记话头已消费失败: {exc}") from exc

    async def recall_life(
        self,
        *,
        query: str,
        days: int = 7,
        limit: int = 5,
        target_visibility: str = "public",
    ) -> str:
        """调用 HTTP 经历检索口，按目标可见性返回与关键字相关的跨端事件与立场。"""
        if not self._base_url:
            raise SwitchClientError("MaiSwitch 服务端地址未配置")
        client = self._get_client()
        try:
            resp = await client.get(
                "/api/v1/views/recall",
                params={
                    "query": query,
                    "days": int(days),
                    "limit": int(limit),
                    "target_visibility": target_visibility,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return str(data.get("text") or "") if isinstance(data, dict) else ""
        except httpx.HTTPError as exc:
            raise SwitchClientError(f"检索 MaiSwitch 跨端经历失败: {exc}") from exc

    async def close(self) -> None:
        """关闭底层 httpx 连接池（幂等）。"""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
        self._client = None
