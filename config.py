"""MaiBot-MaiGO-Adapter 配置模型定义。"""

from __future__ import annotations

from typing import List

from maibot_sdk import Field, PluginConfigBase


class PluginSectionConfig(PluginConfigBase):
    """插件总开关与版本节。"""

    __ui_label__ = "插件"

    enabled: bool = Field(default=True, description="是否启用 MaiGO 跨端记忆适配器")
    config_version: str = Field(default="0.1.0", description="配置文件结构版本")


class MaiGOServerConfig(PluginConfigBase):
    """MaiGO 服务端连接配置。"""

    __ui_label__ = "MaiGO 服务"

    url: str = Field(
        default="http://127.0.0.1:8768",
        description="MaiGO HTTP 服务根地址；为空时停用远端同步，插件优雅降级",
    )
    token: str = Field(
        default="",
        description="与 MaiGO auth_token 一致的 Bearer 令牌；本机未设令牌时可留空",
        json_schema_extra={"input_type": "password", "label": "访问令牌"},
    )
    branch_id: str = Field(
        default="maibot",
        description="向 MaiGO 自报的分支标识（默认 maibot）",
    )
    poll_interval_ms: int = Field(
        default=30_000,
        ge=1_000,
        description="后台定时刷新慢层简报缓存与跨端在播状态的间隔（毫秒）",
    )
    request_timeout_ms: int = Field(
        default=5_000,
        ge=500,
        description="单次请求 MaiGO 接口的超时上限（毫秒）",
    )


class ScopeConfig(PluginConfigBase):
    """事件上报与视图注入作用域控制。"""

    __ui_label__ = "同期作用域"

    platform: str = Field(default="qq", description="QQ 适配器平台名称")
    group_ids: List[str] = Field(
        default_factory=list,
        description="与直播及跨端生活互通的白名单群号列表；非名单群一律不上报、不注入",
    )
    sync_private: bool = Field(
        default=True,
        description="是否将私聊互动按 visibility=private 上报并在私聊窗口按 private 档注入",
    )


class InjectionConfig(PluginConfigBase):
    """规划与回复双阶段上下文注入开关。"""

    __ui_label__ = "上下文注入"

    inject_planner: bool = Field(default=True, description="是否在规划请求前注入慢层今日生活与快层动态上下文")
    inject_replyer: bool = Field(default=True, description="是否在生成回复前附加近期立场与直播近况参考")
    include_actor_cards: bool = Field(default=True, description="是否按近期发言对象向规划器附加脱敏跨端熟人提示")
    include_topics: bool = Field(default=True, description="是否在群聊动态上下文中附上未聊过的鲜活话头")


class GoLiveNotifyConfig(PluginConfigBase):
    """跨端正式开播延迟确认喊人配置。"""

    __ui_label__ = "开播喊人"

    group_ids: List[str] = Field(
        default_factory=list,
        description="检测到正式场稳定开播后，唤醒 QQ 麦麦前去喊人的通知群号列表；留空则不自动喊人",
    )
    delay_ms: int = Field(
        default=300_000,
        ge=0,
        description="直播开启并连续维持健康心跳多久后才去群里喊人（毫秒，默认 5 分钟）",
    )
    stale_after_ms: int = Field(
        default=180_000,
        ge=5_000,
        description="心跳超过多久未更新视为断开或异常，此时不触发喊人（毫秒）",
    )
    room_url: str = Field(
        default="",
        description="覆盖拼装的直播间访问地址；留空时对 B 站房间号自动拼 https://live.bilibili.com/<房间号>",
    )


class BackfillConfig(PluginConfigBase):
    """首次连通或新增白名单群聊时的低速历史对话回填配置。"""

    __ui_label__ = "历史回填"

    enabled: bool = Field(default=True, description="首次连通 MaiGO 时是否在后台低速回填近期高信噪比本人回复")
    days: int = Field(default=30, ge=0, le=365, description="首次回填覆盖最近几天（0 表示不限天数）")
    min_chars: int = Field(default=6, ge=1, le=200, description="忽略过短水词回复的最少字符数")
    batch_size: int = Field(default=20, ge=1, le=100, description="每批上报事件条数")
    batch_interval_ms: int = Field(default=1_000, ge=0, le=60_000, description="批次间低速间隔（毫秒）")
    db_path: str = Field(default="", description="显式指定 MaiBot.db 路径；留空自动向上定位")


class MaiGOAdapterConfig(PluginConfigBase):
    """MaiBot-MaiGO-Adapter 顶层配置根。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    server: MaiGOServerConfig = Field(default_factory=MaiGOServerConfig)
    scope: ScopeConfig = Field(default_factory=ScopeConfig)
    injection: InjectionConfig = Field(default_factory=InjectionConfig)
    go_live: GoLiveNotifyConfig = Field(default_factory=GoLiveNotifyConfig)
    backfill: BackfillConfig = Field(default_factory=BackfillConfig)
