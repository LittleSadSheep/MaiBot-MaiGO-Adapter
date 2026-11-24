# MaiBot-MaiGO-Adapter

MaiBot 侧的轻量跨端记忆适配器，将 QQ 白名单互通群及可选私聊的麦麦发言上报至 **MaiGO** 跨端生活流中心，并在每次规划与表达前注入分层上下文与熟人名片。

## 核心能力

- **出站自动采集**：挂接 `send_service.after_send`，在白名单群（`visibility="group"`）与私聊（`visibility="private"`）发言完成后异步上报原文与近景对象；未入白名单群聊完全隔离跳过。
- **双层视图注入**：后台按 `if_brief_etag` 刷新并持久化慢层今日简报缓存；`maisaka.planner.before_request` 将慢层稳定简报置于系统提示词后保前缀缓存，将快层实时态与 `actor_cards` 熟人提示附于末尾；`maisaka.replyer.before_request` 同步补充近况约束。
- **跨端开播喊人**：监听 `active_sessions` 中外部直播分支的在播状态，连续稳定达到 `go_live.delay_ms` 且心跳正常时，唤醒 QQ 麦麦进通知群自然喊人。
- **按需检索**：通过 `@Tool("recall_life")` 按当前会话可见性查询近期跨端生活明细；MaiGO 离线时全程零异常降级。

## 运行测试

```bash
.venv\Scripts\ruff check .
.venv\Scripts\pytest -v
```
