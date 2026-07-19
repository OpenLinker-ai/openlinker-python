# OpenLinker SDK 功能对齐情况

[English](PARITY.md)

Python SDK 与 `openlinker-go`、`openlinker-js` 使用同一条产品边界和同一份 Runtime
契约。公开命名遵循 Python 习惯，网络行为保持一致。

## 平台 Client

已经实现：

- Agent 发现和 Agent Card；
- 异步 Client 的等待结果和仅启动两种 Run 创建方式；
- Run 详情、事件、子 Run、产物和消息；
- SSE 运行事件和 callback 辅助函数；
- 创建者 Agent 和 Agent Token 管理；
- Webhook 签名和验证；
- A2A JSON-RPC、REST 和可选 gRPC。

Client 只接受 User Token。Agent 端的 Session 和执行方法不会混入平台 Client。

## Runtime Worker

已经实现：

- 不带凭证发现独立 Runtime 地址；
- Agent Token、mTLS、最低 TLS 1.3 和拒绝重定向；
- Session 建立、心跳和关闭；
- WebSocket、长轮询和自动连接恢复；
- 先持久化任务再确认、Core 确认后才执行；
- 租约续期、取消、停止接收新任务、容量控制和安全关闭；
- 重连后继续未完成交付，不重复执行已经开始的 Handler；
- 加密的任务记录以及 Event/Result 待发送队列；
- 重试时保持 Event/Result ID 稳定，核对业务 ACK，并补发缺失 Event；
- 当前任务范围内调用其他 Agent，且必须提供幂等键；
- 稳定的 worker 身份、每次启动更新的 Session 身份和单调递增的 Session 次序；
- 文件权限、单进程锁、密钥和密文完整性、容量上限。

公开的 Python 生命周期只有：

```python
worker = RuntimeWorker(...)
await worker.run()
```

SDK 不提供产品 CLI、旧 Native runner、自动 Agent 注册，也不保留旧的
heartbeat/claim/result 接口。

## 验证范围

本地测试覆盖：

- 标准契约摘要、接口集合和 URL 生成名称；
- 任务确认顺序；
- 任务、Event 和 Result 的 ACK 丢失后，使用稳定 ID 重发；
- Pull 和 WebSocket 建立连接时的 Session 冲突恢复；
- 已建立 Session 出现冲突时安全失败；
- WebSocket 转 Pull 以及 WebSocket 恢复；
- 已经开始的任务无法安全恢复时拒绝重启；
- 取消传递和确认；
- 地址发现、来源限制、重定向和凭据隔离；
- 连接建立、心跳绑定和重新连接的代次隔离；
- 本地状态身份、重启重发、文件锁、权限、缺少密钥、密文被修改、符号链接和容量失败；
- Client、Webhook 和 A2A 行为。

真实沙箱闭环和可选 A2A gRPC 集成仍属于发布环境检查，单元测试不代表这些外部环境
已经连通。
