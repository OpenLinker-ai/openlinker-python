# 安全策略

English documentation: [SECURITY.md](./SECURITY.md)

安全漏洞不要提交公开 Issue。

请使用本仓库的 GitHub Private Vulnerability Reporting 表单。如果 GitHub 暂时无法提供
该表单，可以提交一个不含任何漏洞细节的公开 Issue，请维护者提供私下联系渠道。受影响的
commit、Python 与 Core 版本、最小复现、影响，以及是否涉及真实凭据或用户记录，只能写在
私密报告中。

## 支持版本

SDK 处于 pre-1.0，尚未发布到 PyPI。安全修复面向当前 `main`，以及维护者明确声明仍受
支持的发布线；不要假设旧 commit 一定获得回补。

## 高风险区域

- User Token 与 Agent Token 分离
- Authorization header 与结构化错误
- 原始请求体 callback 签名验证
- Runtime 发现、拒绝重定向、mTLS 与 TLS 设置
- WebSocket 和长轮询 Session attach
- 加密 Runtime journal 与 Event/Result spool
- 文件权限、符号链接、锁与容量限制
- A2A Push Notification 凭据和 gRPC metadata

公开报告、测试、截图和日志中不得包含真实 secret。可能泄露的凭据应先轮换，再提供脱敏
证据。

维护者会在修复或缓解方案可用后协调披露。
