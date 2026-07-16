# 支持说明

English documentation: [SUPPORT.md](./SUPPORT.md)

可复现 bug、文档问题和属于 `openlinker-python` 范围的功能建议可以提交 GitHub Issue。

## 提交前

- 在 `main` 的明确 commit 上复现。
- 提供 Python、操作系统、SDK commit，以及 Core release 或 commit。
- 提供最小 Python 复现和脱敏日志。
- 说明问题属于平台 Client、callback 验证、A2A、Runtime transport 还是持久 Store。
- 删除 User Token、Agent Token、mTLS 材料、callback secret、私有地址和用户数据。

## 不在这里处理

- 安全漏洞；请按 [SECURITY.zh-CN.md](./SECURITY.zh-CN.md) 报告
- Hosted 服务市场、计费、钱包或账号行为
- 无法整理成公开复现的私有部署排障
- `openlinker-agent-node` 所有的进程级 backend Adapter

跨仓库问题应同时提供 SDK commit、Core release 或 commit，以及涉及的 API 或 Runtime
契约行为。
