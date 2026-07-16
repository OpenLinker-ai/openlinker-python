# 参与 openlinker-python

English documentation: [CONTRIBUTING.md](./CONTRIBUTING.md)

感谢你帮助改进 OpenLinker Core 的官方异步 Python SDK。

## 开发环境

```bash
python -m pip install -e ".[dev,grpc]"
python -m ruff check .
python -m compileall -q src
python -m pytest
python -m build
```

需要 Python 3.10 或更高版本。测试凭据和地址必须使用虚构值；不要提交真实 User Token、
Agent Token、mTLS 私钥、callback secret、私有地址或用户数据。

## 范围

本仓库接受：

- Core 公共 API 的异步封装
- SSE 与签名 Webhook 辅助函数
- A2A JSON-RPC、HTTP+JSON/SSE 和可选 gRPC 客户端
- 持久化 Runtime Worker、transport、Store 与契约测试
- 与这些能力有关的类型、打包、示例和文档

不属于本仓库：

- Hosted 服务商品、订单、钱包、计费和账号 API
- 本包尚未实现的独立 MCP client
- 进程级 HTTP、命令、Codex 或 A2A Adapter；它们属于
  `openlinker-agent-node`

## Pull Request

- 公开 API 改动保持聚焦并带完整类型。
- 为行为变化补充或更新测试。
- 公开事实变化时同步 README、PARITY、CHANGELOG 和契约副本。
- 保持 User Token 与 Agent Token 边界。
- 明确写出 pre-1.0 breaking change。

安全漏洞请按 [SECURITY.zh-CN.md](./SECURITY.zh-CN.md) 报告，不要提交公开 Issue。

## 许可证

贡献内容按本仓库的 MIT 许可证发布。
