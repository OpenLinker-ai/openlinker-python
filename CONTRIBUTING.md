# Contributing to openlinker-python

Chinese documentation: [CONTRIBUTING.zh-CN.md](./CONTRIBUTING.zh-CN.md)

Thanks for helping improve the official async Python SDK for OpenLinker Core.

## Development Setup

```bash
python -m pip install -e ".[dev,grpc]"
python -m ruff check .
python -m compileall -q src
python -m pytest
python -m build
```

Use Python 3.10 or newer. Keep test credentials and URLs synthetic; never commit
real User Tokens, Agent Tokens, mTLS keys, callback secrets, private endpoints,
or captured user data.

## Scope

In scope:

- async wrappers for public OpenLinker Core APIs
- SSE and signed webhook helpers
- A2A JSON-RPC, HTTP+JSON/SSE, and optional gRPC clients
- the durable Runtime Worker, transports, stores, and contract tests
- typing, packaging, examples, and documentation for those surfaces

Out of scope:

- Hosted listings, orders, wallets, billing, and account APIs
- a separate MCP client that is not implemented by this package
- process-level HTTP, command, Codex, or A2A adapters; those belong in
  `openlinker-agent-node`

## Pull Requests

- Keep public API changes focused and typed.
- Add or update tests for changed behavior.
- Update README, PARITY, CHANGELOG, and contract copies when their public facts
  change.
- Preserve the User Token / Agent Token boundary.
- Call out pre-1.0 breaking changes explicitly.

Report vulnerabilities through [SECURITY.md](./SECURITY.md), not a public issue.

## License

Contributions are licensed under the MIT license used by this repository.
