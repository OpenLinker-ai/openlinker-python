# Support

Chinese documentation: [SUPPORT.zh-CN.md](./SUPPORT.zh-CN.md)

Use GitHub issues for reproducible bugs, documentation problems, and feature
requests within the `openlinker-python` scope.

## Before Opening an Issue

- Reproduce on a named commit from `main`.
- Include Python, operating system, SDK commit, and Core release or commit.
- Include a minimal Python reproduction and sanitized logs.
- State whether the problem affects the platform Client, callback verification,
  A2A, Runtime transport, or durable store.
- Remove User Tokens, Agent Tokens, mTLS material, callback secrets, private URLs,
  and user data.

## Not Supported Here

- vulnerabilities; follow [SECURITY.md](./SECURITY.md)
- Hosted service marketplace, billing, wallet, or account behavior
- private deployment debugging without a reproducible public case
- process-level backend adapters owned by `openlinker-agent-node`

When a problem crosses repositories, include both the SDK commit and the Core
release or commit, plus the endpoint or Runtime contract behavior involved.
