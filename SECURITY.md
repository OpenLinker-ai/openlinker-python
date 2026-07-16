# Security Policy

Chinese documentation: [SECURITY.zh-CN.md](./SECURITY.zh-CN.md)

Do not open public issues for vulnerabilities.

Use this repository's GitHub private vulnerability reporting form. If GitHub
temporarily makes that form unavailable, open a public issue containing no
vulnerability details and ask the maintainers to provide a private channel.
Include the affected commit, Python and Core versions, a minimal reproduction,
impact, and whether a live credential or user record was involved only in the
private report.

## Supported Versions

The SDK is pre-1.0 and is not yet published on PyPI. Security fixes target the
current `main` branch and any release line maintainers explicitly mark as
supported. Do not assume backports for older commits.

## Security-Sensitive Areas

- User Token and Agent Token separation
- authorization headers and structured errors
- raw-body callback signature verification
- Runtime discovery, redirect refusal, mTLS, and TLS settings
- WebSocket and long-poll Session attachment
- encrypted Runtime journal and Event/Result spool
- filesystem permissions, symlinks, locks, and capacity limits
- A2A push notification credentials and gRPC metadata

Never include real secrets in public reports, tests, screenshots, or logs. Rotate
any credential that may have been exposed before sharing sanitized evidence.

Maintainers will coordinate disclosure after a fix or mitigation is available.
