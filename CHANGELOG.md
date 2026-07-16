# Changelog

## 0.2.0 — unreleased

This is a pre-1.0 breaking Runtime cutover.

- Added the async, single-use `RuntimeWorker` with direct Python handler execution.
- Added credential-free Runtime discovery, mTLS, Session attachment generations,
  WebSocket/long-poll recovery, lease renewal, resume, cancellation and drain.
- Added an authenticated-encryption file store with assignment journal, stable-ID
  Event/Result spool, process locking and capacity protection.
- Restricted the platform `Client` to User Token responsibilities. Runtime uses an
  Agent Token and the dedicated mTLS origin.
- Removed the old heartbeat/claim/result and WebSocket APIs, native runner, automatic
  registration helpers, registration store and SDK CLI.
- Renamed the canonical contract file to the generation-neutral
  `contracts/core-runtime.json`; public Runtime URLs and API names do not expose a
  protocol generation.
- Documented Agent Node as an optional migration adapter rather than the default
  Runtime path.
- Added complete bilingual repository, contribution, security, support, release,
  package metadata, typing marker, and MIT license files for the first public
  Python SDK release.
