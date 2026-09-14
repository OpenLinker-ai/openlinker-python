# Changelog

## SDK feature synchronization — Unreleased

- Align the three SDKs on Core Run cancellation and private task recommendation
  contracts; keep task recommendation separate from the general client manifest.
- Document optional Attempt-scoped delegated result reads and the negotiated
  `delegated_run_read.v1` capability without changing the base Runtime digest.
- Add the missing client methods, delegated read transport/handler methods, and
  validated optional Worker features. Preserve credential separation and stop
  delegated reads when the handler finishes or its Attempt is canceled.
- Delegated reads propagate ordinary HTTP errors (including `NOT_FOUND`)
  immediately, matching Go and TypeScript. Only the existing explicit Runtime
  transport-policy recovery can replay a read; durable Worker operations keep
  their retry behavior.
- Correct token-only/mTLS, Agent Node and lifecycle documentation.

## 0.2.0 — unreleased

This is a pre-1.0 breaking Runtime cutover.

- Runtime Workers now accept and strictly validate Core-owned standard and
  Browser authority envelopes, expose Browser interaction policy and canonical
  mutation-origin evidence to handlers, and reject tampered authority fields.
- Added Browser interaction policy, policy generation, canonical mutation
  origins, origin digest, and Browser contract evidence to `RunResponse` and
  the public Core client contract fixture.
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
- Token-only Workers now derive a deterministic, token-scoped Node ID when
  `node_id` is omitted; explicit mTLS deployments still require their
  provisioned Node identity.
- Classified only enumerated permanent HTTP error and WebSocket close shapes as
  fatal. Unknown auth-like failures remain recoverable for DB-backed
  revalidation.
- Added complete bilingual repository, contribution, security, support, release,
  package metadata, typing marker, and MIT license files for the first public
  Python SDK release.
