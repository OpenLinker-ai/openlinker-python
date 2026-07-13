# openlinker-go parity

This SDK is a Python translation of `github.com/OpenLinker-ai/openlinker-go`.

## Implemented locally, smoke-tested

- Core `Client`
  - `list_agents`, `get_agent`, `get_agent_card`
  - `run_agent`, `start_agent_run`, callback streaming helpers
  - run lookup, events, children, artifacts, messages
  - runtime heartbeat, claim, complete, and `call_agent`
- Creator registration APIs
  - create/list/get/update creator agents
  - create/list/revoke agent tokens
  - register agent via token
- Runtime bootstrap
  - `ensure_runtime_agent`
  - `EnvRegistrationStore`
  - `reuse_existing`, `rotate_token`, `force_new`, `validate_only`
- Native runtime
  - `Native(handler)` runner
  - `WithAgent` / `WithFunc` text-agent runner
  - `runtime_pull`
  - `runtime_ws`
  - live event reporting, message deltas, runtime results
  - `native_run_from_context`
- Webhooks
  - external task callback config
  - HMAC-SHA256 signing and verification helpers
- A2A JSON-RPC and REST
  - send/stream message
  - get/list/cancel/resubscribe task
  - task push-notification config CRUD
  - extended agent card
  - current and legacy method dialects
- A2A gRPC
  - optional `grpc` extra backed by the official `a2a-sdk[grpc]`
  - send/stream message
  - get/list/cancel/resubscribe task
  - task push-notification config CRUD
  - extended agent card
  - OpenLinker dataclass API normalized to/from A2A protobuf messages

## Pending

- Full parity certification.
  The current state is a local first pass with targeted smoke tests. It should not be
  called fully equivalent to `openlinker-go` until the Go SDK test suite has been ported
  or covered by Python equivalents, plus at least one live sandbox integration pass.
- A2A gRPC live conformance.
  The adapter is implemented locally against the official A2A Python SDK transport, but
  has not yet been run against an OpenLinker live gRPC sandbox/conformance fixture.
- Generated protobuf-free conversion edge cases.
  The adapter uses `google.protobuf.json_format` to bridge this SDK's dataclasses and
  official A2A protobuf messages. Text/data/file/message/task basics are covered; richer
  binary file part and push-notification metadata combinations still need live fixtures.

## Verification

Current local tests cover:

- core URL/query/header behavior
- request JSON encoding
- structured platform errors
- `Client.a2a_agent`
- A2A gRPC request/metadata conversion when `a2a-sdk[grpc]` is installed
- native `runtime_pull`
- `WithFunc` and `native_run_from_context`
- webhook signing helpers
