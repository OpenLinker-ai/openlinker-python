# OpenLinker SDK parity

The Python SDK follows the same product boundary and canonical Runtime contract as
`openlinker-go` and `openlinker-js`. Naming follows Python conventions; wire behavior
is shared.

## Platform Client

Implemented:

- Agent discovery and Agent Cards
- synchronous and asynchronous Run creation
- Run lookup, events, children, artifacts and messages
- server-sent Run event streaming and callback helpers
- creator Agent and Agent Token management
- webhook signing and verification
- A2A JSON-RPC, REST and optional gRPC helpers

The Client surface accepts a User Token only. Agent-side Session and execution methods
are intentionally absent.

## Runtime Worker

Implemented:

- credential-free discovery of the dedicated Runtime origin
- Agent Token plus mTLS, TLS 1.3 minimum and redirect refusal
- Session attachment generation, heartbeat and close
- WebSocket, long-poll and automatic transport recovery
- durable assignment-before-ACK and confirmed-before-execute ordering
- lease renewal, cancellation, drain, capacity and safe shutdown
- reconnect resume without re-running a previously started handler
- encrypted assignment journal and Event/Result spool
- stable Event/Result IDs, business ACK validation and missing-Event repair
- assignment-scoped delegated Agent calls with mandatory idempotency
- stable worker identity, rotating Session identity and monotonic Session epoch
- file permissions, single-process locking, key/ciphertext integrity and capacity gates

The public Python lifecycle is only:

```python
worker = RuntimeWorker(...)
await worker.run()
```

There is no SDK product CLI, native runner, automatic Agent registration or legacy
heartbeat/claim/result surface.

## Verification

The local suite covers:

- canonical contract digest, endpoint set and URL generation-name guard
- assignment confirmation ordering
- lost assignment/Event/Result ACK replay with stable IDs
- Pull and WebSocket Session conflict recovery during attachment
- established Session conflict failure
- WebSocket to Pull fallback and WebSocket recovery
- unsafe restart refusal for a previously started Attempt
- cancellation propagation and acknowledgement
- discovery/origin/redirect/credential isolation
- attachment establishment, exact heartbeat binding and reattachment generation isolation
- store identity, restart replay, locking, permissions, missing key, modified ciphertext,
  symlink and capacity failures
- Client, webhook and A2A behavior

Live sandbox closure and optional A2A gRPC integration remain release-environment
checks rather than unit tests.
