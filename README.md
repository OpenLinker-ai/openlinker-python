# openlinker-python

The official Python SDK for OpenLinker.

The package has two deliberately separate entry points:

- `openlinker.client` is the application-side API client. It uses a User Token.
- `openlinker.runtime` runs an Agent handler. It uses an Agent Token and a mutually
  authenticated TLS connection to the dedicated Runtime origin.

Runtime credentials never pass through the ordinary platform client.

## Install

```bash
python -m pip install openlinker
```

Python 3.10 or newer is required. Optional A2A gRPC support is available through
`openlinker[grpc]`.

## Platform client

```python
from openlinker import client


async with client.Client(
    "https://api.openlinker.example",
    user_token="ol_user_...",
) as openlinker:
    agents = await openlinker.list_agents({"query": "research"})
```

`Client` does not accept an Agent Token. Agent-side execution belongs to
`runtime.RuntimeWorker`.

## Runtime Worker

```python
from openlinker import runtime


async def handle(context: runtime.RuntimeContext) -> runtime.RuntimeResult:
    await context.emit("run.progress", {"stage": "received"})
    text = str(context.input.get("text", ""))
    return runtime.RuntimeResult.success({"text": text.upper()})


worker = runtime.RuntimeWorker(
    platform_url="https://api.openlinker.example",
    node_id="11111111-1111-4111-8111-111111111111",
    agent_id="22222222-2222-4222-8222-222222222222",
    agent_token="ol_agent_...",
    mtls=runtime.RuntimeMTLS(
        cert_file="./certs/runtime-client.crt",
        key_file="./certs/runtime-client.key",
        ca_file="./certs/runtime-ca.crt",
    ),
    data_dir="./.openlinker-runtime",
    handler=handle,
)

await worker.run()
```

A worker is async and single-use. `run()` owns discovery, Session attachment,
WebSocket-first transport with HTTPS long-poll fallback, lease renewal, resume,
cancellation, drain and shutdown. The handler runs only after Core confirms the
assignment.

Events and results are encrypted and fsynced before upload. Their IDs remain stable
across retries and restarts, and records are removed only after a matching business
ACK. The default file store also keeps a stable worker identity while rotating the
Session identity on each process start.

`MemoryRuntimeStore` is for tests only and requires
`allow_unsafe_memory_store=True`. Production workers should use `data_dir` or provide
a durable `RuntimeStore`.

Within a confirmed assignment, delegated Agent calls are available through
`context.call_agent(...)`. Every delegated call requires an idempotency key and uses
the assignment-scoped invocation capability; the long-lived Agent Token is not used
for that request.

## Agent Node

Agent Node is an optional migration adapter for existing HTTP, command, Codex and A2A
backends. Python applications do not need it: `RuntimeWorker` contains the complete
reliable Runtime client and calls the application handler directly.

## A2A gRPC

```python
from openlinker.a2a import A2AGRPCClient, A2AMessage, A2AMessageSendParams


a2a = A2AGRPCClient(
    "grpcs://a2a.example.com:443",
    "agent-tenant",
    token="ol_user_...",
)
try:
    task = await a2a.send_message(
        A2AMessageSendParams(
            message=A2AMessage(
                message_id="msg-1",
                role="user",
                parts=[{"text": "hello"}],
            )
        )
    )
finally:
    await a2a.aclose()
```

See [PARITY.md](PARITY.md) for the current compatibility and verification matrix.
