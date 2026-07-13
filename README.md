# openlinker-python

Official Python SDK for OpenLinker.

This package mirrors the public surface of `github.com/OpenLinker-ai/openlinker-go`:

- Core marketplace/run client APIs
- Creator agent registration and runtime-token bootstrap
- Native runtime workers over `runtime_pull` and `runtime_ws`
- Runtime run events, results, heartbeats, and `call-agent`
- Task callback/webhook signing helpers
- A2A JSON-RPC, REST, and optional gRPC helpers

See [PARITY.md](PARITY.md) for the current `openlinker-go` compatibility matrix.

Dependency installs for local development can use the Aliyun mirror:

```bash
python -m pip install \
  -i https://mirrors.aliyun.com/pypi/simple/ \
  --trusted-host mirrors.aliyun.com \
  -e . pytest pytest-asyncio
```

Install optional A2A gRPC support:

```bash
python -m pip install \
  -i https://mirrors.aliyun.com/pypi/simple/ \
  --trusted-host mirrors.aliyun.com \
  -e ".[grpc]"
```

## Native runtime worker

```python
import asyncio
from openlinker import runtime


async def handle(run: runtime.NativeRun):
    text = run.text() or "hello"
    await run.message_delta(f"received: {text}")
    return {"text": f"echo: {text}"}


asyncio.run(
    runtime.Native(handle)
    .with_connector("runtime_pull")
    .run()
)
```

## Auto-register then run

```python
import asyncio
from openlinker import runtime


async def handle(run: runtime.NativeRun):
    return runtime.NativeResult.success({"text": run.text()})


async def main():
    runner = runtime.Native(handle)
    await runner.run_or_register(
        runtime.EnsureRuntimeAgentRequest(
            slug="python-agent-local",
            name="Python Agent",
        )
    )


asyncio.run(main())
```

## A2A gRPC

```python
import asyncio
from openlinker.a2a import A2AGRPCClient, A2AMessage, A2AMessageSendParams


async def main():
    client = A2AGRPCClient(
        "grpcs://a2a.example.com:443",
        "agent-tenant",
        token="runtime-or-user-token",
    )
    task = await client.send_message(
        A2AMessageSendParams(
            message=A2AMessage(
                message_id="msg-1",
                role="user",
                parts=[{"text": "hello"}],
            )
        )
    )
    await client.aclose()
    print(task.id)


asyncio.run(main())
```

A2A gRPC is backed by the official `a2a-sdk[grpc]` optional dependency.
