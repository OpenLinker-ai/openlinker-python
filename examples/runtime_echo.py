from __future__ import annotations

import asyncio
import os

from openlinker import runtime


async def handle(context: runtime.RuntimeContext) -> runtime.RuntimeResult:
    text = str(context.input.get("text", ""))
    await context.emit("run.progress", {"stage": "received"})
    return runtime.RuntimeResult.success({"text": f"echo: {text}"})


async def main() -> None:
    worker = runtime.RuntimeWorker(
        platform_url=os.environ["OPENLINKER_URL"],
        node_id=os.environ["OPENLINKER_NODE_ID"],
        agent_id=os.environ["OPENLINKER_AGENT_ID"],
        agent_token=os.environ["OPENLINKER_AGENT_TOKEN"],
        mtls=runtime.RuntimeMTLS(
            cert_file=os.environ["OPENLINKER_RUNTIME_MTLS_CERT_FILE"],
            key_file=os.environ["OPENLINKER_RUNTIME_MTLS_KEY_FILE"],
            ca_file=os.environ["OPENLINKER_RUNTIME_MTLS_CA_FILE"],
        ),
        data_dir=os.environ.get("OPENLINKER_RUNTIME_DATA_DIR", "./.openlinker-runtime"),
        handler=handle,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
