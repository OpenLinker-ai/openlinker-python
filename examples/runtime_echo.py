from __future__ import annotations

import asyncio
import logging
import os
import signal

from openlinker import runtime


executions = 0


async def handle(context: runtime.RuntimeContext) -> runtime.RuntimeResult:
    global executions
    executions += 1
    await context.emit(
        "run.progress",
        {"stage": "handled", "sdk": "python", "execution": executions},
    )
    return runtime.RuntimeResult.success(
        {
            "sdk_language": "python",
            "configured_transport": os.environ.get("OPENLINKER_RUNTIME_TRANSPORT", "auto"),
            "handler_execution": executions,
            "input": context.input,
        }
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    worker = runtime.RuntimeWorker(
        platform_url=os.environ["OPENLINKER_URL"],
        runtime_url=os.environ.get("OPENLINKER_RUNTIME_URL", ""),
        node_id=os.environ["OPENLINKER_NODE_ID"],
        node_version="openlinker-python/runtime-worker",
        agent_id=os.environ["OPENLINKER_AGENT_ID"],
        agent_token=os.environ["OPENLINKER_AGENT_TOKEN"],
        mtls=runtime.RuntimeMTLS(
            cert_file=os.environ["OPENLINKER_RUNTIME_MTLS_CERT_FILE"],
            key_file=os.environ["OPENLINKER_RUNTIME_MTLS_KEY_FILE"],
            ca_file=os.environ["OPENLINKER_RUNTIME_MTLS_CA_FILE"],
        ),
        data_dir=os.environ.get("OPENLINKER_RUNTIME_DATA_DIR", "./.openlinker-runtime"),
        transport=os.environ.get("OPENLINKER_RUNTIME_TRANSPORT", "auto"),
        retry_minimum=float(os.environ.get("OPENLINKER_RUNTIME_RETRY_MIN_SECONDS", "0.1")),
        retry_maximum=float(os.environ.get("OPENLINKER_RUNTIME_RETRY_MAX_SECONDS", "1")),
        heartbeat_interval=float(
            os.environ.get("OPENLINKER_RUNTIME_HEARTBEAT_SECONDS", "2")
        ),
        handler=handle,
    )
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        asyncio.create_task(worker.stop())

    for stop_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(stop_signal, request_stop)
        except NotImplementedError:
            pass

    print(
        "runtime worker example starting: "
        f"sdk=python transport={os.environ.get('OPENLINKER_RUNTIME_TRANSPORT', 'auto')}",
        flush=True,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
