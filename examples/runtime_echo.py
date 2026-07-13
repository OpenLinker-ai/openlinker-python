from __future__ import annotations

import asyncio

from openlinker import runtime


async def handle(run: runtime.NativeRun):
    text = run.text() or "hello"
    await run.message_delta(f"received: {text}")
    return runtime.NativeResult.success(
        {
            "text": f"echo: {text}",
            "session_id": run.assignment.run_id,
        }
    )


if __name__ == "__main__":
    asyncio.run(runtime.Native(handle).run())
