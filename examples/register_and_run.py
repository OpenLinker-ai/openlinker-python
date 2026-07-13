from __future__ import annotations

import asyncio

from openlinker import runtime


async def handle(run: runtime.NativeRun):
    return {"text": run.text() or "hello"}


async def main():
    runner = runtime.Native(handle)
    await runner.run_or_register(
        runtime.EnsureRuntimeAgentRequest(
            slug="openlinker-python-local",
            name="OpenLinker Python Agent",
            description="Runtime agent created by openlinker-python.",
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
