from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
from typing import Any

from .client import Client
from .registration import DEFAULT_NATIVE_API_BASE, ensure_runtime_agent
from .runtime import Native
from .types import EnsureRuntimeAgentRequest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="openlinker")
    parser.add_argument("--api-base", default=os.getenv("OPENLINKER_API_BASE", DEFAULT_NATIVE_API_BASE))
    sub = parser.add_subparsers(dest="command", required=True)

    register = sub.add_parser("register")
    register.add_argument("--slug", required=True)
    register.add_argument("--name", required=True)
    register.add_argument("--description", default="")
    register.add_argument("--connector", default=os.getenv("OPENLINKER_WORKER_CONNECTOR", "runtime_pull"))
    register.add_argument("--policy", default=os.getenv("OPENLINKER_REGISTER_POLICY", "reuse_existing"))

    status = sub.add_parser("status")
    status.add_argument("--runtime-token", default=os.getenv("OPENLINKER_RUNTIME_TOKEN", ""))

    worker = sub.add_parser("worker")
    worker.add_argument("handler", help="module:function returning a Native handler")
    worker.add_argument("--runtime-token", default=os.getenv("OPENLINKER_RUNTIME_TOKEN", ""))
    worker.add_argument("--connector", default=os.getenv("OPENLINKER_WORKER_CONNECTOR", "runtime_pull"))
    worker.add_argument("--max-runs", type=int, default=int(os.getenv("OPENLINKER_WORKER_MAX_RUNS", "0") or 0))

    args = parser.parse_args(argv)
    asyncio.run(_main(args))


async def _main(args: argparse.Namespace) -> None:
    if args.command == "register":
        reg = await ensure_runtime_agent(
            EnsureRuntimeAgentRequest(
                slug=args.slug,
                name=args.name,
                description=args.description,
                connector=args.connector,
                policy=args.policy,
                api_base=args.api_base,
            )
        )
        print(json.dumps(reg.to_dict(), indent=2, ensure_ascii=False))
        return
    if args.command == "status":
        async with Client(args.api_base, runtime_token=args.runtime_token) as client:
            heartbeat = await client.validate_runtime_token()
            print(json.dumps(heartbeat.to_dict(), indent=2, ensure_ascii=False))
        return
    if args.command == "worker":
        handler = _load_handler(args.handler)
        await (
            Native(handler)
            .with_api_base(args.api_base)
            .with_runtime_token(args.runtime_token)
            .with_connector(args.connector)
            .with_max_runs(args.max_runs)
            .run()
        )
        return


def _load_handler(spec: str) -> Any:
    module_name, sep, attr = spec.partition(":")
    if not sep or not module_name or not attr:
        raise SystemExit("handler must be module:function")
    module = importlib.import_module(module_name)
    return getattr(module, attr)


if __name__ == "__main__":
    main()

