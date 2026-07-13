from __future__ import annotations

import json

import httpx
import pytest

from openlinker import client as openlinker_client
from openlinker import runtime


@pytest.mark.asyncio
async def test_native_runner_completes_pull_run():
    claimed = False
    result = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal claimed, result
        assert request.headers.get("Authorization") == "Bearer ol_agent_native"
        if request.method == "POST" and request.url.path == "/api/v1/agent-runtime/heartbeat":
            return httpx.Response(200, json={"agent_id": "agent-native"})
        if request.method == "GET" and request.url.path == "/api/v1/agent-runtime/runs/claim":
            if claimed:
                return httpx.Response(204)
            claimed = True
            return httpx.Response(
                200,
                json={
                    "run_id": "run-native",
                    "agent_id": "agent-native",
                    "input": {"text": "hello"},
                    "source": "api",
                    "result_endpoint": "/api/v1/agent-runtime/runs/run-native/result",
                    "result_method": "POST",
                    "result_required": True,
                },
            )
        if request.method == "POST" and request.url.path == "/api/v1/agent-runtime/runs/run-native/result":
            result = json.loads(request.content)
            return httpx.Response(200, json={"run_id": "run-native", "status": "success"})
        raise AssertionError(f"unexpected request {request.method} {request.url.path}")

    async def native_handler(run: runtime.NativeRun):
        assert run.text() == "hello"
        return {"answer": "ok"}

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client("https://api.example.test", http_client=hc, runtime_token="ol_agent_native")
        await (
            runtime.Native(native_handler)
            .with_client(sdk)
            .with_connector(runtime.RUNTIME_CONNECTOR_PULL)
            .with_pull_wait(0.01)
            .with_max_runs(1)
            .run()
        )

    assert result["status"] == "success"
    assert result["output"]["answer"] == "ok"


@pytest.mark.asyncio
async def test_with_func_runner_and_native_run_from_context():
    claimed = False
    result = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal claimed, result
        if request.method == "POST" and request.url.path == "/api/v1/agent-runtime/heartbeat":
            return httpx.Response(200, json={"agent_id": "agent-native"})
        if request.method == "GET" and request.url.path == "/api/v1/agent-runtime/runs/claim":
            if claimed:
                return httpx.Response(204)
            claimed = True
            return httpx.Response(
                200,
                json={
                    "run_id": "run-text",
                    "agent_id": "agent-native",
                    "input": {"task": "summarize"},
                    "source": "api",
                    "result_required": True,
                },
            )
        if request.method == "POST" and request.url.path == "/api/v1/agent-runtime/runs/run-text/result":
            result = json.loads(request.content)
            return httpx.Response(200, json={"run_id": "run-text", "status": "success"})
        raise AssertionError(f"unexpected request {request.method} {request.url.path}")

    async def text_agent(text: str) -> str:
        assert text == "summarize"
        assert runtime.native_run_from_context().assignment.run_id == "run-text"
        return "done"

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client("https://api.example.test", http_client=hc, runtime_token="ol_agent_native")
        await (
            runtime.WithFunc(text_agent)
            .with_client(sdk)
            .with_connector(runtime.RUNTIME_CONNECTOR_PULL)
            .with_pull_wait(0.01)
            .with_max_runs(1)
            .run()
        )

    assert result["status"] == "success"
    assert result["output"]["text"] == "done"
