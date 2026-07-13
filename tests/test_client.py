from __future__ import annotations

import json

import httpx
import pytest

from openlinker import client as openlinker_client


@pytest.mark.asyncio
async def test_list_agents_builds_core_url_and_auth_header():
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["query"] = str(request.url.query.decode())
        seen["auth"] = request.headers.get("Authorization")
        seen["sdk"] = request.headers.get("X-OpenLinker-SDK")
        return httpx.Response(200, json={"items": [], "total": 0, "page": 2, "size": 5})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as hc:
        sdk = openlinker_client.Client(
            "https://api.example.test/api/v1",
            http_client=hc,
            user_token="ol_user_test",
        )
        resp = await sdk.list_agents(
            openlinker_client.ListAgentsParams(
                query="data",
                tags=["sql", "finance"],
                page=2,
                size=5,
                callable_only=True,
            )
        )

    assert resp.page == 2
    assert resp.size == 5
    assert seen["path"] == "/api/v1/agents"
    for want in ["q=data", "page=2", "size=5", "callable_only=true", "tags=sql%2Cfinance"]:
        assert want in seen["query"]
    assert seen["auth"] == "Bearer ol_user_test"


@pytest.mark.asyncio
async def test_run_agent_encodes_request_body():
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"run_id": "run-1", "status": "success", "duration_ms": 12})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client("https://api.example.test", http_client=hc)
        resp = await sdk.run_agent(
            openlinker_client.RunAgentRequest(
                agent_id="agent-1",
                input={"query": "hello"},
                task_callback=openlinker_client.TaskCallbackConfig(
                    url="https://caller.example.com/events",
                    token="caller-token",
                    secret="caller-secret",
                    event_types=["run.completed", "run.failed"],
                ),
            )
        )

    assert resp.run_id == "run-1"
    assert seen["path"] == "/api/v1/run"
    assert seen["body"]["agent_id"] == "agent-1"
    assert seen["body"]["task_callback"]["secret"] == "caller-secret"


@pytest.mark.asyncio
async def test_error_response_becomes_openlinker_error():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            headers={"X-Request-Id": "req-1"},
            json={"error": {"code": "FORBIDDEN", "message": "missing scope"}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client("https://api.example.test", http_client=hc)
        with pytest.raises(openlinker_client.OpenLinkerError) as err:
            await sdk.get_run("run-1")

    assert err.value.status_code == 403
    assert err.value.code == "FORBIDDEN"
    assert err.value.request_id == "req-1"


@pytest.mark.asyncio
async def test_a2a_agent_inherits_endpoint_token_and_headers():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200))
    ) as hc:
        sdk = openlinker_client.Client(
            "https://api.example.test/api/v1",
            http_client=hc,
            user_token="ol_user_test",
            headers={"X-Test": "yes"},
        )
        a2a = sdk.a2a_agent("writer-agent")

    assert a2a.endpoint == "https://api.example.test/api/v1/a2a/agents/writer-agent"
    assert a2a.token == "ol_user_test"
    assert a2a.headers["X-Test"] == "yes"
