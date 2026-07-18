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
        seen["idempotency_key"] = request.headers.get("Idempotency-Key")
        return httpx.Response(
            200,
            headers={"Idempotency-Replayed": "true"},
            json={
                "run_id": "run-1",
                "agent_id": "agent-1",
                "agent_slug": "runtime-agent",
                "agent_name": "Runtime Agent",
                "agent_connection_mode": "runtime",
                "status": "success",
                "cost_cents": 0,
                "duration_ms": 12,
                "started_at": "2026-07-18T00:00:00Z",
                "finished_at": "2026-07-18T00:00:01Z",
                "source": "api",
                "runtime_contract_id": "openlinker.runtime.v2",
                "runtime_transport": "long_poll",
                "runtime_transport_reason": "websocket_unavailable",
                "runtime_transport_changed_at": "2026-07-18T00:00:00Z",
                "dispatch_state": "terminal",
                "attempt_count": 1,
                "max_attempts": 3,
                "latest_attempt_id": "attempt-1",
                "replayed": False,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client("https://api.example.test", http_client=hc)
        resp = await sdk.run_agent(
            openlinker_client.RunAgentRequest(
                agent_id="agent-1",
                input={"query": "hello"},
                idempotency_key="logical-run-1",
                task_callback=openlinker_client.TaskCallbackConfig(
                    url="https://caller.example.com/events",
                    token="caller-token",
                    secret="caller-secret",
                    event_types=["run.completed", "run.failed"],
                ),
            )
        )

    assert resp.run_id == "run-1"
    assert resp.agent_connection_mode == "runtime"
    assert resp.runtime_transport == "long_poll"
    assert resp.runtime_transport_reason == "websocket_unavailable"
    assert resp.dispatch_state == "terminal"
    assert resp.attempt_count == 1
    assert seen["path"] == "/api/v1/run"
    assert seen["body"]["agent_id"] == "agent-1"
    assert "agent_connection_mode" not in seen["body"]
    assert "runtime_transport" not in seen["body"]
    assert "idempotency_key" not in seen["body"]
    assert seen["body"]["task_callback"]["secret"] == "caller-secret"
    assert seen["idempotency_key"] == "logical-run-1"
    assert resp.replayed is True


@pytest.mark.asyncio
async def test_run_agent_generates_unique_idempotency_keys_when_omitted():
    keys = []

    async def handler(request: httpx.Request) -> httpx.Response:
        keys.append(request.headers.get("Idempotency-Key"))
        return httpx.Response(201, json={"run_id": f"run-{len(keys)}", "status": "running"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client("https://api.example.test", http_client=hc)
        await sdk.run_agent({"agent_id": "agent-1", "input": {}})
        await sdk.run_agent({"agent_id": "agent-1", "input": {}})

    assert len(keys) == 2
    assert keys[0] != keys[1]
    assert all(key and len(key) == 64 for key in keys)


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["", "line\nbreak", "é", "x" * 256])
async def test_run_agent_rejects_invalid_idempotency_keys_before_network(key):
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client("https://api.example.test", http_client=hc)
        with pytest.raises(ValueError) as raised:
            await sdk.run_agent(
                {"agent_id": "agent-1", "input": {}, "idempotency_key": key}
            )

    assert calls == 0
    if key:
        assert key not in str(raised.value)


@pytest.mark.asyncio
async def test_list_run_events_decodes_items_meta_and_legacy_events():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "event_id": "event-1",
                        "run_id": "run-1",
                        "sequence": 4,
                        "event_type": "run.completed",
                        "payload": {"ok": True},
                        "created_at": "2026-07-18T00:00:00Z",
                    }
                ],
                "meta": {
                    "requested_after_sequence": 0,
                    "effective_after_sequence": 3,
                    "retained_through_sequence": 3,
                    "earliest_available_sequence": 4,
                    "latest_available_sequence": 4,
                    "retention_gap": True,
                    "terminal": True,
                    "stream_complete": True,
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client("https://api.example.test", http_client=hc)
        response = await sdk.list_run_events("run-1")

    assert response.items[0].event_id == "event-1"
    assert response.events is response.items
    assert response.meta.retention_gap is True
    assert response.meta.stream_complete is True


@pytest.mark.asyncio
async def test_list_run_children_decodes_complete_recursive_dto():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "parent_run_id": "run-parent",
                "items": [
                    {
                        "child_run_id": "run-child",
                        "parent_run_id": "run-parent",
                        "caller_agent_id": "agent-caller",
                        "caller_agent_slug": "caller",
                        "caller_agent_name": "Caller",
                        "caller_agent_tags": ["orchestrator"],
                        "caller_skills": [{"id": "skill-plan", "name": "Planning"}],
                        "target_agent_id": "agent-target",
                        "target_agent_slug": "target",
                        "target_agent_name": "Target",
                        "target_agent_tags": ["research"],
                        "target_skills": [
                            {"id": "skill-research", "name": "Research"}
                        ],
                        "reason": "delegate research",
                        "status": "success",
                        "cost_cents": 4,
                        "duration_ms": 12,
                        "started_at": "2026-07-18T00:00:00Z",
                        "finished_at": "2026-07-18T00:00:01Z",
                        "source": "api",
                        "billing_mode": "caller",
                        "a2a_context": {"trace_id": "trace-1"},
                        "children": [],
                    }
                ],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client("https://api.example.test", http_client=hc)
        response = await sdk.list_run_children("run-parent")

    assert response.parent_run_id == "run-parent"
    assert response.items[0].target_skills[0].name == "Research"
    assert response.items[0].a2a_context["trace_id"] == "trace-1"


@pytest.mark.asyncio
async def test_register_agent_via_token_uses_agent_auth_and_runtime_defaults():
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["auth"] = request.headers.get("Authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "agent": {"id": "agent-1", "slug": "demo", "name": "Demo"},
                "agent_token": {
                    "id": "token-1",
                    "prefix": "ol_agent_demo",
                    "status": "active_runtime",
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client(
            "https://api.example.test", http_client=hc, user_token="ol_user_creator"
        )
        response = await sdk.register_agent_via_token(
            " ol_agent_pending ", {"name": "Demo", "tags": ["runtime"]}
        )

    assert response.agent.id == "agent-1"
    assert response.agent_token.id == "token-1"
    assert seen["path"] == "/api/v1/agent-registration/agents"
    assert seen["auth"] == "Bearer ol_agent_pending"
    assert seen["body"]["visibility"] == "private"
    assert seen["body"]["connection_mode"] == "runtime"


@pytest.mark.asyncio
async def test_client_rejects_oversized_response_before_reading_body():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Length": str(openlinker_client.MAX_RESPONSE_BODY_BYTES + 1)},
            content=b"{}",
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        sdk = openlinker_client.Client("https://api.example.test", http_client=hc)
        with pytest.raises(ValueError, match="response body exceeds"):
            await sdk.list_agents()


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
