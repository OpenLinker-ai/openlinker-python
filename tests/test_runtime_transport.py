from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from openlinker import runtime
from openlinker.runtime.transport import (
    HTTPRuntimeTransport,
    WebSocketRuntimeTransport,
    decode_runtime_discovery_manifest,
    discover_runtime_origin,
    resolve_runtime_transport_selection,
    validate_platform_origin,
    validate_runtime_origin,
)


ATTACHMENT_ID = "88888888-8888-4888-8888-888888888888"
NEXT_ATTACHMENT_ID = "99999999-9999-4999-8999-999999999999"


def test_runtime_discovery_policy_fixtures_are_language_consistent():
    fixture = json.loads(
        (Path(__file__).parents[1] / "contracts/runtime-discovery-policy-fixtures.json").read_text()
    )
    connections = {}
    for item in fixture["cases"]:
        connection = decode_runtime_discovery_manifest(item["manifest"])
        connections[item["name"]] = connection
        policy = connection.policy
        assert {
            "allowed": list(policy.allowed_transports),
            "default": policy.default_transport,
            "heartbeat_interval_ms": round((policy.heartbeat_interval or 5.0) * 1000),
            "session_stale_after_ms": round((policy.session_stale_after or 0.0) * 1000),
            "retry_minimum_ms": round((policy.retry_minimum or 0.25) * 1000),
            "retry_maximum_ms": round((policy.retry_maximum or 15.0) * 1000),
            "websocket_probe_interval_ms": round(
                (policy.websocket_probe_interval or 15.0) * 1000
            ),
            "websocket_probe_timeout_ms": round(
                (policy.websocket_probe_timeout or 10.0) * 1000
            ),
        } == item["expected"]

    for item in fixture["configured_transport_cases"]:
        connection = (
            decode_runtime_discovery_manifest(item["manifest"])
            if "manifest" in item
            else connections[item["manifest_case"]]
        )
        if "error" in item:
            with pytest.raises((ValueError, runtime.RuntimeProtocolError), match=item["error"]):
                resolve_runtime_transport_selection(item["configured"], connection.policy)
            continue
        mode, _ = resolve_runtime_transport_selection(item["configured"], connection.policy)
        assert mode == item["effective"]


def ready_payload(attachment_id: str = ATTACHMENT_ID) -> dict[str, object]:
    return {
        "core_instance_id": "core-a",
        "attachment_id": attachment_id,
        "features": list(runtime.RUNTIME_REQUIRED_FEATURES),
        "offer_ttl_seconds": 30,
        "lease_ttl_seconds": 60,
        "database_time": datetime.now(timezone.utc).isoformat(),
    }


@pytest.mark.asyncio
async def test_discovery_is_credential_free_and_rejects_redirects():
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "base_urls": {"runtime": "https://runtime.example.test"},
                "runtime": {"enabled": True, "mtls_required": True},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await discover_runtime_origin("https://platform.example.test", _client=client)
    assert result == "https://runtime.example.test"
    assert seen[0].url.path == "/.well-known/openlinker.json"
    assert "Authorization" not in seen[0].headers

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                302, headers={"Location": "https://elsewhere.example.test"}
            )
        )
    ) as client:
        with pytest.raises(runtime.RuntimeProtocolError, match="redirect"):
            await discover_runtime_origin("https://platform.example.test", _client=client)


def test_runtime_origin_is_https_only_and_platform_http_is_loopback_only():
    assert validate_runtime_origin("https://runtime.example.test/") == (
        "https://runtime.example.test"
    )
    assert validate_platform_origin("http://127.0.0.1:8080") == "http://127.0.0.1:8080"
    with pytest.raises(ValueError, match="HTTPS"):
        validate_runtime_origin("http://runtime.example.test")
    with pytest.raises(ValueError, match="loopback"):
        validate_platform_origin("http://platform.example.test")
    for value in (
        "https://token@runtime.example.test",
        "https://runtime.example.test/path",
        "https://runtime.example.test?token=secret",
    ):
        with pytest.raises(ValueError):
            validate_runtime_origin(value)


@pytest.mark.asyncio
async def test_http_runtime_uses_canonical_unversioned_path_and_agent_token():
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path.endswith("/heartbeat"):
            return httpx.Response(200, json=ready_payload())
        if request.url.path.endswith("/runs/claim"):
            return httpx.Response(204)
        return httpx.Response(200, json=ready_payload())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transport = HTTPRuntimeTransport(
            "https://runtime.example.test",
            "ol_agent_secret",
            runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
            _client=client,
        )
        await transport.create_session({"runtime_session_id": "session"})
        await transport.heartbeat_session({"runtime_session_id": "session"})
        assert (
            await transport.claim_assignment(
                0, {"runtime_session_id": "session", "capacity": 1, "inflight": 0}
            )
            is None
        )

    assert seen[0].url.path == "/api/v1/agent-runtime/sessions"
    assert "/v2/" not in seen[0].url.path.lower()
    assert seen[0].headers["Authorization"] == "Bearer ol_agent_secret"
    assert "OpenLinker-Runtime-Attachment" not in seen[0].headers
    assert seen[1].headers["OpenLinker-Runtime-Attachment"] == ATTACHMENT_ID
    assert seen[2].headers["OpenLinker-Runtime-Attachment"] == ATTACHMENT_ID


@pytest.mark.asyncio
async def test_runtime_redirect_never_forwards_agent_credentials():
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.headers["Authorization"] == "Bearer ol_agent_secret"
        return httpx.Response(307, headers={"Location": "https://attacker.example.test/steal"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transport = HTTPRuntimeTransport(
            "https://runtime.example.test",
            "ol_agent_secret",
            runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
            _client=client,
        )
        with pytest.raises(runtime.RuntimeProtocolError, match="redirect"):
            await transport.create_session({"runtime_session_id": "session"})
    assert calls == 1


@pytest.mark.asyncio
async def test_invalid_heartbeat_ready_cannot_replace_the_attachment():
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/api/v1/agent-runtime/sessions":
            return httpx.Response(200, json=ready_payload())
        if request.url.path.endswith("/heartbeat"):
            return httpx.Response(200, json=ready_payload("not-a-uuid"))
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transport = HTTPRuntimeTransport(
            "https://runtime.example.test",
            "ol_agent_secret",
            runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
            _client=client,
        )
        await transport.create_session({"runtime_session_id": "session"})
        with pytest.raises(runtime.RuntimeProtocolError, match="attachment_id"):
            await transport.heartbeat_session({"runtime_session_id": "session"})
        assert (
            await transport.claim_assignment(
                0, {"runtime_session_id": "session", "capacity": 1, "inflight": 0}
            )
            is None
        )

    assert seen[-1].headers["OpenLinker-Runtime-Attachment"] == ATTACHMENT_ID


@pytest.mark.asyncio
async def test_heartbeat_requires_the_exact_current_attachment():
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/api/v1/agent-runtime/sessions":
            return httpx.Response(200, json=ready_payload())
        if request.url.path.endswith("/heartbeat"):
            return httpx.Response(200, json=ready_payload(NEXT_ATTACHMENT_ID))
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transport = HTTPRuntimeTransport(
            "https://runtime.example.test",
            "ol_agent_secret",
            runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
            _client=client,
        )
        await transport.create_session({"runtime_session_id": "session"})
        with pytest.raises(runtime.RuntimeProtocolError, match="does not match"):
            await transport.heartbeat_session({"runtime_session_id": "session"})
        assert (
            await transport.claim_assignment(
                0, {"runtime_session_id": "session", "capacity": 1, "inflight": 0}
            )
            is None
        )

    assert seen[-1].headers["OpenLinker-Runtime-Attachment"] == ATTACHMENT_ID


@pytest.mark.asyncio
async def test_inflight_pull_response_is_rejected_after_reattach():
    seen: list[httpx.Request] = []
    claim_entered = asyncio.Event()
    release_claim = asyncio.Event()
    create_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal create_count
        seen.append(request)
        if request.url.path == "/api/v1/agent-runtime/sessions":
            create_count += 1
            attachment = ATTACHMENT_ID if create_count == 1 else NEXT_ATTACHMENT_ID
            return httpx.Response(200, json=ready_payload(attachment))
        if request.url.path.endswith("/runs/claim") and not claim_entered.is_set():
            claim_entered.set()
            await release_claim.wait()
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transport = HTTPRuntimeTransport(
            "https://runtime.example.test",
            "ol_agent_secret",
            runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
            _client=client,
        )
        await transport.create_session({"runtime_session_id": "session"})
        stale_claim = asyncio.create_task(
            transport.claim_assignment(
                0, {"runtime_session_id": "session", "capacity": 1, "inflight": 0}
            )
        )
        await claim_entered.wait()
        await transport.create_session({"runtime_session_id": "session"})
        release_claim.set()
        with pytest.raises(runtime.RuntimeProtocolError, match="changed while a request"):
            await stale_claim
        assert (
            await transport.claim_assignment(
                0, {"runtime_session_id": "session", "capacity": 1, "inflight": 0}
            )
            is None
        )

    claim_headers = [
        request.headers["OpenLinker-Runtime-Attachment"]
        for request in seen
        if request.url.path.endswith("/runs/claim")
    ]
    assert claim_headers == [ATTACHMENT_ID, NEXT_ATTACHMENT_ID]


@pytest.mark.asyncio
async def test_websocket_heartbeat_allows_capacity_change_but_not_identity_change():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(204))
    ) as client:
        http = HTTPRuntimeTransport(
            "https://runtime.example.test",
            "ol_agent_secret",
            runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
            _client=client,
        )
        websocket = WebSocketRuntimeTransport(
            "https://runtime.example.test",
            "ol_agent_secret",
            runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
            http,
        )
        hello = {
            "node_id": "11111111-1111-4111-8111-111111111111",
            "agent_id": "22222222-2222-4222-8222-222222222222",
            "worker_id": "worker-a",
            "runtime_session_id": "33333333-3333-4333-8333-333333333333",
            "session_epoch": 1,
            "capacity": 1,
        }
        websocket._hello = hello
        websocket._ready = runtime.RuntimeReady.from_dict(ready_payload())

        changed_capacity = {**hello, "capacity": 0}
        assert await websocket.heartbeat_session(changed_capacity) == websocket._ready
        with pytest.raises(runtime.RuntimeProtocolError, match="identity mismatch"):
            await websocket.heartbeat_session({**hello, "session_epoch": 2})


@pytest.mark.asyncio
async def test_call_agent_signs_exactly_the_body_it_sends():
    seen: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = request.content
        seen["headers"] = dict(request.headers)
        return httpx.Response(
            200,
            json={
                "run_id": "33333333-3333-4333-8333-333333333333",
                "status": "running",
                "dispatch_state": "pending",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transport = HTTPRuntimeTransport(
            "https://runtime.example.test",
            "ol_agent_secret",
            runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
            _client=client,
        )
        response = await transport.call_agent(
            {
                "target_agent_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "input": {"q": "hello"},
            },
            node_envelope="ol_ctx_v2.current.payload.signature",
            invocation_token="ol_inv_v2.current.payload.signature",
            idempotency_key="delegation-42",
        )

    assert response["status"] == "running"
    assert seen["path"] == "/api/v1/agent-runtime/call-agent"
    body = seen["body"]
    assert isinstance(body, bytes)
    assert json.loads(body) == {
        "target_agent_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "input": {"q": "hello"},
    }
    headers = seen["headers"]
    assert isinstance(headers, dict)
    assert headers["authorization"] == "Bearer ol_inv_v2.current.payload.signature"
    assert "openlinker-runtime-attachment" not in headers
    assert headers["idempotency-key"] == "delegation-42"
    assert headers["openlinker-invocation-context"] == ("ol_ctx_v2.current.payload.signature")
    assert headers["openlinker-invocation-proof"]
