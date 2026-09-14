import base64
import hashlib
import hmac
import json
from pathlib import Path

import httpx
import pytest

from openlinker.client import Client
from openlinker.runtime import RuntimeDelegationUnsupportedError, RuntimeProtocolError, RuntimeMTLS
from openlinker.runtime.transport import HTTPRuntimeTransport, WebSocketRuntimeTransport
from openlinker.runtime.types import (
    RUNTIME_DELEGATED_RUN_READ_PATH,
    runtime_delegation_read_advertised,
    normalize_runtime_optional_features,
)
from openlinker.types import RecommendTaskRequest

RUN_ID = "77777777-7777-4777-8777-777777777777"
NODE_ID = "11111111-1111-4111-8111-111111111111"
PAYLOAD = base64.urlsafe_b64encode(b'{"audience":"openlinker.runtime.v2/delegation"}').rstrip(b"=").decode()
TOKEN = f"ol_inv_v2.current.{PAYLOAD}.signature"
AUTH = dict(node_envelope="ol_ctx_v2.current.payload.signature", invocation_token=TOKEN,
            idempotency_key="read-child")


@pytest.mark.asyncio
async def test_platform_cancel_and_recommendation_use_user_token_and_core_payloads():
    requests = []

    async def respond(request):
        assert request.method == "POST"
        assert request.headers["authorization"] == "Bearer ol_user_test"
        requests.append((request.url.raw_path, json.loads(request.content) if request.content else None))
        if request.url.path.endswith("/cancel"):
            return httpx.Response(200, json={"run_id": RUN_ID, "status": "running"})
        return httpx.Response(200, json={"task_id": "task-1", "visibility": "private",
            "recommendations": [{"agent": {"slug": "research"}, "matched_skills": [{"id": "skill-1"}]}],
            "next_action": {"type": "choose", "href": "/tasks/task-1"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = Client("https://core.example", user_token="ol_user_test", http_client=http)
        assert (await client.cancel_run("run /?#")).status == "running"
        task = await client.recommend_task(RecommendTaskRequest(
            query="research", template_id="template-1", skill_ids=["skill-1"],
            mcp_tools=["search"], agent_slugs=["research"],
        ))
        assert task.recommendations[0].agent.slug == "research"
        assert task.recommendations[0].matched_skills[0].id == "skill-1"
        assert task.next_action.href == "/tasks/task-1"
    assert requests == [
        (b"/api/v1/runs/run%20%2F%3F%23/cancel", None),
        (b"/api/v1/tasks/recommend", {"query": "research", "template_id": "template-1",
          "skill_ids": ["skill-1"], "mcp_tools": ["search"], "agent_slugs": ["research"]}),
    ]


@pytest.mark.asyncio
async def test_delegated_read_signs_exact_body_and_survives_websocket_wrapper():
    calls = 0

    async def respond(request):
        nonlocal calls
        calls += 1
        assert request.method == "POST"
        assert request.url.path == RUNTIME_DELEGATED_RUN_READ_PATH
        assert json.loads(request.content) == {"run_id": RUN_ID}
        assert request.headers["authorization"] == "Bearer " + TOKEN
        assert "openlinker-runtime-attachment" not in request.headers
        assert request.headers["openlinker-invocation-context"] == AUTH["node_envelope"]
        assert request.headers["idempotency-key"] == AUTH["idempotency_key"]
        domain = "openlinker/runtime-v2/invocation-proof"
        canonical = json.dumps({"body_sha256": hashlib.sha256(request.content).hexdigest(),
            "context": AUTH["node_envelope"], "idempotency_key": AUTH["idempotency_key"],
            "method": "POST", "path": RUNTIME_DELEGATED_RUN_READ_PATH, "version": domain},
            separators=(",", ":"), sort_keys=True).encode()
        signature = hmac.new(hashlib.sha256((domain + "\0" + TOKEN).encode()).digest(),
                             canonical, hashlib.sha256).digest()
        assert request.headers["openlinker-invocation-proof"] == base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
        return httpx.Response(200, json={"run_id": RUN_ID, "status": "success",
                                        "dispatch_state": "terminal", "output": {"answer": 42}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        http = HTTPRuntimeTransport("https://runtime.example", "ol_agent_must_not_leak", RuntimeMTLS(),
                                    node_id=NODE_ID, mtls_required=False, _client=client)
        websocket = WebSocketRuntimeTransport("https://runtime.example", "ol_agent_test", RuntimeMTLS(), http, mtls_required=False)
        for transport in (http, websocket):
            assert (await transport.read_delegated_run(RUN_ID, **AUTH))["output"] == {"answer": 42}
    assert calls == 2


@pytest.mark.asyncio
async def test_delegated_read_rejects_legacy_credentials_and_invalid_responses():
    calls = 0
    response = {"run_id": RUN_ID, "status": "running", "dispatch_state": "pending"}

    async def respond(request):
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=response)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        transport = HTTPRuntimeTransport("https://runtime.example", "ol_agent_test", RuntimeMTLS(),
                                         node_id=NODE_ID, mtls_required=False, _client=client)
        for token in ("", "ol_agent_test", "ol_inv_v2.current.payload.signature"):
            assert not runtime_delegation_read_advertised(token)
            with pytest.raises(RuntimeDelegationUnsupportedError):
                await transport.read_delegated_run(RUN_ID, **{**AUTH, "invocation_token": token})
        with pytest.raises(ValueError):
            await transport.read_delegated_run("invalid", **AUTH)
        assert calls == 0
        assert (await transport.read_delegated_run(RUN_ID, **AUTH))["status"] == "running"
        for invalid in (
            {**response, "run_id": NODE_ID}, {**response, "status": "success"},
            {**response, "input": {"secret": True}}, {**response, "output": "wrong"},
            {**response, "error_code": 42}, {**response, "status": []},
        ):
            response = invalid
            with pytest.raises(RuntimeProtocolError):
                await transport.read_delegated_run(RUN_ID, **AUTH)


def test_optional_contracts_map_to_implemented_methods():
    root = Path(__file__).parents[1] / "contracts"
    tasks = json.loads((root / "core-tasks.v1.json").read_text())
    assert tasks["rules"]["allowed_paths"] == ["/api/v1/tasks/recommend"]
    assert len(tasks["endpoints"]) == 1
    assert callable(getattr(Client, tasks["endpoints"][0]["client_method"]))
    delegated = json.loads((root / "core-runtime-delegation.json").read_text())
    assert delegated["endpoints"][0]["path"] == RUNTIME_DELEGATED_RUN_READ_PATH
    assert callable(getattr(HTTPRuntimeTransport, delegated["endpoints"][0]["client_method"]))


def test_optional_worker_features_are_validated_and_stable():
    assert normalize_runtime_optional_features(["z.v1", "a.v1"]) == ("a.v1", "z.v1")
    for invalid in ([""], ["feature", "feature"], ["lease_fence"], [" Bad"], ["a" * 101]):
        with pytest.raises(ValueError):
            normalize_runtime_optional_features(invalid)
