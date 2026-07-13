from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

from openlinker import runtime
from openlinker.runtime.types import build_invocation_proof


ROOT = Path(__file__).parents[1]


def test_runtime_contract_matches_constants_and_has_unversioned_urls():
    raw = (ROOT / "contracts" / "core-runtime.json").read_bytes()
    contract = json.loads(raw)

    assert hashlib.sha256(raw).hexdigest() == runtime.RUNTIME_CONTRACT_DIGEST
    assert contract["runtime_contract_id"] == runtime.RUNTIME_CONTRACT_ID
    assert contract["protocol_version"] == runtime.RUNTIME_PROTOCOL_VERSION
    assert tuple(contract["required_features"]) == runtime.RUNTIME_REQUIRED_FEATURES
    assert contract["websocket"]["path"] == "/api/v1/agent-runtime/ws"
    assert "/v2/" not in contract["websocket"]["path"].lower()

    expected = {
        "POST /api/v1/agent-runtime/sessions",
        "POST /api/v1/agent-runtime/sessions/{id}/heartbeat",
        "POST /api/v1/agent-runtime/sessions/{id}/close",
        "POST /api/v1/agent-runtime/runs/claim",
        "POST /api/v1/agent-runtime/runs/{id}/assignment-ack",
        "POST /api/v1/agent-runtime/runs/{id}/assignment-reject",
        "POST /api/v1/agent-runtime/runs/{id}/lease-renew",
        "POST /api/v1/agent-runtime/runs/{id}/events",
        "POST /api/v1/agent-runtime/runs/{id}/result",
        "POST /api/v1/agent-runtime/runs/resume",
        "POST /api/v1/agent-runtime/runs/{id}/cancel-ack",
        "GET /api/v1/agent-runtime/commands",
        "POST /api/v1/agent-runtime/call-agent",
    }
    actual = {f"{item['http_method']} {item['path']}" for item in contract["endpoints"]}
    assert actual == expected
    versioned_runtime_path = "/agent-runtime/" + "v" + str(2) + "/"
    assert all(versioned_runtime_path not in value.lower() for value in actual)
    assert contract.get("legacy_routes", []) == []
    attachment_header = "OpenLinker-Runtime-Attachment"
    for endpoint in contract["endpoints"]:
        headers = endpoint.get("required_headers", [])
        if endpoint["path"] in {
            "/api/v1/agent-runtime/sessions",
            "/api/v1/agent-runtime/call-agent",
        }:
            assert attachment_header not in headers
        else:
            assert attachment_header in headers
    ready_schema = contract["$defs"]["RuntimeReadyPayload"]
    assert "attachment_id" in ready_schema["required"]
    assert ready_schema["properties"]["attachment_id"]["format"] == "uuid"


def test_contract_is_byte_identical_to_the_canonical_core_copy():
    canonical = ROOT.parents[1] / "openlinker-core" / "contracts" / "core-runtime.json"
    if canonical.exists():
        assert (ROOT / "contracts" / "core-runtime.json").read_bytes() == canonical.read_bytes()


def test_public_runtime_surface_has_no_generation_named_identifier_or_file():
    runtime_root = ROOT / "src" / "openlinker" / "runtime"
    assert not (ROOT / "contracts" / "core-runtime.v2.json").exists()
    for path in runtime_root.rglob("*.py"):
        assert "runtime_v2" not in path.name.lower()
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                if not node.name.startswith("_"):
                    assert "runtimev2" not in node.name.lower()


def test_invocation_proof_matches_core_vector():
    body = (
        b'{"target_agent_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",'
        b'"input":{"q":"hello"},"reason":"need data"}'
    )
    proof = build_invocation_proof(
        "ol_inv_v2.current.payload.signature",
        body=body,
        context="ol_ctx_v2.current.payload.signature",
        idempotency_key="delegation-42<&",
    )
    assert proof == "lBuoEqAJKl9ujEr72b0oR3cuuoqJPqCs1vkABcw6zA0"
