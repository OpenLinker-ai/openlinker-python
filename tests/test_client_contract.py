from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path

from openlinker.client import Client
from openlinker.types import ListRunChildrenResponse, ListRunEventsResponse, RunResponse


ROOT = Path(__file__).parents[1]


def test_core_client_contract_maps_to_methods_and_response_types():
    contract = _contract("core-client.v1.json")
    assert contract["package"] == "openlinker"
    forbidden = contract["rules"]["forbidden_path_prefixes"]
    for endpoint in contract["endpoints"]:
        assert callable(getattr(Client, endpoint["client_method"], None))
        assert endpoint["path"].startswith("/api/v1/")
        assert not any(endpoint["path"].startswith(prefix) for prefix in forbidden)

    run_fields = {item.name for item in fields(RunResponse)}
    for endpoint in contract["endpoints"]:
        if endpoint["path"] in {"/api/v1/run", "/api/v1/runs"}:
            assert endpoint["required_headers"] == ["Idempotency-Key"]
            assert endpoint["success_statuses"] == [200, 201, 202]
            assert set(endpoint["response_fields"]).issubset(run_fields)

    event_fields = {item.name for item in fields(ListRunEventsResponse)}
    child_fields = {item.name for item in fields(ListRunChildrenResponse)}
    assert {"items", "meta"}.issubset(event_fields)
    assert {"parent_run_id", "items"}.issubset(child_fields)


def test_core_registration_contract_maps_to_client_methods():
    contract = _contract("core-registration.v1.json")
    assert contract["scope"] == "core-registration"
    assert len(contract["endpoints"]) == 9
    for endpoint in contract["endpoints"]:
        assert callable(getattr(Client, endpoint["client_method"], None))
        assert endpoint["auth"] in {"user_token", "agent_token"}


def _contract(name: str):
    return json.loads((ROOT / "contracts" / name).read_text(encoding="utf-8"))
