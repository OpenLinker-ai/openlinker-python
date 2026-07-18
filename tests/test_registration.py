from __future__ import annotations

import json
import stat
from dataclasses import replace

import httpx
import pytest

from openlinker.client import Client
from openlinker.registration import (
    AgentRegistration,
    EnsureAgentRequest,
    EnvRegistrationStore,
    REGISTER_REUSE_EXISTING,
    ensure_agent,
)


class MemoryRegistrationStore:
    def __init__(self, registration: AgentRegistration | None = None) -> None:
        self.registration = registration

    def load_agent_registration(self) -> AgentRegistration | None:
        return replace(self.registration) if self.registration else None

    def save_agent_registration(self, registration: AgentRegistration) -> None:
        self.registration = replace(registration)


@pytest.mark.asyncio
async def test_ensure_agent_registers_pending_token_then_reuses_store(monkeypatch):
    for key in ("OPENLINKER_USER_TOKEN", "OPENLINKER_AGENT_TOKEN", "OPENLINKER_API_BASE"):
        monkeypatch.delenv(key, raising=False)
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.headers.get("Authorization"), json.loads(request.content)))
        if request.url.path == "/api/v1/creator/agent-tokens":
            return httpx.Response(
                200,
                json={
                    "id": "token-1",
                    "prefix": "ol_agent_demo",
                    "status": "pending_registration",
                    "plaintext_token": "ol_agent_plaintext",
                },
            )
        if request.url.path == "/api/v1/agent-registration/agents":
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
        raise AssertionError(f"unexpected request {request.url.path}")

    store = MemoryRegistrationStore()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = Client(
            "https://api.example.test",
            http_client=http_client,
            user_token="ol_user_creator",
        )
        request = EnsureAgentRequest(
            slug="demo",
            name="Demo",
            api_base="https://api.example.test",
            user_token="ol_user_creator",
            store=store,
        )
        first = await ensure_agent(request, client=client)
        second = await ensure_agent(
            EnsureAgentRequest(policy=REGISTER_REUSE_EXISTING, store=store),
            client=client,
        )

    assert first.agent_id == "agent-1"
    assert first.agent_token == "ol_agent_plaintext"
    assert second == first
    assert [item[0] for item in calls] == [
        "/api/v1/creator/agent-tokens",
        "/api/v1/agent-registration/agents",
    ]
    assert calls[0][1] == "Bearer ol_user_creator"
    assert calls[1][1] == "Bearer ol_agent_plaintext"
    assert calls[1][2]["connection_mode"] == "runtime"


def test_env_registration_store_preserves_unrelated_values_and_forces_private_mode(tmp_path):
    path = tmp_path / ".env"
    path.write_text("UNRELATED=value\nOPENLINKER_AGENT_TOKEN=old\n", encoding="utf-8")
    path.chmod(0o644)
    store = EnvRegistrationStore(path)

    store.save_agent_registration(
        AgentRegistration(
            agent_id="agent-1",
            agent_slug="demo",
            agent_name="Demo",
            agent_token="ol_agent_secret",
            token_id="token-1",
            api_base="https://api.example.test",
            updated_at="2026-07-18T00:00:00Z",
        )
    )

    raw = path.read_text(encoding="utf-8")
    loaded = store.load_agent_registration()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert "UNRELATED=value" in raw
    assert "OPENLINKER_AGENT_TOKEN=old" not in raw
    assert loaded is not None
    assert loaded.agent_id == "agent-1"
    assert loaded.agent_token == "ol_agent_secret"
