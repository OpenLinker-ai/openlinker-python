from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
import threading
from dataclasses import replace

import httpx
import pytest

from openlinker import registration as registration_module
from openlinker.client import Client
from openlinker.error import OpenLinkerError
from openlinker.registration import (
    AgentRegistration,
    EnsureAgentRequest,
    EnvRegistrationStore,
    REGISTER_FORCE_NEW,
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


PENDING_KEY = "OPENLINKER_PENDING_REGISTRATION"
REPLAY_BASE = "https://api.example.test"


class ReplayCore:
    """Models Core registration, including the replay of a redeemed token for the same slug."""

    def __init__(self) -> None:
        self.pending: set[str] = set()
        self.redeemed: dict[str, dict] = {}
        self.slugs: set[str] = set()
        self.create_token_calls = 0
        self.register_tokens: list[str] = []
        self.drop_after_commit = 0
        self.fail_register_transiently = 0

    async def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        auth = request.headers.get("Authorization", "")
        if request.url.path == "/api/v1/creator/agent-tokens":
            assert auth.startswith("Bearer ol_user_")
            self.create_token_calls += 1
            token = f"ol_agent_minted_{self.create_token_calls}"
            self.pending.add(token)
            return httpx.Response(
                201,
                json={
                    "id": "token-pending",
                    "prefix": "ol_agent_mint",
                    "status": "pending_registration",
                    "plaintext_token": token,
                },
            )
        assert request.url.path == "/api/v1/agent-registration/agents"
        if self.fail_register_transiently:
            self.fail_register_transiently -= 1
            raise httpx.ConnectError("connection failed before Core", request=request)
        token = auth.removeprefix("Bearer ")
        self.register_tokens.append(token)
        slug = body.get("slug", "")
        if token in self.pending and slug in self.slugs:
            return _error_response(409, "CONFLICT", "slug 已被占用")
        if token in self.pending:
            response = {
                "agent": {"id": "agent-1", "slug": slug, "name": body.get("name", "")},
                "agent_token": {
                    "id": "token-1",
                    "prefix": "ol_agent_reg",
                    "status": "active_runtime",
                },
            }
            self.pending.discard(token)
            self.redeemed[token] = response
            self.slugs.add(slug)
            if self.drop_after_commit:
                self.drop_after_commit -= 1
                raise httpx.ConnectError("connection dropped", request=request)
            return httpx.Response(201, json=response)
        replay = self.redeemed.get(token)
        if replay is not None and replay["agent"]["slug"] == slug:
            return httpx.Response(201, json=replay)
        return _error_response(401, "UNAUTHORIZED", "Agent Token 已过期 / 已撤销 / 已被使用")


def _error_response(status: int, code: str, message: str) -> httpx.Response:
    return httpx.Response(status, json={"error": {"code": code, "message": message}})


class RelocatedRegistrationStore(EnvRegistrationStore):
    """Subclasses the env store but keeps registrations elsewhere, as an encrypting store might."""

    def __init__(self, path) -> None:
        super().__init__(path)
        self.registration: AgentRegistration | None = None

    def load_agent_registration(self) -> AgentRegistration | None:
        return replace(self.registration) if self.registration else None

    def save_agent_registration(self, registration: AgentRegistration) -> None:
        self.registration = replace(registration)


def _pending_line(record: dict) -> str:
    return f"{PENDING_KEY}={json.dumps(json.dumps({'version': 1, **record}))}\n"


def _read_pending(path) -> dict | None:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{PENDING_KEY}="):
            return json.loads(json.loads(line[len(PENDING_KEY) + 1 :]))
    return None


@pytest.fixture
def clean_registration_env(monkeypatch):
    for key in ("OPENLINKER_USER_TOKEN", "OPENLINKER_AGENT_TOKEN", "OPENLINKER_API_BASE"):
        monkeypatch.delenv(key, raising=False)


async def _ensure(core: ReplayCore, request: EnsureAgentRequest) -> AgentRegistration:
    async with httpx.AsyncClient(transport=httpx.MockTransport(core.handler)) as http_client:
        client = Client(
            REPLAY_BASE,
            http_client=http_client,
            user_token=request.user_token or "ol_user_creator",
        )
        return await ensure_agent(request, client=client)


@pytest.mark.asyncio
async def test_ensure_agent_resumes_minted_registration_after_lost_response(
    tmp_path, clean_registration_env
):
    path = tmp_path / ".env"
    path.write_text("UNRELATED=value\n", encoding="utf-8")
    path.chmod(0o600)
    core = ReplayCore()
    core.drop_after_commit = 1
    store = EnvRegistrationStore(path)
    request = EnsureAgentRequest(
        slug="demo-agent",
        name="Demo Agent",
        endpoint_auth_header="Bearer endpoint-secret",
        api_base=REPLAY_BASE,
        user_token="ol_user_creator",
        store=store,
    )

    with pytest.raises(httpx.ConnectError):
        await _ensure(core, request)
    record = _read_pending(path)
    assert record is not None
    assert record["agent_token"] == "ol_agent_minted_1"
    assert record["api_base"] == REPLAY_BASE
    assert record["request"]["slug"] == "demo-agent"
    assert "endpoint-secret" not in path.read_text(encoding="utf-8")
    assert record["endpoint_auth_fingerprint"] == _fingerprint("Bearer endpoint-secret")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert store.load_agent_registration() is None

    registration = await _ensure(core, request)
    assert registration.agent_token == "ol_agent_minted_1"
    assert registration.agent_id == "agent-1"
    assert core.create_token_calls == 1
    assert core.register_tokens == ["ol_agent_minted_1", "ol_agent_minted_1"]
    assert _read_pending(path) is None
    assert path.read_text(encoding="utf-8").startswith("UNRELATED=value\n")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    again = await _ensure(core, request)
    assert again.agent_token == "ol_agent_minted_1"
    assert len(core.register_tokens) == 2


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.platform == "win32" or getattr(os, "geteuid", lambda: 1)() == 0,
    reason="needs a directory the current user cannot write",
)
async def test_ensure_agent_resumes_minted_registration_after_save_failure(
    tmp_path, clean_registration_env
):
    directory = tmp_path / "state"
    directory.mkdir()
    core = ReplayCore()
    read_only_after_commit = True

    async def handler(request: httpx.Request) -> httpx.Response:
        response = await core.handler(request)
        if read_only_after_commit and request.url.path == "/api/v1/agent-registration/agents":
            directory.chmod(0o500)
        return response

    request = EnsureAgentRequest(
        slug="demo-agent",
        name="Demo Agent",
        api_base=REPLAY_BASE,
        user_token="ol_user_creator",
        store=EnvRegistrationStore(directory / ".env"),
    )
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = Client(REPLAY_BASE, http_client=http_client, user_token="ol_user_creator")
            with pytest.raises(PermissionError):
                await ensure_agent(request, client=client)
            read_only_after_commit = False
            directory.chmod(0o700)
            registration = await ensure_agent(request, client=client)
    finally:
        directory.chmod(0o700)
    assert registration.agent_token == "ol_agent_minted_1"
    assert core.create_token_calls == 1
    assert core.register_tokens == ["ol_agent_minted_1", "ol_agent_minted_1"]


@pytest.mark.asyncio
async def test_env_registration_store_subclass_keeps_previous_behaviour(
    tmp_path, clean_registration_env
):
    path = tmp_path / ".env"
    core = ReplayCore()
    core.drop_after_commit = 1
    request = EnsureAgentRequest(
        slug="demo-agent",
        name="Demo Agent",
        api_base=REPLAY_BASE,
        user_token="ol_user_creator",
        store=RelocatedRegistrationStore(path),
    )

    with pytest.raises(httpx.ConnectError):
        await _ensure(core, request)
    # No plaintext journal is written outside the subclass's own save.
    assert not path.exists()
    # Exactly as before: a new token is minted and the slug is already taken.
    with pytest.raises(OpenLinkerError) as raised:
        await _ensure(core, request)
    assert raised.value.status_code == 409
    assert core.create_token_calls == 2
    assert core.register_tokens == ["ol_agent_minted_1", "ol_agent_minted_2"]


@pytest.mark.asyncio
async def test_ensure_agent_retries_same_supplied_token_after_lost_response(
    tmp_path, clean_registration_env
):
    path = tmp_path / ".env"
    core = ReplayCore()
    core.pending.add("ol_agent_provided")
    core.drop_after_commit = 1
    store = EnvRegistrationStore(path)
    request = EnsureAgentRequest(
        slug="provided-agent",
        name="Provided Agent",
        agent_token="ol_agent_provided",
        api_base=REPLAY_BASE,
        store=store,
    )

    with pytest.raises(httpx.ConnectError):
        await _ensure(core, request)
    assert store.load_agent_registration() is None
    assert _read_pending(path)["agent_token"] == "ol_agent_provided"

    registration = await _ensure(core, request)
    assert registration.agent_token == "ol_agent_provided"
    assert core.register_tokens == ["ol_agent_provided", "ol_agent_provided"]
    assert _read_pending(path) is None


def _recorded_request(**fields) -> dict:
    """The request an ensure_agent call with these fields records, in wire form."""
    request = {
        "slug": "demo-agent",
        "name": "Demo Agent",
        "endpoint_url": "https://agent.example.test/old",
        "price_per_call_cents": 0,
        "tags": ["agent", "runtime"],
        "ability_tags": ["agent", "runtime"],
        "visibility": "private",
        "connection_mode": "runtime",
    }
    request.update(fields)
    return {key: value for key, value in request.items() if value is not None}


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _recorded_mint(**fields) -> dict:
    """The mint section a minting ensure_agent call with these settings records."""
    mint = {
        "token_name": "Demo Agent",
        "token_scopes": ["agent:pull", "agent:call"],
        "token_expires_in_minutes": 0,
        "creator_fingerprint": hashlib.sha256(b"ol_user_creator").hexdigest()[:32],
    }
    mint.update(fields)
    return mint


_MISMATCH_BASE = {
    "slug": "demo-agent",
    "name": "Demo Agent",
    "endpoint_url": "https://agent.example.test/old",
    "api_base": REPLAY_BASE,
    "user_token": "ol_user_creator",
}
_EXACT_RECORD = {
    "request": _recorded_request(),
    "endpoint_auth_fingerprint": "none",
    "mint": _recorded_mint(),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("record", "fields", "want"),
    [
        pytest.param(
            {**_EXACT_RECORD, "request": _recorded_request(slug="old-agent")},
            {},
            "ol_agent_minted_1",
            id="changed-slug",
        ),
        pytest.param(
            _EXACT_RECORD,
            {"endpoint_url": "https://agent.example.test/new"},
            "ol_agent_minted_1",
            id="changed-endpoint-url",
        ),
        pytest.param(
            _EXACT_RECORD,
            {"user_token": "", "agent_token": "ol_agent_supplied_b"},
            "ol_agent_supplied_b",
            id="different-supplied-token",
        ),
        pytest.param(
            {**_EXACT_RECORD, "request": _recorded_request(slug="first-agent")},
            {"slug": "second-agent", "policy": REGISTER_FORCE_NEW},
            "ol_agent_minted_1",
            id="force-new-different-slug",
        ),
        pytest.param(
            {**_EXACT_RECORD, "api_base": "https://other.example.test"},
            {},
            "ol_agent_minted_1",
            id="other-platform",
        ),
        pytest.param(
            _EXACT_RECORD,
            {"token_scopes": ["agent:pull"]},
            "ol_agent_minted_1",
            id="changed-token-scopes",
        ),
        pytest.param(
            _EXACT_RECORD,
            {"token_expires_in_minutes": 60},
            "ol_agent_minted_1",
            id="changed-token-expiry",
        ),
        pytest.param(
            _EXACT_RECORD,
            {"token_name": "Renamed token"},
            "ol_agent_minted_1",
            id="changed-token-name",
        ),
        pytest.param(
            _EXACT_RECORD,
            {"user_token": "ol_user_other"},
            "ol_agent_minted_1",
            id="different-user-token",
        ),
        pytest.param(
            {"request": _recorded_request(), "endpoint_auth_fingerprint": "none"},
            {},
            "ol_agent_minted_1",
            id="record-without-mint-parameters",
        ),
        pytest.param(
            {**_EXACT_RECORD, "endpoint_auth_fingerprint": _fingerprint("Bearer endpoint-a")},
            {"endpoint_auth_header": "Bearer endpoint-b"},
            "ol_agent_minted_1",
            id="changed-endpoint-auth-header",
        ),
        pytest.param(
            {**_EXACT_RECORD, "endpoint_auth_fingerprint": _fingerprint("Bearer endpoint-a")},
            {},
            "ol_agent_minted_1",
            id="removed-endpoint-auth-header",
        ),
        pytest.param(
            {"request": _recorded_request(), "mint": _recorded_mint()},
            {},
            "ol_agent_minted_1",
            id="record-without-endpoint-auth-fingerprint",
        ),
    ],
)
async def test_ensure_agent_never_replays_mismatched_pending_record(
    tmp_path, clean_registration_env, record, fields, want
):
    path = tmp_path / ".env"
    path.write_text(
        _pending_line({"api_base": REPLAY_BASE, "agent_token": "ol_agent_stale_a", **record}),
        encoding="utf-8",
    )
    core = ReplayCore()
    # The stale token is still redeemable, so replaying it would succeed visibly.
    core.pending.update({"ol_agent_stale_a", "ol_agent_supplied_b"})

    registration = await _ensure(
        core,
        EnsureAgentRequest(**{**_MISMATCH_BASE, **fields}, store=EnvRegistrationStore(path)),
    )
    assert core.register_tokens == [want]
    assert registration.agent_token == want
    assert _read_pending(path) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "recorded_credential"),
    [
        pytest.param({"Authorization": "Bearer ol_user_override"}, "ol_user_override", id="override"),
        pytest.param({"authorization": "Bearer ol_user_lower"}, None, id="ambiguous-case-variant"),
    ],
)
async def test_creator_fingerprint_names_the_authorization_the_mint_sends(
    tmp_path, clean_registration_env, headers, recorded_credential
):
    path = tmp_path / ".env"
    core = ReplayCore()
    core.fail_register_transiently = 1
    mint_auth: list[list[str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/creator/agent-tokens":
            mint_auth.append(request.headers.get_list("Authorization"))
        return await core.handler(request)

    async def start(client_headers: dict[str, str]) -> AgentRegistration:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = Client(
                REPLAY_BASE,
                http_client=http_client,
                user_token="ol_user_creator",
                headers=client_headers,
            )
            return await ensure_agent(
                EnsureAgentRequest(
                    slug="demo-agent",
                    name="Demo Agent",
                    api_base=REPLAY_BASE,
                    store=EnvRegistrationStore(path),
                ),
                client=client,
            )

    # The mint names a creator other than the configured User Token, then the
    # redemption fails before Core sees it.
    with pytest.raises(httpx.ConnectError):
        await start(headers)
    if recorded_credential is None:
        assert len(mint_auth[0]) == 2
        assert not path.exists()
    else:
        assert mint_auth[0] == [f"Bearer {recorded_credential}"]
        record = _read_pending(path)
        assert record["mint"]["creator_fingerprint"] == _fingerprint(recorded_credential)

    # Restarting as the configured creator without the override must not
    # replay the token the other credential minted.
    registration = await start({})
    assert registration.agent_token == "ol_agent_minted_2"
    assert core.create_token_calls == 2
    assert "ol_agent_minted_1" not in core.register_tokens
    assert _read_pending(path) is None


@pytest.mark.asyncio
async def test_ensure_agent_keeps_error_and_drops_record_when_core_refuses(
    tmp_path, clean_registration_env
):
    path = tmp_path / ".env"
    core = ReplayCore()
    core.slugs.add("taken-agent")

    with pytest.raises(OpenLinkerError) as raised:
        await _ensure(
            core,
            EnsureAgentRequest(
                slug="taken-agent",
                name="Taken Agent",
                api_base=REPLAY_BASE,
                user_token="ol_user_creator",
                store=EnvRegistrationStore(path),
            ),
        )
    assert raised.value.status_code == 409
    assert str(raised.value) == "openlinker: CONFLICT: slug 已被占用"
    assert not path.exists()


@pytest.mark.asyncio
async def test_refused_attempt_keeps_record_written_by_another_attempt(
    tmp_path, clean_registration_env
):
    path = tmp_path / ".env"
    core = ReplayCore()
    core.slugs.add("taken-agent")
    concurrent = _pending_line(
        {
            "api_base": REPLAY_BASE,
            "agent_token": "ol_agent_concurrent_a",
            "request": _recorded_request(slug="other-agent"),
        }
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/agent-registration/agents":
            # Attempt A records its token while this attempt's redemption is in flight.
            path.write_text(concurrent, encoding="utf-8")
        return await core.handler(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = Client(REPLAY_BASE, http_client=http_client, user_token="ol_user_creator")
        with pytest.raises(OpenLinkerError) as raised:
            await ensure_agent(
                EnsureAgentRequest(
                    slug="taken-agent",
                    name="Taken Agent",
                    api_base=REPLAY_BASE,
                    user_token="ol_user_creator",
                    store=EnvRegistrationStore(path),
                ),
                client=client,
            )
    assert raised.value.status_code == 409
    assert _read_pending(path)["agent_token"] == "ol_agent_concurrent_a"


_COMPLETED_ELSEWHERE_ENV = (
    'OPENLINKER_AGENT_ID="agent-elsewhere"\n'
    'OPENLINKER_AGENT_TOKEN="ol_agent_completed_elsewhere"\n'
)
_COMPLETED_ELSEWHERE = AgentRegistration(
    agent_id="agent-elsewhere",
    agent_slug="other-agent",
    agent_token="ol_agent_completed_elsewhere",
    api_base=REPLAY_BASE,
)


def _write_as_other_process(path) -> None:
    """Append a completed registration directly, as another process would."""
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    path.write_text(existing + _COMPLETED_ELSEWHERE_ENV, encoding="utf-8")


def _save_concurrently(path) -> tuple[threading.Thread, list[BaseException]]:
    """Start another store instance's completed save and give it 0.2s to land.

    While the caller holds the path lock it cannot.
    """
    errors: list[BaseException] = []

    def save() -> None:
        try:
            EnvRegistrationStore(path).save_agent_registration(_COMPLETED_ELSEWHERE)
        except BaseException as error:  # noqa: BLE001 - reported by the test
            errors.append(error)

    thread = threading.Thread(target=save)
    thread.start()
    thread.join(timeout=0.2)
    return thread, errors


def _assert_completed_elsewhere_survived(path, existing: str, saved) -> None:
    if saved is not None:
        thread, errors = saved
        thread.join()
        assert errors == []
    loaded = EnvRegistrationStore(path).load_agent_registration()
    assert loaded is not None
    assert loaded.agent_token == "ol_agent_completed_elsewhere"
    assert path.read_text(encoding="utf-8").startswith(existing)


async def _ensure_taken(core: ReplayCore, path) -> None:
    with pytest.raises(OpenLinkerError) as raised:
        await _ensure(
            core,
            EnsureAgentRequest(
                slug="taken-agent",
                name="Taken Agent",
                api_base=REPLAY_BASE,
                user_token="ol_user_creator",
                store=EnvRegistrationStore(path),
            ),
        )
    assert raised.value.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", ["", "UNRELATED=value\n"], ids=["created-file", "other-lines"])
@pytest.mark.parametrize("same_process", [False, True], ids=["other-process", "other-store"])
async def test_pending_cleanup_keeps_completed_registration_written_meanwhile(
    tmp_path, clean_registration_env, monkeypatch, existing, same_process
):
    path = tmp_path / ".env"
    if existing:
        path.write_text(existing, encoding="utf-8")
    core = ReplayCore()
    core.slugs.add("taken-agent")
    saved = None

    def hook(stage: str) -> None:
        # Another writer completes a registration after this attempt compared
        # its record: another process before the guarding re-read, or another
        # store instance just before the unlink or replace.
        nonlocal saved
        if not same_process and stage == "cleanup.reread":
            _write_as_other_process(path)
        elif same_process and stage == "cleanup.mutate" and saved is None:
            saved = _save_concurrently(path)

    monkeypatch.setattr(registration_module, "_pending_test_hook", hook)
    await _ensure_taken(core, path)
    assert not same_process or saved is not None
    _assert_completed_elsewhere_survived(path, existing, saved)


@pytest.mark.asyncio
@pytest.mark.parametrize("same_process", [False, True], ids=["other-process", "other-store"])
async def test_pending_write_keeps_registration_another_start_completed_meanwhile(
    tmp_path, clean_registration_env, monkeypatch, same_process
):
    path = tmp_path / ".env"
    core = ReplayCore()
    core.slugs.add("taken-agent")
    saved = None
    wrote = False

    def hook(stage: str) -> None:
        # Start A completes while start B's record write is in flight: another
        # process before B's guarding re-read, or another store instance just
        # before B's replace.
        nonlocal saved, wrote
        if not same_process and stage == "write.reread" and not wrote:
            wrote = True
            _write_as_other_process(path)
        elif same_process and stage == "write.rename" and saved is None:
            saved = _save_concurrently(path)

    monkeypatch.setattr(registration_module, "_pending_test_hook", hook)
    await _ensure_taken(core, path)
    assert not same_process or saved is not None
    _assert_completed_elsewhere_survived(path, "", saved)
    assert _read_pending(path) is None


@pytest.mark.asyncio
async def test_contended_pending_write_is_given_up_and_start_continues(
    tmp_path, clean_registration_env, monkeypatch
):
    path = tmp_path / ".env"
    core = ReplayCore()
    writes = 0

    def other_writer(stage: str) -> None:
        # Another process changes the file before every guarding re-read.
        nonlocal writes
        if stage != "write.reread":
            return
        writes += 1
        path.write_text(f"OTHER_WRITER={writes}\n", encoding="utf-8")

    monkeypatch.setattr(registration_module, "_pending_test_hook", other_writer)
    registration = await _ensure(
        core,
        EnsureAgentRequest(
            slug="demo-agent",
            name="Demo Agent",
            api_base=REPLAY_BASE,
            user_token="ol_user_creator",
            store=EnvRegistrationStore(path),
        ),
    )
    assert registration.agent_token == "ol_agent_minted_1"
    assert writes == 3
    raw = path.read_text(encoding="utf-8")
    assert _read_pending(path) is None
    assert raw.startswith("OTHER_WRITER=3\n")


class _MintCredentialAuth(httpx.Auth):
    """Sends another creator's credential on mint requests only."""

    def auth_flow(self, request: httpx.Request):
        if request.url.path == "/api/v1/creator/agent-tokens":
            request.headers["Authorization"] = "Bearer ol_user_from_auth"
        yield request


async def _observe_request(request: httpx.Request) -> None:
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_options",
    [
        pytest.param({"auth": _MintCredentialAuth()}, id="httpx-auth"),
        pytest.param({"event_hooks": {"request": [_observe_request]}}, id="request-hook"),
    ],
)
async def test_unknowable_minting_credential_writes_no_record(
    tmp_path, clean_registration_env, client_options
):
    path = tmp_path / ".env"
    core = ReplayCore()
    core.fail_register_transiently = 1

    async def start() -> AgentRegistration:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(core.handler), **client_options
        ) as http_client:
            client = Client(REPLAY_BASE, http_client=http_client, user_token="ol_user_creator")
            return await ensure_agent(
                EnsureAgentRequest(
                    slug="demo-agent",
                    name="Demo Agent",
                    api_base=REPLAY_BASE,
                    store=EnvRegistrationStore(path),
                ),
                client=client,
            )

    # The redemption fails before Core sees it; with an unknowable creator
    # credential no record is written, so the restart mints as before.
    with pytest.raises(httpx.ConnectError):
        await start()
    assert not path.exists()
    registration = await start()
    assert registration.agent_token == "ol_agent_minted_2"
    assert core.create_token_calls == 2


@pytest.mark.asyncio
async def test_ensure_agent_continues_normally_after_matching_record_is_refused(
    tmp_path, clean_registration_env
):
    path = tmp_path / ".env"
    path.write_text(
        _pending_line(
            {
                "api_base": REPLAY_BASE,
                "agent_token": "ol_agent_expired_unused",
                "request": _recorded_request(endpoint_url=None),
                "endpoint_auth_fingerprint": "none",
                "mint": _recorded_mint(),
            }
        ),
        encoding="utf-8",
    )
    core = ReplayCore()

    registration = await _ensure(
        core,
        EnsureAgentRequest(
            slug="demo-agent",
            name="Demo Agent",
            api_base=REPLAY_BASE,
            user_token="ol_user_creator",
            store=EnvRegistrationStore(path),
        ),
    )
    assert registration.agent_token == "ol_agent_minted_1"
    assert core.register_tokens == ["ol_agent_expired_unused", "ol_agent_minted_1"]
    assert _read_pending(path) is None


@pytest.mark.asyncio
async def test_ensure_agent_leaves_completed_registration_and_stale_record_untouched(
    tmp_path, clean_registration_env
):
    core = ReplayCore()
    completed = tmp_path / "completed.env"
    original = (
        'OPENLINKER_AGENT_ID="agent-1"\nOPENLINKER_AGENT_TOKEN="ol_agent_active"\n'
        + _pending_line(
            {
                "api_base": REPLAY_BASE,
                "agent_token": "ol_agent_stale",
                "request": _recorded_request(),
            }
        )
    )
    completed.write_text(original, encoding="utf-8")
    reused = await _ensure(
        core,
        EnsureAgentRequest(
            api_base=REPLAY_BASE,
            user_token="ol_user_creator",
            store=EnvRegistrationStore(completed),
        ),
    )
    assert reused.agent_token == "ol_agent_active"
    assert core.register_tokens == []
    assert completed.read_text(encoding="utf-8") == original
