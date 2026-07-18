from __future__ import annotations

import json
import os
import tempfile
import threading
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Protocol, runtime_checkable

from .client import Client
from .types import CreateAgentTokenRequest, RegisterAgentViaTokenRequest


DEFAULT_API_BASE = "https://api.openlinker.ai"
DEFAULT_REGISTRATION_ENV_PATH = ".env"
REGISTER_REUSE_EXISTING = "reuse_existing"
REGISTER_ROTATE_TOKEN = "rotate_token"
REGISTER_FORCE_NEW = "force_new"
REGISTER_VALIDATE_ONLY = "validate_only"
_REGISTER_POLICIES = {
    REGISTER_REUSE_EXISTING,
    REGISTER_ROTATE_TOKEN,
    REGISTER_FORCE_NEW,
    REGISTER_VALIDATE_ONLY,
}
_ENV_KEYS = (
    "OPENLINKER_API_BASE",
    "OPENLINKER_AGENT_ID",
    "OPENLINKER_AGENT_SLUG",
    "OPENLINKER_AGENT_NAME",
    "OPENLINKER_AGENT_TOKEN",
    "OPENLINKER_AGENT_TOKEN_ID",
    "OPENLINKER_AGENT_TOKEN_PREFIX",
    "OPENLINKER_REGISTERED_AT",
    "OPENLINKER_UPDATED_AT",
)


@dataclass
class AgentRegistration:
    agent_id: str = ""
    agent_slug: str = ""
    agent_name: str = ""
    agent_token: str = ""
    token_id: str = ""
    token_prefix: str = ""
    api_base: str = ""
    registered_at: str = ""
    updated_at: str = ""


@runtime_checkable
class RegistrationStore(Protocol):
    def load_agent_registration(self) -> AgentRegistration | None: ...

    def save_agent_registration(self, registration: AgentRegistration) -> None: ...


class EnvRegistrationStore:
    def __init__(self, path: str | Path = DEFAULT_REGISTRATION_ENV_PATH) -> None:
        self.path = Path(path or DEFAULT_REGISTRATION_ENV_PATH)
        self._lock = threading.Lock()

    def load_agent_registration(self) -> AgentRegistration | None:
        with self._lock:
            try:
                values = _read_env(self.path)
            except FileNotFoundError:
                return None
        registration = AgentRegistration(
            agent_id=values.get("OPENLINKER_AGENT_ID", ""),
            agent_slug=values.get("OPENLINKER_AGENT_SLUG", ""),
            agent_name=values.get("OPENLINKER_AGENT_NAME", ""),
            agent_token=values.get("OPENLINKER_AGENT_TOKEN", ""),
            token_id=values.get("OPENLINKER_AGENT_TOKEN_ID", ""),
            token_prefix=values.get("OPENLINKER_AGENT_TOKEN_PREFIX", ""),
            api_base=values.get("OPENLINKER_API_BASE", ""),
            registered_at=values.get("OPENLINKER_REGISTERED_AT", ""),
            updated_at=values.get("OPENLINKER_UPDATED_AT", ""),
        )
        if not registration.agent_id and not registration.agent_token:
            return None
        return registration

    def save_agent_registration(self, registration: AgentRegistration) -> None:
        if registration is None:
            return
        with self._lock:
            try:
                values = _read_env(self.path)
            except FileNotFoundError:
                values = {}
            updates = {
                "OPENLINKER_AGENT_ID": registration.agent_id,
                "OPENLINKER_AGENT_SLUG": registration.agent_slug,
                "OPENLINKER_AGENT_NAME": registration.agent_name,
                "OPENLINKER_AGENT_TOKEN": registration.agent_token,
                "OPENLINKER_AGENT_TOKEN_ID": registration.token_id,
                "OPENLINKER_AGENT_TOKEN_PREFIX": registration.token_prefix,
                "OPENLINKER_API_BASE": registration.api_base,
                "OPENLINKER_REGISTERED_AT": registration.registered_at,
                "OPENLINKER_UPDATED_AT": registration.updated_at,
            }
            values.update({key: value for key, value in updates.items() if value})
            for key, value in updates.items():
                if not value:
                    values.pop(key, None)
            _write_env(self.path, values)


@dataclass
class EnsureAgentRequest:
    slug: str = ""
    name: str = ""
    description: str = ""
    endpoint_url: str = ""
    endpoint_auth_header: str = ""
    price_per_call_cents: int = 0
    tags: list[str] = field(default_factory=list)
    skill_ids: list[str] = field(default_factory=list)
    visibility: str = ""
    connection_mode: str = ""
    mcp_tool_name: str = ""
    token_name: str = ""
    token_scopes: list[str] = field(default_factory=list)
    token_expires_in_minutes: int = 0
    policy: str = REGISTER_REUSE_EXISTING
    user_token: str = ""
    agent_token: str = ""
    api_base: str = ""
    store: RegistrationStore | None = None
    env_path: str | Path = DEFAULT_REGISTRATION_ENV_PATH


async def ensure_agent(
    request: EnsureAgentRequest,
    *,
    client: Client | None = None,
) -> AgentRegistration:
    if not isinstance(request, EnsureAgentRequest):
        raise TypeError("openlinker: ensure_agent requires EnsureAgentRequest")
    req = replace(request)
    req.tags = list(request.tags)
    req.skill_ids = list(request.skill_ids)
    req.token_scopes = list(request.token_scopes)
    if req.policy not in _REGISTER_POLICIES:
        raise ValueError(f"openlinker: unsupported registration policy {req.policy!r}")

    store = req.store or EnvRegistrationStore(req.env_path)
    stored = store.load_agent_registration()
    req.user_token = _first(req.user_token, os.getenv("OPENLINKER_USER_TOKEN"))
    req.agent_token = _first(
        req.agent_token,
        os.getenv("OPENLINKER_AGENT_TOKEN"),
        stored.agent_token if stored else "",
    )
    req.api_base = _first(
        req.api_base,
        os.getenv("OPENLINKER_API_BASE"),
        stored.api_base if stored else "",
        client.base_url if client else "",
        DEFAULT_API_BASE,
    )
    req.slug = _first(req.slug, stored.agent_slug if stored else "")
    req.name = _first(req.name, stored.agent_name if stored else "")
    req.visibility = _first(req.visibility, "private")
    req.connection_mode = _normalize_connection_mode(req.connection_mode)
    req.token_name = _first(req.token_name, req.name, req.slug, "Python runtime worker")
    if not req.token_scopes:
        req.token_scopes = ["agent:pull", "agent:call"]
    if not req.tags:
        req.tags = ["agent", "runtime"]

    if stored and req.policy == REGISTER_REUSE_EXISTING and req.agent_token:
        return _effective_registration(stored, req)

    owns_client = client is None
    sdk = client or Client(req.api_base, user_token=req.user_token)
    try:
        if req.policy == REGISTER_VALIDATE_ONLY:
            if not stored or not req.agent_token:
                raise ValueError(
                    "openlinker: no stored Agent registration is available to validate"
                )
            if not req.user_token and owns_client:
                raise ValueError(
                    "openlinker: OPENLINKER_USER_TOKEN is required to validate registration"
                )
            tokens = await sdk.list_agent_tokens(
                {"agent_id": stored.agent_id, "limit": 50}
            )
            valid = any(
                item.id == stored.token_id
                and item.status == "active_runtime"
                and item.revoked_at is None
                for item in tokens.items
            )
            if not valid:
                raise ValueError("openlinker: no valid stored Agent registration found")
            return _effective_registration(stored, req)

        if stored and req.policy == REGISTER_ROTATE_TOKEN:
            if not stored.agent_id:
                raise ValueError("openlinker: stored Agent ID is required to rotate token")
            token = await sdk.create_agent_token(
                CreateAgentTokenRequest(
                    name=req.token_name,
                    agent_id=stored.agent_id,
                    scopes=req.token_scopes,
                    expires_in_minutes=req.token_expires_in_minutes,
                )
            )
            if not token.plaintext_token:
                raise ValueError("openlinker: platform did not return Agent Token plaintext")
            registration = AgentRegistration(
                agent_id=stored.agent_id,
                agent_slug=_first(req.slug, stored.agent_slug),
                agent_name=_first(req.name, stored.agent_name),
                agent_token=token.plaintext_token,
                token_id=token.id,
                token_prefix=token.prefix,
                api_base=req.api_base,
                registered_at=stored.registered_at or _utc_now(),
                updated_at=_utc_now(),
            )
            store.save_agent_registration(registration)
            return registration

        if stored is None and req.policy == REGISTER_REUSE_EXISTING and req.agent_token:
            registered = await sdk.register_agent_via_token(
                req.agent_token, _registration_request(req)
            )
            registration = _registered_response(req, req.agent_token, registered)
            store.save_agent_registration(registration)
            return registration

        if not req.user_token and owns_client:
            raise ValueError(
                "openlinker: OPENLINKER_USER_TOKEN is required to create an Agent"
            )

        if not req.slug or not req.name:
            raise ValueError("openlinker: Agent slug and name are required to create an Agent")
        pending = await sdk.create_agent_token(
            CreateAgentTokenRequest(
                name=req.token_name,
                scopes=req.token_scopes,
                expires_in_minutes=req.token_expires_in_minutes,
            )
        )
        if not pending.plaintext_token:
            raise ValueError(
                "openlinker: platform did not return pending Agent Token plaintext"
            )
        registered = await sdk.register_agent_via_token(
            pending.plaintext_token, _registration_request(req)
        )
        registration = _registered_response(req, pending.plaintext_token, registered)
        store.save_agent_registration(registration)
        return registration
    finally:
        if owns_client:
            await sdk.aclose()


def _registration_request(req: EnsureAgentRequest) -> RegisterAgentViaTokenRequest:
    return RegisterAgentViaTokenRequest(
        slug=req.slug or None,
        name=req.name,
        description=req.description or None,
        endpoint_url=req.endpoint_url or None,
        endpoint_auth_header=req.endpoint_auth_header or None,
        price_per_call_cents=req.price_per_call_cents,
        tags=req.tags,
        ability_tags=req.tags,
        skill_ids=req.skill_ids,
        visibility=req.visibility,
        connection_mode=req.connection_mode,
        mcp_tool_name=req.mcp_tool_name or None,
    )


def _registered_response(req, agent_token, response) -> AgentRegistration:
    now = _utc_now()
    return AgentRegistration(
        agent_id=response.agent.id,
        agent_slug=response.agent.slug,
        agent_name=response.agent.name,
        agent_token=agent_token,
        token_id=response.agent_token.id,
        token_prefix=response.agent_token.prefix,
        api_base=req.api_base,
        registered_at=now,
        updated_at=now,
    )


def _effective_registration(
    stored: AgentRegistration,
    req: EnsureAgentRequest,
) -> AgentRegistration:
    return replace(
        stored,
        agent_slug=_first(req.slug, stored.agent_slug),
        agent_name=_first(req.name, stored.agent_name),
        agent_token=_first(req.agent_token, stored.agent_token),
        api_base=_first(req.api_base, stored.api_base),
    )


def _normalize_connection_mode(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"", "runtime", "runtime_ws", "runtime_pull", "agent_node"}:
        return "runtime"
    return value.strip()


def _first(*values: str | None) -> str:
    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[7:].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = _unquote(value.strip())
    return values


def _write_env(path: Path, values: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        existing = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        existing = []
    managed = set(_ENV_KEYS)
    seen: set[str] = set()
    output: list[str] = []
    for raw in existing:
        line = raw.strip()
        candidate = line[7:].strip() if line.startswith("export ") else line
        key = candidate.split("=", 1)[0].strip() if "=" in candidate else ""
        if key not in managed:
            output.append(raw)
            continue
        seen.add(key)
        if values.get(key):
            output.append(f"{key}={json.dumps(values[key])}")
    for key in _ENV_KEYS:
        if key not in seen and values.get(key):
            output.append(f"{key}={json.dumps(values[key])}")
    content = "\n".join(output) + ("\n" if output else "")
    descriptor, temporary = tempfile.mkstemp(
        prefix=".openlinker-registration-",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", closefd=True) as stream:
            descriptor = -1
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] == '"':
        try:
            decoded = json.loads(value)
            if isinstance(decoded, str):
                return decoded
        except ValueError:
            pass
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1]
    return value


EnsureAgent = ensure_agent
NewEnvRegistrationStore = EnvRegistrationStore
