from __future__ import annotations

import os
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from .client import Client
from .error import OpenLinkerError
from .types import (
    REGISTER_POLICY_FORCE_NEW,
    REGISTER_POLICY_REUSE_EXISTING,
    REGISTER_POLICY_ROTATE_TOKEN,
    REGISTER_POLICY_VALIDATE_ONLY,
    CreateAgentRequest,
    CreateAgentTokenRequest,
    EnsureRuntimeAgentRequest,
    RuntimeAgentRegistration,
)


DEFAULT_REGISTRATION_ENV_PATH = ".env"
DEFAULT_NATIVE_API_BASE = "https://api.openlinker.ai"
DEFAULT_NATIVE_SDK_AGENT = "openlinker-python/native"
RUNTIME_CONNECTOR_PULL = "runtime_pull"
RUNTIME_CONNECTOR_WEBSOCKET = "runtime_ws"


class RegistrationStore(Protocol):
    async def load_runtime_agent_registration(self) -> RuntimeAgentRegistration | None: ...

    async def save_runtime_agent_registration(self, reg: RuntimeAgentRegistration) -> None: ...


def first_non_empty(*values: str | None) -> str:
    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


class EnvRegistrationStore:
    def __init__(self, path: str = DEFAULT_REGISTRATION_ENV_PATH) -> None:
        self.path = path.strip() or DEFAULT_REGISTRATION_ENV_PATH

    async def load_runtime_agent_registration(self) -> RuntimeAgentRegistration | None:
        values = _read_env_file(self.path)
        if values is None:
            return None
        reg = RuntimeAgentRegistration(
            agent_id=values.get("OPENLINKER_AGENT_ID"),
            agent_slug=values.get("OPENLINKER_AGENT_SLUG"),
            agent_name=values.get("OPENLINKER_AGENT_NAME"),
            runtime_token=first_non_empty(
                values.get("OPENLINKER_RUNTIME_TOKEN"), values.get("OPENLINKER_AGENT_TOKEN")
            ),
            runtime_token_id=values.get("OPENLINKER_RUNTIME_TOKEN_ID"),
            runtime_prefix=values.get("OPENLINKER_RUNTIME_TOKEN_PREFIX"),
            api_base=values.get("OPENLINKER_API_BASE"),
            connector=values.get("OPENLINKER_WORKER_CONNECTOR"),
            registered_at=_parse_datetime(values.get("OPENLINKER_REGISTERED_AT")),
            updated_at=_parse_datetime(values.get("OPENLINKER_UPDATED_AT")),
        )
        if not reg.agent_id and not reg.runtime_token:
            return None
        return reg

    async def save_runtime_agent_registration(self, reg: RuntimeAgentRegistration) -> None:
        values = _read_env_file(self.path) or {}
        _set_env_value(values, "OPENLINKER_AGENT_ID", reg.agent_id)
        _set_env_value(values, "OPENLINKER_AGENT_SLUG", reg.agent_slug)
        _set_env_value(values, "OPENLINKER_AGENT_NAME", reg.agent_name)
        _set_env_value(values, "OPENLINKER_RUNTIME_TOKEN", reg.runtime_token)
        _set_env_value(values, "OPENLINKER_RUNTIME_TOKEN_ID", reg.runtime_token_id)
        _set_env_value(values, "OPENLINKER_RUNTIME_TOKEN_PREFIX", reg.runtime_prefix)
        _set_env_value(values, "OPENLINKER_API_BASE", reg.api_base)
        _set_env_value(values, "OPENLINKER_WORKER_CONNECTOR", reg.connector)
        if reg.registered_at:
            values["OPENLINKER_REGISTERED_AT"] = _format_datetime(reg.registered_at)
        if reg.updated_at:
            values["OPENLINKER_UPDATED_AT"] = _format_datetime(reg.updated_at)
        _write_env_file(self.path, values)

    LoadRuntimeAgentRegistration = load_runtime_agent_registration
    SaveRuntimeAgentRegistration = save_runtime_agent_registration


def normalize_ensure_runtime_agent_request(req: EnsureRuntimeAgentRequest) -> EnsureRuntimeAgentRequest:
    req.api_base = first_non_empty(req.api_base, os.getenv("OPENLINKER_API_BASE"), DEFAULT_NATIVE_API_BASE)
    req.user_token = first_non_empty(req.user_token, os.getenv("OPENLINKER_USER_TOKEN"))
    req.runtime_token = first_non_empty(
        req.runtime_token, os.getenv("OPENLINKER_RUNTIME_TOKEN"), os.getenv("OPENLINKER_AGENT_TOKEN")
    )
    req.connector = first_non_empty(req.connector, os.getenv("OPENLINKER_WORKER_CONNECTOR"), RUNTIME_CONNECTOR_PULL)
    req.policy = first_non_empty(req.policy, REGISTER_POLICY_REUSE_EXISTING)
    req.visibility = first_non_empty(req.visibility, "private")
    req.connection_mode = first_non_empty(req.connection_mode, req.connector)
    req.token_name = first_non_empty(req.token_name, req.name, req.slug, "native runtime")
    req.token_scopes = req.token_scopes or ["agent:pull", "agent:call"]
    req.tags = req.tags or ["agent", "runtime"]
    req.skill_ids = req.skill_ids or []
    if req.store is None:
        req.store = EnvRegistrationStore(DEFAULT_REGISTRATION_ENV_PATH)
    return req


async def ensure_runtime_agent(req: EnsureRuntimeAgentRequest) -> RuntimeAgentRegistration:
    req = normalize_ensure_runtime_agent_request(req)
    async with Client(req.api_base, user_token=req.user_token, sdk_agent=DEFAULT_NATIVE_SDK_AGENT) as client:
        return await client.ensure_runtime_agent(req)


async def client_ensure_runtime_agent(client: Client, req: EnsureRuntimeAgentRequest) -> RuntimeAgentRegistration:
    req = normalize_ensure_runtime_agent_request(req)
    stored = await _load_registration(req.store)
    if stored is not None:
        req.runtime_token = first_non_empty(req.runtime_token, stored.runtime_token)
        req.api_base = first_non_empty(req.api_base, stored.api_base)
        req.connector = first_non_empty(req.connector, stored.connector)
        req.slug = first_non_empty(req.slug, stored.agent_slug)
        req.name = first_non_empty(req.name, stored.agent_name)

    if (
        req.policy not in (REGISTER_POLICY_ROTATE_TOKEN, REGISTER_POLICY_FORCE_NEW)
        and req.runtime_token
    ):
        reg = await _valid_runtime_registration(client, req, stored)
        if reg is not None:
            return reg
        if req.policy == REGISTER_POLICY_VALIDATE_ONLY:
            raise RuntimeError("openlinker: stored runtime token is invalid")
    if req.policy == REGISTER_POLICY_VALIDATE_ONLY:
        raise RuntimeError("openlinker: no valid runtime token found")

    user_client = client
    if req.user_token and not client.user_token:
        user_client = client.clone(user_token=req.user_token)
    if not user_client.user_token:
        raise RuntimeError("openlinker: OPENLINKER_USER_TOKEN is required to register or rotate a runtime agent")

    agent = await _ensure_agent(user_client, req, stored)
    token = await user_client.create_agent_token(
        CreateAgentTokenRequest(
            name=req.token_name,
            agent_id=agent.id,
            scopes=req.token_scopes or [],
            expires_in_minutes=req.token_expires_in_minutes,
        )
    )
    if not token.plaintext_token:
        raise RuntimeError("openlinker: platform did not return runtime token plaintext")

    now = datetime.now(timezone.utc)
    registered_at = stored.registered_at if stored and stored.registered_at else now
    reg = RuntimeAgentRegistration(
        agent_id=agent.id,
        agent_slug=agent.slug,
        agent_name=agent.name,
        runtime_token=token.plaintext_token,
        runtime_token_id=token.id,
        runtime_prefix=token.prefix,
        api_base=req.api_base,
        connector=req.connector,
        registered_at=registered_at,
        updated_at=now,
    )
    await _save_registration(req.store, reg)
    return reg


async def _valid_runtime_registration(
    client: Client, req: EnsureRuntimeAgentRequest, stored: RuntimeAgentRegistration | None
) -> RuntimeAgentRegistration | None:
    runtime_client = client
    if client.agent_token != req.runtime_token:
        runtime_client = client.clone(agent_token=req.runtime_token)
    try:
        heartbeat = await runtime_client.validate_runtime_token()
    except Exception:
        return None
    now = datetime.now(timezone.utc)
    reg = RuntimeAgentRegistration(
        agent_id=heartbeat.agent_id,
        agent_slug=req.slug or (stored.agent_slug if stored else None),
        agent_name=req.name or (stored.agent_name if stored else None),
        runtime_token=req.runtime_token,
        api_base=req.api_base,
        connector=req.connector,
        updated_at=now,
    )
    if stored is not None:
        reg.runtime_token_id = stored.runtime_token_id
        reg.runtime_prefix = stored.runtime_prefix
        reg.registered_at = stored.registered_at
    if not reg.registered_at:
        reg.registered_at = now
    await _save_registration(req.store, reg)
    return reg


async def _ensure_agent(client: Client, req: EnsureRuntimeAgentRequest, stored: RuntimeAgentRegistration | None):
    if req.policy != REGISTER_POLICY_FORCE_NEW and stored is not None and stored.agent_id:
        try:
            return await client.get_my_agent(stored.agent_id)
        except OpenLinkerError as exc:
            if exc.status_code != 404:
                raise
    if req.policy != REGISTER_POLICY_FORCE_NEW and req.slug:
        try:
            return await client.get_my_agent_by_slug(req.slug)
        except OpenLinkerError as exc:
            if exc.status_code != 404:
                raise
    if not req.slug or not req.name:
        raise RuntimeError("openlinker: agent slug and name are required to create a runtime agent")
    return await client.create_agent(
        CreateAgentRequest(
            slug=req.slug,
            name=req.name,
            description=req.description,
            endpoint_url=req.endpoint_url,
            endpoint_auth_header=req.endpoint_auth_header,
            price_per_call_cents=req.price_per_call_cents,
            tags=req.tags or [],
            skill_ids=req.skill_ids or [],
            visibility=req.visibility,
            connection_mode=req.connection_mode,
            mcp_tool_name=req.mcp_tool_name,
        )
    )


async def _load_registration(store: RegistrationStore | None) -> RuntimeAgentRegistration | None:
    if store is None:
        return None
    return await store.load_runtime_agent_registration()


async def _save_registration(store: RegistrationStore | None, reg: RuntimeAgentRegistration) -> None:
    if store is not None:
        await store.save_runtime_agent_registration(reg)


def _read_env_file(path: str) -> dict[str, str] | None:
    p = Path(path)
    if not p.exists():
        return None
    values: dict[str, str] = {}
    for raw in p.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key:
            try:
                values[key] = shlex.split(value.strip())[0] if value.strip() else ""
            except ValueError:
                values[key] = value.strip().strip("\"'")
    return values


def _write_env_file(path: str, values: dict[str, str]) -> None:
    p = Path(path)
    if p.parent and str(p.parent) != ".":
        p.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    keys = [
        "OPENLINKER_API_BASE",
        "OPENLINKER_AGENT_ID",
        "OPENLINKER_AGENT_SLUG",
        "OPENLINKER_AGENT_NAME",
        "OPENLINKER_RUNTIME_TOKEN",
        "OPENLINKER_RUNTIME_TOKEN_ID",
        "OPENLINKER_RUNTIME_TOKEN_PREFIX",
        "OPENLINKER_WORKER_CONNECTOR",
        "OPENLINKER_REGISTERED_AT",
        "OPENLINKER_UPDATED_AT",
    ]
    known = set(keys)
    seen: set[str] = set()
    out: list[str] = []
    if p.exists():
        for raw in p.read_text().splitlines():
            key = _env_line_key(raw)
            if not key or key not in known:
                out.append(raw)
                continue
            seen.add(key)
            value = values.get(key, "")
            if value:
                out.append(f"{key}={shlex.quote(value)}")
    for key in keys:
        if key in seen:
            continue
        value = values.get(key, "")
        if value:
            out.append(f"{key}={shlex.quote(value)}")
    p.write_text("\n".join(out) + ("\n" if out else ""))
    os.chmod(p, 0o600)


def _env_line_key(line: str) -> str:
    trimmed = line.strip()
    if not trimmed or trimmed.startswith("#"):
        return ""
    if trimmed.startswith("export "):
        trimmed = trimmed.removeprefix("export ").strip()
    if "=" not in trimmed:
        return ""
    return trimmed.split("=", 1)[0].strip()


def _set_env_value(values: dict[str, str], key: str, value: str | None) -> None:
    value = (value or "").strip()
    if value:
        values[key] = value
    else:
        values.pop(key, None)


def _parse_datetime(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_datetime(value) -> str:
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


setattr(Client, "ensure_runtime_agent", client_ensure_runtime_agent)
setattr(Client, "EnsureRuntimeAgent", client_ensure_runtime_agent)

