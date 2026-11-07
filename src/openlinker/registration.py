from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from .client import Client
from .error import OpenLinkerError
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
# Holds the in-progress first registration beside the completed keys in the
# same private env file, so it shares their location, 0600 mode and ignore
# rules; loaders that only read the completed keys skip it.
_PENDING_REGISTRATION_ENV_KEY = "OPENLINKER_PENDING_REGISTRATION"
_PENDING_REGISTRATION_VERSION = 1
# Keeps the record well below the line limit of other SDKs' env readers. A
# request within Core's field limits needs a few KiB; a larger one is sent
# without a record, as before.
_MAX_PENDING_REGISTRATION_BYTES = 16 * 1024
# Test seam, called with a stage name inside a pending-record write and
# removal: "write.reread" and "cleanup.reread" run between the read the change
# is based on and the guarding re-read; "write.rename" and "cleanup.mutate" run
# after that guard, just before the replace or unlink.
_pending_test_hook: Callable[[str], None] | None = None
_PENDING_REGISTRATION_WRITE_ATTEMPTS = 3
# One lock per absolute env file path. Every mutation of a registration env
# file (completed save, pending-record write and pending-record removal) holds
# it from its read through its final replace or unlink, so store instances for
# the same file in this process cannot interleave. Processes are not
# coordinated; that remains the pre-existing limitation of sharing one env
# file between concurrent registrations.
_ENV_PATH_LOCKS: dict[str, threading.Lock] = {}
_ENV_PATH_LOCKS_GUARD = threading.Lock()


def _env_path_lock(path: Path) -> threading.Lock:
    """The lock for ``path``; callers take their store's own lock first."""
    key = os.path.abspath(path)
    with _ENV_PATH_LOCKS_GUARD:
        return _ENV_PATH_LOCKS.setdefault(key, threading.Lock())


def _run_pending_test_hook(stage: str) -> None:
    if _pending_test_hook is not None:
        _pending_test_hook(stage)


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


@dataclass
class _PendingMint:
    """The normalized token request and a non-secret fingerprint of the minting creator."""

    token_name: str
    token_scopes: list[str]
    token_expires_in_minutes: int
    creator_fingerprint: str


@dataclass
class _PendingRegistration:
    """A first registration whose pending Agent Token may already be redeemed.

    The endpoint auth header is never written, because that credential was not
    stored before; only its fingerprint is, and a retry takes the header from
    the current request. ``created_file``
    marks an env file the record itself created, so clearing the record removes
    an otherwise empty file.
    """

    api_base: str
    agent_token: str
    request: RegisterAgentViaTokenRequest
    token_id: str = ""
    token_prefix: str = ""
    # Stands in for the endpoint auth header, which is never written; see
    # _endpoint_auth_fingerprint. Empty when a record lacks it.
    endpoint_auth_fingerprint: str = ""
    # Set when this SDK minted the token; a minting start replays only a record
    # whose mint parameters and creator match its own.
    mint: _PendingMint | None = None
    created_file: bool = False


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
        with self._lock, _env_path_lock(self.path):
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

    # The pending-registration journal is private to this store, so custom
    # RegistrationStore implementations keep their previous behaviour.
    def _load_pending_registration(self) -> _PendingRegistration | None:
        with self._lock:
            try:
                values = _read_env(self.path)
            except FileNotFoundError:
                return None
        return _decode_pending_registration(values.get(_PENDING_REGISTRATION_ENV_KEY))

    def _save_pending_registration(self, record: _PendingRegistration) -> bool:
        """Write the record without losing data another writer stores meanwhile.

        In this process it holds the env file's path lock from its read through
        the replace, like every other mutation of that file. Against other
        processes it renders from one read, re-reads right before the replace
        and writes only if the file is unchanged, otherwise it renders again
        from the fresh content, a bounded number of times. Returns False when
        the record was not written (still contended, or too large), so the
        attempt continues without one, exactly as before the journal existed.
        There is no cross-process lock: another process's write between the
        final re-read and the replace can still be lost, the same pre-existing
        limitation as concurrent registrations against one env file. The
        directory is synced after the replace, so the record is durable before
        the token is redeemed.
        """
        request = {
            key: value
            for key, value in record.request.to_dict().items()
            if key in _PENDING_REQUEST_FIELDS
        }
        with self._lock, _env_path_lock(self.path):
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            for _ in range(_PENDING_REGISTRATION_WRITE_ATTEMPTS):
                before = _read_env_bytes(self.path)
                content = before.decode("utf-8") if before is not None else ""
                values = _parse_env(content)
                previous = _decode_pending_registration(
                    values.get(_PENDING_REGISTRATION_ENV_KEY)
                )
                encoded: dict[str, Any] = {
                    "version": _PENDING_REGISTRATION_VERSION,
                    "api_base": record.api_base,
                    "agent_token": record.agent_token,
                    "request": request,
                    "endpoint_auth_fingerprint": _endpoint_auth_fingerprint(
                        record.request.endpoint_auth_header
                    ),
                }
                if record.token_id:
                    encoded["token_id"] = record.token_id
                if record.token_prefix:
                    encoded["token_prefix"] = record.token_prefix
                if record.mint is not None:
                    encoded["mint"] = {
                        "token_name": record.mint.token_name,
                        "token_scopes": list(record.mint.token_scopes),
                        "token_expires_in_minutes": record.mint.token_expires_in_minutes,
                        "creator_fingerprint": record.mint.creator_fingerprint,
                    }
                if before is None or (previous is not None and previous.created_file):
                    encoded["created_file"] = True
                raw = json.dumps(encoded, ensure_ascii=False, separators=(",", ":"))
                if len(raw.encode("utf-8")) > _MAX_PENDING_REGISTRATION_BYTES:
                    return False
                values[_PENDING_REGISTRATION_ENV_KEY] = raw
                rendered = _render_env(
                    content.splitlines(), values, (_PENDING_REGISTRATION_ENV_KEY,)
                )
                _run_pending_test_hook("write.reread")

                def unchanged(before: bytes | None = before) -> bool:
                    if _read_env_bytes(self.path) != before:
                        return False
                    _run_pending_test_hook("write.rename")
                    return True

                if _replace_env_file(self.path, rendered, unchanged, sync_directory=True):
                    return True
            return False

    def _clear_pending_registration(self, agent_token: str) -> None:
        """Remove this attempt's record and never another writer's data.

        It decides from one read of the file, then re-reads it immediately
        before mutating and leaves everything untouched if anything changed: a
        newer record, a completed registration or any other line written
        meanwhile survives, and only this attempt's leftover record may remain.

        - The whole file is deleted only if this record created it and the file
          holds nothing but this attempt's own pending key.
        - Otherwise only the pending key is removed, and only while it still
          holds ``agent_token``; completed keys and other lines are kept.

        In this process the env file's path lock is held from the first read
        through the replace or unlink, like every other mutation of that file.
        There is no cross-process lock: another process can still write between
        the re-read and the replace or unlink, the same pre-existing limitation
        as concurrent registrations against one env file.
        """
        with self._lock, _env_path_lock(self.path):
            try:
                before = self.path.read_bytes()
            except FileNotFoundError:
                return
            content = before.decode("utf-8")
            record = _decode_pending_registration(
                _parse_env(content).get(_PENDING_REGISTRATION_ENV_KEY)
            )
            if record is None or record.agent_token != agent_token.strip():
                return
            delete_file = record.created_file and _env_has_only_key(
                content, _PENDING_REGISTRATION_ENV_KEY
            )
            _run_pending_test_hook("cleanup.reread")

            def unchanged() -> bool:
                if _read_env_bytes(self.path) != before:
                    return False
                _run_pending_test_hook("cleanup.mutate")
                return True

            if delete_file:
                if unchanged():
                    with suppress(FileNotFoundError):
                        self.path.unlink()
                    _fsync_directory(self.path.parent)
                return
            _replace_env_file(
                self.path,
                _render_env(content.splitlines(), {}, (_PENDING_REGISTRATION_ENV_KEY,)),
                unchanged,
                sync_directory=True,
            )


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
            return await _redeem_new_registration(
                sdk, store, req, _pending_journal(store), req.agent_token
            )

        if not req.user_token and owns_client:
            raise ValueError(
                "openlinker: OPENLINKER_USER_TOKEN is required to create an Agent"
            )

        if not req.slug or not req.name:
            raise ValueError("openlinker: Agent slug and name are required to create an Agent")

        async def mint() -> tuple[str, str, str]:
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
            return pending.plaintext_token, pending.id, pending.prefix

        # Only a first registration is journaled; creating an Agent over an
        # existing stored registration keeps its previous behaviour.
        journal = _pending_journal(store) if stored is None else None
        return await _redeem_new_registration(sdk, store, req, journal, mint)
    finally:
        if owns_client:
            await sdk.aclose()


def _pending_journal(store: RegistrationStore) -> EnvRegistrationStore | None:
    """Journal only when the store's class is exactly EnvRegistrationStore.

    Subclasses (for example ones that encrypt or relocate registrations) and
    custom stores keep their previous behaviour, so no token is written anywhere
    except through their own save.
    """
    return store if type(store) is EnvRegistrationStore else None


async def _redeem_new_registration(
    sdk: Client,
    store: RegistrationStore,
    req: EnsureAgentRequest,
    journal: EnvRegistrationStore | None,
    token: str | Callable[[], Awaitable[tuple[str, str, str]]],
) -> AgentRegistration:
    """Redeem a pending Agent Token for a new Agent and save the result.

    With a journal, the token and request are recorded before Core is asked to
    redeem them. If the process stops, the response is lost or the save fails
    after Core committed the registration, the next start sends the same token
    and request again instead of minting a new token; Core answers that exact
    replay with the original registration. ``token`` is either the caller's
    pending token or a coroutine function that mints one for a fresh attempt.
    """
    request = _registration_request(req)
    api_base = sdk.base_url
    mint: _PendingMint | None = None
    if journal is not None and not isinstance(token, str):
        credential = _minting_credential(sdk)
        if credential is None:
            # The creator the mint request names cannot be told reliably, so
            # this start writes no record and runs exactly as before.
            journal = None
        else:
            mint = _PendingMint(
                token_name=req.token_name,
                token_scopes=list(req.token_scopes),
                token_expires_in_minutes=req.token_expires_in_minutes,
                creator_fingerprint=_creator_fingerprint(credential),
            )
    # Only a minting start replays a record. With a supplied token, an exactly
    # matching record holds that same token and request, which is what the flow
    # below sends anyway, so replaying it first would only repeat it.
    if journal is not None and mint is not None:
        previous = journal._load_pending_registration()
        # A record that differs in any way (platform, request, mint parameters
        # or creator) is never replayed; it stays until the attempt below
        # replaces it.
        if previous is not None and _pending_registration_matches(
            previous, api_base, request, mint
        ):
            previous.request.endpoint_auth_header = request.endpoint_auth_header
            try:
                return await _redeem_pending_registration(sdk, store, req, journal, previous)
            except OpenLinkerError as error:
                if not _is_definitive_rejection(error):
                    raise
                # Core refused the recorded attempt outright (for example its
                # token expired unused) and its record is gone. Continue with the
                # normal flow instead of re-raising: without the record this start
                # would have minted a fresh token, and a stale record must not
                # turn a start that succeeds today into a failure.
    if isinstance(token, str):
        agent_token, token_id, token_prefix = token, "", ""
    else:
        agent_token, token_id, token_prefix = await token()
    attempt = _PendingRegistration(
        api_base=api_base,
        agent_token=agent_token,
        request=request,
        token_id=token_id or "",
        token_prefix=token_prefix or "",
        mint=mint,
    )
    if journal is not None:
        if not journal._save_pending_registration(attempt):
            journal = None
    return await _redeem_pending_registration(sdk, store, req, journal, attempt)


async def _redeem_pending_registration(
    sdk: Client,
    store: RegistrationStore,
    req: EnsureAgentRequest,
    journal: EnvRegistrationStore | None,
    attempt: _PendingRegistration,
) -> AgentRegistration:
    try:
        registered = await sdk.register_agent_via_token(attempt.agent_token, attempt.request)
    except OpenLinkerError as error:
        # Core refuses the same token and request the same way again, so a
        # definitive rejection drops this attempt's record and the next start
        # behaves as before. Any other failure (network, timeout, 408, 429, 5xx
        # or an unreadable success) may follow a committed registration, so the
        # record stays for the next start to replay.
        if journal is not None and _is_definitive_rejection(error):
            with suppress(OSError, ValueError):
                journal._clear_pending_registration(attempt.agent_token)
        raise
    registration = _registered_response(req, attempt.agent_token, registered)
    # If this save fails the record stays, so the next start can replay.
    store.save_agent_registration(registration)
    if journal is not None:
        # The saved registration takes precedence on every later start, so a
        # record left by a failed cleanup is not consulted again.
        with suppress(OSError, ValueError):
            journal._clear_pending_registration(attempt.agent_token)
    return registration


def _pending_registration_matches(
    record: _PendingRegistration,
    api_base: str,
    request: RegisterAgentViaTokenRequest,
    mint: _PendingMint,
) -> bool:
    """Whether a record is exactly the attempt a minting start would make.

    Same platform, every recorded request field equal, the same endpoint auth
    header (by fingerprint), and the same mint parameters and creator. A
    record without mint parameters or an endpoint auth fingerprint never
    matches.
    """
    return (
        record.api_base == api_base
        and bool(record.endpoint_auth_fingerprint)
        and record.endpoint_auth_fingerprint
        == _endpoint_auth_fingerprint(request.endpoint_auth_header)
        and record.mint is not None
        and bool(mint.creator_fingerprint)
        and _comparable_request(record.request) == _comparable_request(request)
        and record.mint == mint
    )


def _endpoint_auth_fingerprint(header: str | None) -> str:
    """Identify the exact endpoint auth header bytes without storing the credential.

    Lowercase hex SHA-256 truncated to 32 characters, or "none" when there is
    no header.
    """
    if not header:
        return "none"
    return hashlib.sha256(header.encode("utf-8")).hexdigest()[:32]


def _minting_credential(sdk: Client) -> str | None:
    """The bearer credential the mint request sends, or None if it is uncertain.

    A client-level ``Authorization`` header replaces the User Token on the
    wire, so this reads the headers the client builds for that request. The
    creator cannot be told reliably with no Authorization header, with several
    that differ only in case, or when the httpx client has an ``auth`` object
    or request event hooks, which can replace the credential when it is sent.
    """
    build = getattr(sdk, "_request_headers", None)
    http_client = getattr(sdk, "_client", None)
    if build is None or http_client is None:
        return None
    if getattr(http_client, "auth", None) is not None:
        return None
    event_hooks = getattr(http_client, "event_hooks", None)
    if not isinstance(event_hooks, dict) or event_hooks.get("request"):
        return None
    headers = build("application/json", getattr(sdk, "user_token", "") or "", True)
    values = [value for key, value in headers.items() if key.lower() == "authorization"]
    if len(values) != 1:
        return None
    value = values[0].strip()
    if value[:7].lower() == "bearer ":
        value = value[7:].strip()
    return value or None


def _creator_fingerprint(credential: str) -> str:
    """Lowercase hex SHA-256 of the creator credential, truncated to 32 characters.

    It identifies the creator without storing the credential.
    """
    token = credential.strip()
    if not token:
        return ""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:32]


def _comparable_request(request: RegisterAgentViaTokenRequest) -> tuple[Any, ...]:
    # An absent field equals its empty value, as the other SDKs' records omit them.
    return (
        request.slug or "",
        request.name,
        request.description or "",
        request.endpoint_url or "",
        request.price_per_call_cents or 0,
        list(request.tags or []),
        list(request.ability_tags or []),
        list(request.skill_ids or []),
        request.visibility or "",
        request.connection_mode or "",
        request.mcp_tool_name or "",
    )


def _is_definitive_rejection(error: OpenLinkerError) -> bool:
    """A 4xx answer Core would repeat for the same token and request; 408 and 429 are retryable."""
    return 400 <= error.status_code < 500 and error.status_code not in (408, 429)


_PENDING_REQUEST_FIELDS = {
    f.name
    for f in fields(RegisterAgentViaTokenRequest)
    if f.name not in {"extra", "endpoint_auth_header"}
}
_PENDING_REQUEST_TEXT = {
    "slug",
    "description",
    "endpoint_url",
    "visibility",
    "connection_mode",
    "mcp_tool_name",
}
_PENDING_REQUEST_LISTS = {"tags", "ability_tags", "skill_ids"}


def _decode_pending_registration(raw: str | None) -> _PendingRegistration | None:
    """Ignore a damaged or unknown-version record instead of blocking start-up.

    The next attempt replaces it.
    """
    if not raw or not raw.strip():
        return None
    try:
        decoded: Any = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(decoded, dict) or decoded.get("version") != _PENDING_REGISTRATION_VERSION:
        return None
    agent_token = decoded.get("agent_token")
    api_base = decoded.get("api_base")
    request = _pending_request_from_json(decoded.get("request"))
    if (
        not isinstance(agent_token, str)
        or not agent_token.strip()
        or not isinstance(api_base, str)
        or not api_base
        or request is None
    ):
        return None
    token_id = decoded.get("token_id")
    token_prefix = decoded.get("token_prefix")
    endpoint_fingerprint = decoded.get("endpoint_auth_fingerprint")
    return _PendingRegistration(
        mint=_pending_mint_from_json(decoded.get("mint")),
        endpoint_auth_fingerprint=endpoint_fingerprint
        if isinstance(endpoint_fingerprint, str)
        else "",
        api_base=api_base,
        agent_token=agent_token.strip(),
        request=request,
        token_id=token_id if isinstance(token_id, str) else "",
        token_prefix=token_prefix if isinstance(token_prefix, str) else "",
        created_file=decoded.get("created_file") is True,
    )


def _pending_mint_from_json(value: Any) -> _PendingMint | None:
    """A malformed mint section counts as absent, so the record never replays."""
    if not isinstance(value, dict):
        return None
    name = value.get("token_name")
    scopes = value.get("token_scopes")
    expires = value.get("token_expires_in_minutes")
    creator = value.get("creator_fingerprint")
    if (
        not isinstance(name, str)
        or not isinstance(scopes, list)
        or not all(isinstance(scope, str) for scope in scopes)
        or isinstance(expires, bool)
        or not isinstance(expires, int)
        or not isinstance(creator, str)
    ):
        return None
    return _PendingMint(
        token_name=name,
        token_scopes=list(scopes),
        token_expires_in_minutes=expires,
        creator_fingerprint=creator,
    )


def _pending_request_from_json(value: Any) -> RegisterAgentViaTokenRequest | None:
    if not isinstance(value, dict) or not isinstance(value.get("name"), str):
        return None
    data = {key: item for key, item in value.items() if key in _PENDING_REQUEST_FIELDS}
    for key in _PENDING_REQUEST_TEXT:
        if key in data and not isinstance(data[key], str):
            return None
    for key in _PENDING_REQUEST_LISTS:
        if key in data and (
            not isinstance(data[key], list)
            or not all(isinstance(item, str) for item in data[key])
        ):
            return None
    price = data.get("price_per_call_cents", 0)
    if isinstance(price, bool) or not isinstance(price, int):
        return None
    return RegisterAgentViaTokenRequest.from_dict(data)


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
    return _parse_env(path.read_text(encoding="utf-8"))


def _parse_env(content: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in content.splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[7:].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = _unquote(value.strip())
    return values


def _write_env(
    path: Path,
    values: dict[str, str],
    keys: tuple[str, ...] = _ENV_KEYS,
) -> None:
    """Rewrite only the given keys and keep every other line as it is."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        existing = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        existing = []
    _replace_env_file(path, _render_env(existing, values, keys))


def _render_env(existing: list[str], values: dict[str, str], keys: tuple[str, ...]) -> str:
    managed = set(keys)
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
    for key in keys:
        if key not in seen and values.get(key):
            output.append(f"{key}={json.dumps(values[key])}")
    return "\n".join(output) + ("\n" if output else "")


def _read_env_bytes(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None


def _fsync_directory(directory: Path) -> None:
    """Flush a directory's entries so a replace or unlink inside it is durable.

    Best effort: platforms that cannot sync a directory, such as Windows, skip it.
    """
    if os.name == "nt":
        return
    try:
        descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _replace_env_file(
    path: Path,
    content: str,
    before_replace: Callable[[], bool] | None = None,
    *,
    sync_directory: bool = False,
) -> bool:
    """Atomically replace ``path`` with ``content`` in mode 0600.

    Returns whether it did; when ``before_replace`` returns False, ``path`` is
    left untouched. With ``sync_directory`` the directory is synced afterwards.
    """
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
        if before_replace is not None and not before_replace():
            return False
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        if sync_directory:
            _fsync_directory(path.parent)
        return True
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _env_has_only_key(content: str, key: str) -> bool:
    for raw in content.splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[7:].strip()
        if not line:
            continue
        if "=" not in line or line.split("=", 1)[0].strip() != key:
            return False
    return True


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
