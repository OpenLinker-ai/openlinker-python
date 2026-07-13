from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import uuid
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from typing import Any, Literal


RUNTIME_PROTOCOL_VERSION = 2
RUNTIME_CONTRACT_ID = "openlinker.runtime.v2"
RUNTIME_CONTRACT_DIGEST = "3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9"
RUNTIME_REQUIRED_FEATURES = (
    "lease_fence",
    "assignment_confirm",
    "renew",
    "resume",
    "event_ack",
    "result_ack",
    "cancel",
    "persistent_spool",
)

RUNTIME_MAX_MESSAGE_BYTES = 4 * 1024 * 1024
RUNTIME_MAX_PULL_WAIT_SECONDS = 30
RUNTIME_MAX_CAPACITY = 1024
RUNTIME_WEBSOCKET_PATH = "/api/v1/agent-runtime/ws"
RUNTIME_CALL_AGENT_PATH = "/api/v1/agent-runtime/call-agent"

RuntimeTransportMode = Literal["auto", "ws", "pull"]

_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")
_CORE_EVENT_TYPES = {"run.completed", "run.failed", "run.canceled", "run.stream.gap"}
_PROOF_DOMAIN = "openlinker/runtime-v2/invocation-proof"
_DETERMINISTIC_DOMAIN = "openlinker/runtime/deterministic-id"


class RuntimeProtocolError(RuntimeError):
    """The peer returned a response that cannot be trusted."""


class RuntimeRemoteError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        status_code: int = 0,
        missing_event_ranges: list[tuple[int, int]] | None = None,
    ) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.retryable = retryable
        self.status_code = status_code
        self.missing_event_ranges = list(missing_event_ranges or [])


class RuntimeStoreError(RuntimeError):
    """Durable Runtime state is unavailable or cannot be authenticated."""


class RuntimeStoreCorrupt(RuntimeStoreError):
    pass


class RuntimeStoreLocked(RuntimeStoreError):
    pass


class RuntimeStoreCapacity(RuntimeStoreError):
    pass


@dataclass(frozen=True)
class RuntimeMTLS:
    cert_file: str
    key_file: str
    ca_file: str
    server_name: str = ""


@dataclass(frozen=True)
class RuntimeAttemptIdentity:
    run_id: str
    attempt_id: str
    lease_id: str
    fencing_token: int
    node_id: str
    agent_id: str
    worker_id: str
    runtime_session_id: str

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> RuntimeAttemptIdentity:
        _require_keys(
            value,
            required={
                "run_id",
                "attempt_id",
                "lease_id",
                "fencing_token",
                "node_id",
                "agent_id",
                "worker_id",
                "runtime_session_id",
            },
            optional=set(),
            name="Runtime Attempt identity",
        )
        try:
            identity = cls(
                run_id=str(value["run_id"]),
                attempt_id=str(value["attempt_id"]),
                lease_id=str(value["lease_id"]),
                fencing_token=int(value["fencing_token"]),
                node_id=str(value["node_id"]),
                agent_id=str(value["agent_id"]),
                worker_id=str(value["worker_id"]),
                runtime_session_id=str(value["runtime_session_id"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeProtocolError("invalid Runtime Attempt identity") from exc
        identity.validate()
        return identity

    def validate(self) -> None:
        for name in (
            "run_id",
            "attempt_id",
            "lease_id",
            "node_id",
            "agent_id",
            "runtime_session_id",
        ):
            _require_uuid(getattr(self, name), name)
        if not self.worker_id or len(self.worker_id) > 200 or self.fencing_token < 1:
            raise RuntimeProtocolError("invalid Runtime Attempt identity")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RuntimeAssignment:
    attempt_identity: RuntimeAttemptIdentity
    offer_no: int
    offer_expires_at: datetime
    attempt_deadline_at: datetime
    run_deadline_at: datetime
    input: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)
    node_envelope: str = ""
    agent_invocation_token: str = ""

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> RuntimeAssignment:
        _require_keys(
            value,
            required={
                "attempt_identity",
                "offer_no",
                "offer_expires_at",
                "attempt_deadline_at",
                "run_deadline_at",
                "input",
                "node_envelope",
                "agent_invocation_token",
            },
            optional={"metadata"},
            name="Runtime assignment",
        )
        try:
            assignment = cls(
                attempt_identity=RuntimeAttemptIdentity.from_dict(value["attempt_identity"]),
                offer_no=int(value["offer_no"]),
                offer_expires_at=parse_datetime(value["offer_expires_at"]),
                attempt_deadline_at=parse_datetime(value["attempt_deadline_at"]),
                run_deadline_at=parse_datetime(value["run_deadline_at"]),
                input=_require_object(value["input"], "assignment input"),
                metadata=_optional_object(value.get("metadata"), "assignment metadata"),
                node_envelope=str(value["node_envelope"]),
                agent_invocation_token=str(value["agent_invocation_token"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeProtocolError("invalid Runtime assignment") from exc
        if assignment.offer_no < 1:
            raise RuntimeProtocolError("invalid Runtime assignment offer")
        _require_capability(assignment.node_envelope, "ol_ctx_v2.")
        _require_capability(assignment.agent_invocation_token, "ol_inv_v2.")
        return assignment


@dataclass(frozen=True)
class RuntimeReady:
    core_instance_id: str
    attachment_id: str
    features: tuple[str, ...]
    offer_ttl_seconds: int
    lease_ttl_seconds: int
    database_time: datetime

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> RuntimeReady:
        _require_keys(
            value,
            required={
                "core_instance_id",
                "attachment_id",
                "features",
                "offer_ttl_seconds",
                "lease_ttl_seconds",
                "database_time",
            },
            optional=set(),
            name="Runtime ready response",
        )
        try:
            ready = cls(
                core_instance_id=str(value["core_instance_id"]),
                attachment_id=str(value["attachment_id"]),
                features=tuple(str(item) for item in value["features"]),
                offer_ttl_seconds=int(value["offer_ttl_seconds"]),
                lease_ttl_seconds=int(value["lease_ttl_seconds"]),
                database_time=parse_datetime(value["database_time"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeProtocolError("invalid Runtime ready response") from exc
        if not ready.core_instance_id or len(ready.core_instance_id) > 200:
            raise RuntimeProtocolError("invalid Runtime Core instance identity")
        _require_uuid(ready.attachment_id, "attachment_id")
        if len(set(ready.features)) != len(ready.features):
            raise RuntimeProtocolError("Runtime ready features must be unique")
        if ready.offer_ttl_seconds < 1 or ready.lease_ttl_seconds < 1:
            raise RuntimeProtocolError("invalid Runtime ready TTL")
        missing = set(RUNTIME_REQUIRED_FEATURES).difference(ready.features)
        if missing:
            raise RuntimeProtocolError(f"Runtime is missing required features: {sorted(missing)}")
        return ready


@dataclass(frozen=True)
class RuntimeEvent:
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if not _EVENT_TYPE.fullmatch(self.event_type) or self.event_type in _CORE_EVENT_TYPES:
            raise ValueError("invalid or Core-reserved Runtime event type")
        _require_object(self.payload, "event payload")


@dataclass(frozen=True)
class RuntimeHandlerError:
    code: str
    message: str


@dataclass(frozen=True)
class RuntimeResult:
    status: Literal["success", "failed"] = "success"
    output: dict[str, Any] | None = field(default_factory=dict)
    events: tuple[RuntimeEvent, ...] = ()
    error: RuntimeHandlerError | None = None
    duration_ms: int = 0

    @classmethod
    def success(
        cls,
        output: dict[str, Any] | None = None,
        *,
        events: tuple[RuntimeEvent, ...] = (),
    ) -> RuntimeResult:
        return cls(status="success", output=output or {}, events=events)

    @classmethod
    def failed(cls, code: str, message: str) -> RuntimeResult:
        return cls(status="failed", output=None, error=RuntimeHandlerError(code, message))


@dataclass(frozen=True)
class RuntimeCallOptions:
    idempotency_key: str
    reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeCommand:
    type: str
    payload: dict[str, Any]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> RuntimeCommand:
        return cls(
            type=str(value.get("type", "")),
            payload=_require_object(value.get("payload"), "command payload"),
        )


def runtime_hello(
    *,
    node_id: str,
    agent_id: str,
    worker_id: str,
    runtime_session_id: str,
    session_epoch: int,
    node_version: str,
    capacity: int,
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "agent_id": agent_id,
        "worker_id": worker_id,
        "runtime_session_id": runtime_session_id,
        "session_epoch": session_epoch,
        "node_version": node_version,
        "capacity": capacity,
        "features": list(RUNTIME_REQUIRED_FEATURES),
        "contract_digest": RUNTIME_CONTRACT_DIGEST,
    }


def wire_value(value: Any) -> Any:
    if is_dataclass(value):
        return wire_value(asdict(value))
    if isinstance(value, datetime):
        return format_datetime(value)
    if isinstance(value, tuple):
        return [wire_value(item) for item in value]
    if isinstance(value, list):
        return [wire_value(item) for item in value]
    if isinstance(value, dict):
        return {key: wire_value(item) for key, item in value.items() if item is not None}
    return value


def wire_json_bytes(value: Any) -> bytes:
    try:
        raw = json.dumps(wire_value(value), ensure_ascii=False, separators=(",", ":")).encode()
    except (TypeError, ValueError) as exc:
        raise ValueError("Runtime value is not JSON encodable") from exc
    if len(raw) > RUNTIME_MAX_MESSAGE_BYTES:
        raise ValueError("Runtime message exceeds 4 MiB")
    return raw


def build_invocation_proof(
    token: str,
    *,
    body: bytes,
    context: str,
    idempotency_key: str,
) -> str:
    _require_capability(token, "ol_inv_v2.")
    _require_capability(context, "ol_ctx_v2.")
    validate_idempotency_key(idempotency_key)
    canonical = {
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "context": context,
        "idempotency_key": idempotency_key,
        "method": "POST",
        "path": RUNTIME_CALL_AGENT_PATH,
        "version": _PROOF_DOMAIN,
    }
    canonical_bytes = json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    key = hashlib.sha256((_PROOF_DOMAIN + "\x00" + token).encode()).digest()
    digest = hmac.new(key, canonical_bytes, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def validate_idempotency_key(value: str) -> None:
    if not value or len(value) > 255 or value != value.strip():
        raise ValueError("idempotency_key must contain 1 to 255 printable ASCII bytes")
    if any(ord(char) < 0x20 or ord(char) > 0x7E for char in value):
        raise ValueError("idempotency_key must contain 1 to 255 printable ASCII bytes")


def deterministic_uuid(*parts: str) -> str:
    value = _DETERMINISTIC_DOMAIN + "".join("\x00" + part for part in parts)
    digest = bytearray(hashlib.sha256(value.encode()).digest()[:16])
    digest[6] = (digest[6] & 0x0F) | 0x50
    digest[8] = (digest[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(digest)))


def parse_datetime(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("timestamp is required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def format_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _require_uuid(value: str, name: str) -> None:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as exc:
        raise RuntimeProtocolError(f"{name} must be a UUID") from exc
    if str(parsed) != value or parsed.int == 0:
        raise RuntimeProtocolError(f"{name} must be a lowercase non-zero UUID")


def _require_object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeProtocolError(f"{name} must be a JSON object")
    return dict(value)


def _optional_object(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    return _require_object(value, name)


def _require_keys(
    value: dict[str, Any],
    *,
    required: set[str],
    optional: set[str],
    name: str,
) -> None:
    if not isinstance(value, dict):
        raise RuntimeProtocolError(f"{name} must be a JSON object")
    keys = set(value)
    missing = required - keys
    unknown = keys - required - optional
    if missing or unknown:
        raise RuntimeProtocolError(f"{name} has missing or unknown fields")


def _require_capability(value: str, prefix: str) -> None:
    if (
        not value
        or value != value.strip()
        or len(value) > 8192
        or not value.startswith(prefix)
        or len(value.split(".")) != 4
        or any(not part for part in value.split("."))
    ):
        raise RuntimeProtocolError("invalid Runtime invocation capability")


__all__ = [
    "RUNTIME_CONTRACT_DIGEST",
    "RUNTIME_CONTRACT_ID",
    "RUNTIME_PROTOCOL_VERSION",
    "RUNTIME_REQUIRED_FEATURES",
    "RuntimeAssignment",
    "RuntimeAttemptIdentity",
    "RuntimeCallOptions",
    "RuntimeCommand",
    "RuntimeEvent",
    "RuntimeHandlerError",
    "RuntimeMTLS",
    "RuntimeProtocolError",
    "RuntimeReady",
    "RuntimeRemoteError",
    "RuntimeResult",
    "RuntimeStoreCapacity",
    "RuntimeStoreCorrupt",
    "RuntimeStoreError",
    "RuntimeStoreLocked",
    "RuntimeTransportMode",
]
