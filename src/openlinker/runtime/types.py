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
RUNTIME_CONTRACT_DIGEST = "4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481"
RUNTIME_REQUIRED_FEATURES = (
    "lease_fence",
    "assignment_confirm",
    "renew",
    "resume",
    "event_ack",
    "result_ack",
    "cancel",
    "persistent_spool",
    "session_drain",
)

RUNTIME_MAX_MESSAGE_BYTES = 4 * 1024 * 1024
RUNTIME_MAX_PULL_WAIT_SECONDS = 30
RUNTIME_MAX_CAPACITY = 1024
RUNTIME_WEBSOCKET_PATH = "/api/v1/agent-runtime/ws"
RUNTIME_CALL_AGENT_PATH = "/api/v1/agent-runtime/call-agent"
RUNTIME_DELEGATED_RUN_READ_PATH = "/api/v1/agent-runtime/delegated-runs/read"
RUNTIME_DELEGATED_RUN_READ_FEATURE = "delegated_run_read.v1"

RuntimeTransportMode = Literal["auto", "ws", "pull"]

_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")
_CORE_EVENT_TYPES = {"run.completed", "run.failed", "run.canceled", "run.stream.gap"}
_PROOF_DOMAIN = "openlinker/runtime-v2/invocation-proof"
_DETERMINISTIC_DOMAIN = "openlinker/runtime/deterministic-id"


class RuntimeProtocolError(RuntimeError):
    """The peer returned a response that cannot be trusted."""


class RuntimeDelegationUnsupportedError(RuntimeError):
    code = "RUNTIME_DELEGATION_UNSUPPORTED"

    def __init__(self) -> None:
        super().__init__("Core/SDK did not negotiate delegated Run results")


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
class RuntimeSpoolStatus:
    """Durable records that must be ACKed before a Worker can exit safely."""

    assignments: int
    events: int
    results: int

    @property
    def empty(self) -> bool:
        return self.assignments == 0 and self.events == 0 and self.results == 0


class RuntimeDrainTimeoutError(RuntimeError):
    """A drain failed closed while durable Runtime work was still pending."""

    code = "RUNTIME_DRAIN_TIMEOUT"

    def __init__(self, timeout: float, spool: RuntimeSpoolStatus) -> None:
        self.timeout = timeout
        self.spool = spool
        super().__init__(
            "OpenLinker Runtime Worker drain timed out after "
            f"{timeout:g}s with {spool.assignments} assignment(s), "
            f"{spool.events} Event(s), and {spool.results} Result(s) still durable"
        )


@dataclass(frozen=True)
class RuntimeMTLS:
    cert_file: str = ""
    key_file: str = ""
    ca_file: str = ""
    server_name: str = ""


@dataclass(frozen=True)
class RuntimeAuthority:
    principal_scope_id: str
    runtime_session_id: str
    runtime_session_epoch: int
    runtime_attachment_id: str
    execution_profile: str = ""
    browser_interaction_policy: str = ""
    browser_interaction_policy_generation: int = 0
    browser_mutation_origins: tuple[str, ...] = ()
    browser_mutation_origins_sha256: str = ""


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
    optional_features: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "agent_id": agent_id,
        "worker_id": worker_id,
        "runtime_session_id": runtime_session_id,
        "session_epoch": session_epoch,
        "node_version": node_version,
        "capacity": capacity,
        "features": [*RUNTIME_REQUIRED_FEATURES, *normalize_runtime_optional_features(optional_features)],
        "contract_digest": RUNTIME_CONTRACT_DIGEST,
    }


def normalize_runtime_optional_features(features: Any) -> tuple[str, ...]:
    if not isinstance(features, (list, tuple)):
        raise ValueError("Runtime optional features must be a sequence")
    seen = set(RUNTIME_REQUIRED_FEATURES)
    for feature in features:
        if not isinstance(feature, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,99}", feature):
            raise ValueError("Runtime optional feature is invalid")
        if feature in seen:
            raise ValueError("Runtime optional features must be unique and distinct from required features")
        seen.add(feature)
    return tuple(sorted(features))


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
    path: str = RUNTIME_CALL_AGENT_PATH,
) -> str:
    _require_capability(token, "ol_inv_v2.")
    _require_capability(context, "ol_ctx_v2.")
    validate_idempotency_key(idempotency_key)
    if not isinstance(path, str) or not path.startswith("/") or path != path.strip():
        raise ValueError("invocation proof path must be an absolute path")
    canonical = {
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "context": context,
        "idempotency_key": idempotency_key,
        "method": "POST",
        "path": path,
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


def runtime_delegation_read_advertised(token: str) -> bool:
    """Feature detection only; Core validates signatures and live Attempt/child ownership."""
    try:
        _require_capability(token, "ol_inv_v2.")
        payload = token.split(".")[2]
        if not re.fullmatch(r"[A-Za-z0-9_-]+", payload):
            return False
        claim = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return isinstance(claim, dict) and claim.get("audience") == "openlinker.runtime.v2/delegation"
    except (ValueError, TypeError, AttributeError, RuntimeProtocolError):
        return False


def validate_runtime_run_summary(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"run_id", "status", "dispatch_state"}:
        raise RuntimeProtocolError("delegated Agent response fields do not match the contract")
    _require_uuid(value["run_id"], "delegated run_id")
    allowed = {
        "running": {"pending", "offered", "executing", "retry_wait"},
        "success": {"terminal"}, "failed": {"terminal", "dead_letter"},
        "timeout": {"terminal"}, "canceled": {"terminal"},
    }
    status, dispatch = value["status"], value["dispatch_state"]
    if not isinstance(status, str) or not isinstance(dispatch, str) or dispatch not in allowed.get(status, set()):
        raise RuntimeProtocolError("delegated Agent response has inconsistent state")
    return dict(value)


def validate_delegated_run(value: Any, run_id: str) -> dict[str, Any]:
    required = {"run_id", "status", "dispatch_state"}
    if not isinstance(value, dict) or not required.issubset(value) or not set(value).issubset(
        required | {"output", "error_code", "error_message"}
    ):
        raise RuntimeProtocolError("delegated Run response fields do not match the contract")
    validate_runtime_run_summary({key: value[key] for key in required})
    if value["run_id"] != run_id:
        raise RuntimeProtocolError("delegated Run ID mismatch")
    if "output" in value and not isinstance(value["output"], dict):
        raise RuntimeProtocolError("delegated Run output must be a JSON object")
    for key in ("error_code", "error_message"):
        if key in value and not isinstance(value[key], str):
            raise RuntimeProtocolError(f"delegated Run {key} must be a string")
    return dict(value)


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


def validate_runtime_drain_payload(value: Any) -> dict[str, Any]:
    """Validate the exact bidirectional ``runtime.drain`` wire payload."""

    if not isinstance(value, dict):
        raise RuntimeProtocolError("Runtime drain payload must be a JSON object")
    _require_keys(
        value,
        required={"deadline_at", "reason_code", "capacity", "inflight"},
        optional=set(),
        name="Runtime drain payload",
    )
    try:
        parse_datetime(value["deadline_at"])
    except (TypeError, ValueError) as exc:
        raise RuntimeProtocolError("Runtime drain deadline is invalid") from exc
    reason_code = value["reason_code"]
    if not isinstance(reason_code, str) or not 1 <= len(reason_code) <= 120:
        raise RuntimeProtocolError("Runtime drain reason is invalid")
    capacity = value["capacity"]
    if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity != 0:
        raise RuntimeProtocolError("Runtime drain capacity is invalid")
    inflight = value["inflight"]
    if not isinstance(inflight, int) or isinstance(inflight, bool) or inflight < 0:
        raise RuntimeProtocolError("Runtime drain inflight is invalid")
    return dict(value)


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
    "RuntimeDrainTimeoutError",
    "RuntimeEvent",
    "RuntimeHandlerError",
    "RuntimeMTLS",
    "RuntimeProtocolError",
    "RuntimeReady",
    "RuntimeRemoteError",
    "RuntimeResult",
    "RuntimeSpoolStatus",
    "RuntimeStoreCapacity",
    "RuntimeStoreCorrupt",
    "RuntimeStoreError",
    "RuntimeStoreLocked",
    "RuntimeTransportMode",
]
