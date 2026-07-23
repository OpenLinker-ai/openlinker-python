from __future__ import annotations

import asyncio
import inspect
import json
import re
import ssl
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import quote, urlencode, urlparse, urlunparse

import httpx
import websockets

from .types import (
    RUNTIME_CONTRACT_ID,
    RUNTIME_MAX_MESSAGE_BYTES,
    RUNTIME_PROTOCOL_VERSION,
    RUNTIME_WEBSOCKET_PATH,
    RuntimeAssignment,
    RuntimeMTLS,
    RuntimeProtocolError,
    RuntimeReady,
    RuntimeRemoteError,
    build_invocation_proof,
    format_datetime,
    parse_datetime,
    validate_runtime_drain_payload,
    wire_json_bytes,
)


_DISCOVERY_PATH = "/.well-known/openlinker.json"
_SDK_AGENT = "openlinker-python/runtime-worker"
_ATTACHMENT_HEADER = "OpenLinker-Runtime-Attachment"
RUNTIME_FALLBACK_REASON_HEADER = "OpenLinker-Runtime-Fallback-Reason"
RUNTIME_NODE_ID_HEADER = "OpenLinker-Runtime-Node"
RUNTIME_FALLBACK_REASONS = frozenset(
    {"explicit", "websocket_unavailable", "policy_forced", "recovery"}
)
_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,119}$")


@dataclass(frozen=True)
class ClaimedAssignment:
    assignment: RuntimeAssignment
    delivery_id: str = ""


@dataclass(frozen=True)
class RuntimeTransportPolicy:
    allowed_transports: tuple[str, ...] = ("ws", "pull")
    default_transport: str = "auto"
    heartbeat_interval: float | None = None
    session_stale_after: float | None = None
    retry_minimum: float | None = None
    retry_maximum: float | None = None
    websocket_probe_interval: float | None = None
    websocket_probe_timeout: float | None = None


@dataclass(frozen=True)
class RuntimeDiscoveryConnection:
    runtime_origin: str
    policy: RuntimeTransportPolicy
    mtls_required: bool = True
    credential_endpoint: str = ""
    trust_bundle_endpoint: str = ""


class RuntimeTransport(Protocol):
    kind: str

    async def create_session(
        self, hello: dict[str, Any], *, fallback_reason: str = ""
    ) -> RuntimeReady: ...

    async def heartbeat_session(self, hello: dict[str, Any]) -> RuntimeReady: ...

    async def close_session(self, request: dict[str, Any]) -> None: ...

    async def claim_assignment(
        self, wait_seconds: int, request: dict[str, Any]
    ) -> ClaimedAssignment | None: ...

    async def ack_assignment(
        self, request: dict[str, Any], *, delivery_id: str = ""
    ) -> dict[str, Any]: ...

    async def reject_assignment(
        self, request: dict[str, Any], *, delivery_id: str = ""
    ) -> dict[str, Any]: ...

    async def renew_lease(self, request: dict[str, Any]) -> dict[str, Any]: ...

    async def send_event(self, request: dict[str, Any]) -> dict[str, Any]: ...

    async def send_result(self, request: dict[str, Any]) -> dict[str, Any]: ...

    async def resume(self, request: dict[str, Any]) -> list[dict[str, Any]]: ...

    async def poll_commands(
        self, runtime_session_id: str, wait_seconds: int
    ) -> list[dict[str, Any]]: ...

    async def ack_cancel(self, request: dict[str, Any]) -> dict[str, Any]: ...

    async def call_agent(
        self,
        request: dict[str, Any],
        *,
        node_envelope: str,
        invocation_token: str,
        idempotency_key: str,
    ) -> dict[str, Any]: ...

    async def close(self) -> None: ...


async def discover_runtime_connection(
    platform_url: str,
    *,
    _client: httpx.AsyncClient | None = None,
) -> RuntimeDiscoveryConnection:
    """Discover the mTLS Runtime origin without sending Runtime credentials."""

    origin = validate_platform_origin(platform_url)
    owns_client = _client is None
    client = _client or httpx.AsyncClient(
        timeout=5.0,
        follow_redirects=False,
        trust_env=False,
        headers={"Accept": "application/json", "X-OpenLinker-SDK": _SDK_AGENT},
    )
    try:
        response = await client.get(origin + _DISCOVERY_PATH)
        if 300 <= response.status_code < 400:
            raise RuntimeProtocolError("Runtime discovery redirects are not allowed")
        response.raise_for_status()
        if len(response.content) > 64 * 1024:
            raise RuntimeProtocolError("Runtime discovery manifest exceeds 64 KiB")
        manifest = _strict_object(response.content, "Runtime discovery manifest")
    finally:
        if owns_client:
            await client.aclose()
    return decode_runtime_discovery_manifest(manifest)


async def discover_runtime_origin(
    platform_url: str,
    *,
    _client: httpx.AsyncClient | None = None,
) -> str:
    connection = await discover_runtime_connection(platform_url, _client=_client)
    return connection.runtime_origin


def decode_runtime_discovery_manifest(manifest: dict[str, Any]) -> RuntimeDiscoveryConnection:
    base_urls = manifest.get("base_urls")
    runtime = manifest.get("runtime")
    if not isinstance(base_urls, dict) or not isinstance(runtime, dict):
        raise RuntimeProtocolError("OpenLinker does not provide Runtime discovery")
    if runtime.get("enabled") is not True or not isinstance(runtime.get("mtls_required"), bool):
        raise RuntimeProtocolError("OpenLinker Runtime is disabled or has no transport policy")
    discovered = base_urls.get("runtime")
    if not isinstance(discovered, str) or not discovered:
        raise RuntimeProtocolError("OpenLinker does not provide a Runtime origin")
    return RuntimeDiscoveryConnection(
        runtime_origin=validate_runtime_origin(
            discovered, allow_loopback_http=runtime["mtls_required"] is False
        ),
        policy=decode_runtime_transport_policy(runtime),
        mtls_required=runtime["mtls_required"],
        credential_endpoint=str(runtime.get("credential_endpoint", "")),
        trust_bundle_endpoint=str(runtime.get("trust_bundle_endpoint", "")),
    )


def resolve_runtime_transport_selection(
    configured: str, policy: RuntimeTransportPolicy
) -> tuple[str, tuple[str, ...]]:
    configured = configured.strip().lower()
    if configured != "auto" and configured not in policy.allowed_transports:
        raise ValueError(
            f"configured Runtime transport {configured!r} is not allowed by OpenLinker"
        )
    mode = policy.default_transport if configured == "auto" else configured
    if mode != "auto":
        if mode not in policy.allowed_transports:
            raise RuntimeProtocolError(
                f"OpenLinker Runtime default transport {mode!r} is not allowed"
            )
        return mode, (mode,)
    return mode, policy.allowed_transports


def decode_runtime_transport_policy(runtime: dict[str, Any]) -> RuntimeTransportPolicy:
    raw_transports = runtime.get("transports", ["websocket", "long_poll"])
    if not isinstance(raw_transports, list):
        raise RuntimeProtocolError("OpenLinker Runtime transport allowlist is invalid")
    allowed: list[str] = []
    for raw in raw_transports:
        if not isinstance(raw, str):
            raise RuntimeProtocolError("OpenLinker Runtime transport allowlist is invalid")
        mode = _manifest_transport_mode(raw)
        if mode is not None and mode not in allowed:
            allowed.append(mode)
    if not allowed:
        raise RuntimeProtocolError(
            "OpenLinker Runtime does not allow a transport supported by this SDK"
        )

    raw_default = runtime.get("default_transport", "auto")
    if not isinstance(raw_default, str):
        raise RuntimeProtocolError("OpenLinker Runtime default transport is invalid")
    default_transport = (
        "auto" if raw_default.strip().lower() == "auto" else _manifest_transport_mode(raw_default)
    )
    if default_transport is None:
        raise RuntimeProtocolError(
            f"OpenLinker Runtime default transport {raw_default.strip()!r} is unsupported"
        )
    if default_transport != "auto" and default_transport not in allowed:
        raise RuntimeProtocolError(
            f"OpenLinker Runtime default transport {default_transport!r} is outside its allowlist"
        )

    if "transport_policy" not in runtime:
        return RuntimeTransportPolicy(tuple(allowed), default_transport)
    raw_policy = runtime["transport_policy"]
    if not isinstance(raw_policy, dict):
        raise RuntimeProtocolError("OpenLinker Runtime transport policy is invalid")
    if "version" in raw_policy and (
        isinstance(raw_policy["version"], bool) or raw_policy["version"] != 1
    ):
        raise RuntimeProtocolError(
            f"OpenLinker Runtime transport policy version {raw_policy['version']!r} is unsupported"
        )
    heartbeat = _optional_policy_duration(raw_policy, "heartbeat_interval_seconds", 1.0)
    stale_after = _optional_policy_duration(raw_policy, "session_stale_after_seconds", 1.0)
    retry_minimum = _optional_policy_duration(raw_policy, "retry_minimum_ms", 0.001)
    retry_maximum = _optional_policy_duration(raw_policy, "retry_maximum_ms", 0.001)
    probe_interval = _optional_policy_duration(raw_policy, "websocket_probe_interval_ms", 0.001)
    probe_timeout = _optional_policy_duration(raw_policy, "websocket_probe_timeout_ms", 0.001)
    if (retry_maximum if retry_maximum is not None else 15.0) < (
        retry_minimum if retry_minimum is not None else 0.25
    ):
        raise RuntimeProtocolError("OpenLinker Runtime retry maximum is below retry minimum")
    if stale_after is not None and (heartbeat if heartbeat is not None else 5.0) >= stale_after:
        raise RuntimeProtocolError(
            "OpenLinker Runtime heartbeat interval must be below the Session stale interval"
        )
    return RuntimeTransportPolicy(
        allowed_transports=tuple(allowed),
        default_transport=default_transport,
        heartbeat_interval=heartbeat,
        session_stale_after=stale_after,
        retry_minimum=retry_minimum,
        retry_maximum=retry_maximum,
        websocket_probe_interval=probe_interval,
        websocket_probe_timeout=probe_timeout,
    )


def _manifest_transport_mode(value: str) -> str | None:
    normalized = value.strip().lower()
    if normalized in {"websocket", "ws"}:
        return "ws"
    if normalized in {"long_poll", "pull"}:
        return "pull"
    return None


def _optional_policy_duration(
    policy: dict[str, Any], field: str, multiplier: float
) -> float | None:
    if field not in policy:
        return None
    value = policy[field]
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 86_400_000:
        raise RuntimeProtocolError(f"OpenLinker Runtime {field} is outside the supported range")
    duration = value * multiplier
    if duration > 86_400:
        raise RuntimeProtocolError(f"OpenLinker Runtime {field} is outside the supported range")
    return duration


def validate_platform_origin(value: str) -> str:
    return _validate_origin(value, runtime=False)


def validate_runtime_origin(value: str, *, allow_loopback_http: bool = False) -> str:
    return _validate_origin(value, runtime=not allow_loopback_http)


def build_runtime_ssl_context(config: RuntimeMTLS) -> ssl.SSLContext:
    if not config.cert_file or not config.key_file or not config.ca_file:
        raise ValueError("Runtime mTLS cert, key, and CA files are required")
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=config.ca_file)
    if not hasattr(ssl.TLSVersion, "TLSv1_3"):
        raise RuntimeError("TLS 1.3 is required for OpenLinker Runtime")
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(config.cert_file, config.key_file)
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context


class HTTPRuntimeTransport:
    kind = "pull"

    def __init__(
        self,
        runtime_origin: str,
        agent_token: str,
        mtls: RuntimeMTLS,
        *,
        node_id: str,
        mtls_required: bool = True,
        credential_manager: Any = None,
        _client: httpx.AsyncClient | None = None,
    ) -> None:
        self.runtime_origin = validate_runtime_origin(
            runtime_origin, allow_loopback_http=not mtls_required
        )
        if not agent_token or agent_token != agent_token.strip():
            raise ValueError("Agent Token is required")
        self.node_id = _runtime_node_id(node_id)
        self._agent_token = agent_token
        self._mtls = mtls
        self._mtls_required = mtls_required
        self._credential_manager = credential_manager
        self._attachment_id = ""
        self._attachment_generation = 0
        self._attachment_transition = asyncio.Lock()
        self._owns_client = _client is None
        if _client is not None:
            self._client = _client
        else:
            ssl_context: ssl.SSLContext | bool = (
                build_runtime_ssl_context(mtls) if mtls_required else True
            )
            if mtls.server_name:
                hostname = urlparse(self.runtime_origin).hostname or ""
                if mtls.server_name != hostname:
                    raise ValueError(
                        "HTTP Runtime server_name override is unsupported; use the certificate hostname in runtime_url"
                    )
            self._client = httpx.AsyncClient(
                verify=ssl_context,
                timeout=httpx.Timeout(35.0, connect=10.0, write=10.0, pool=5.0),
                follow_redirects=False,
                trust_env=False,
            )

    async def create_session(
        self, hello: dict[str, Any], *, fallback_reason: str = ""
    ) -> RuntimeReady:
        _validate_fallback_reason(fallback_reason)
        async with self._attachment_transition:
            ready = RuntimeReady.from_dict(
                await self._request(
                    "POST",
                    "/api/v1/agent-runtime/sessions",
                    body=hello,
                    headers=(
                        {RUNTIME_FALLBACK_REASON_HEADER: fallback_reason}
                        if fallback_reason
                        else None
                    ),
                    use_attachment=False,
                )
            )
            self._attachment_id = ready.attachment_id
            self._attachment_generation += 1
            return ready

    async def heartbeat_session(self, hello: dict[str, Any]) -> RuntimeReady:
        async with self._attachment_transition:
            attachment_id = self._attachment_id
            session_id = quote(str(hello["runtime_session_id"]), safe="")
            ready = RuntimeReady.from_dict(
                await self._request(
                    "POST",
                    f"/api/v1/agent-runtime/sessions/{session_id}/heartbeat",
                    body=hello,
                )
            )
            if ready.attachment_id != attachment_id:
                raise RuntimeProtocolError(
                    "Runtime heartbeat attachment_id does not match the current attachment"
                )
            return ready

    async def drain_session(
        self, runtime_session_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        request = validate_runtime_drain_payload(request)
        runtime_session_id = _runtime_session_id(runtime_session_id)
        async with self._attachment_transition:
            session_id = quote(runtime_session_id, safe="")
            response = await self._request(
                "POST",
                f"/api/v1/agent-runtime/sessions/{session_id}/drain",
                body=request,
            )
        return validate_runtime_drain_payload(response)

    async def close_session(self, request: dict[str, Any]) -> None:
        async with self._attachment_transition:
            session_id = quote(str(request["runtime_session_id"]), safe="")
            await self._request(
                "POST",
                f"/api/v1/agent-runtime/sessions/{session_id}/close",
                body=request,
                expect_empty=True,
            )
            self._attachment_id = ""
            self._attachment_generation += 1

    async def claim_assignment(
        self, wait_seconds: int, request: dict[str, Any]
    ) -> ClaimedAssignment | None:
        value = await self._request(
            "POST",
            "/api/v1/agent-runtime/runs/claim",
            query={"wait": str(wait_seconds)},
            body=request,
            allow_empty=True,
        )
        if value is None:
            return None
        assignment = RuntimeAssignment.from_dict(value)
        if assignment.attempt_identity.runtime_session_id != request["runtime_session_id"]:
            raise RuntimeProtocolError("Runtime assignment belongs to another Session")
        return ClaimedAssignment(assignment)

    async def ack_assignment(
        self, request: dict[str, Any], *, delivery_id: str = ""
    ) -> dict[str, Any]:
        del delivery_id
        run_id = quote(str(request["attempt_identity"]["run_id"]), safe="")
        return await self._request(
            "POST",
            f"/api/v1/agent-runtime/runs/{run_id}/assignment-ack",
            body=request,
        )

    async def reject_assignment(
        self, request: dict[str, Any], *, delivery_id: str = ""
    ) -> dict[str, Any]:
        del delivery_id
        run_id = quote(str(request["attempt_identity"]["run_id"]), safe="")
        return await self._request(
            "POST",
            f"/api/v1/agent-runtime/runs/{run_id}/assignment-reject",
            body=request,
        )

    async def renew_lease(self, request: dict[str, Any]) -> dict[str, Any]:
        run_id = quote(str(request["attempt_identity"]["run_id"]), safe="")
        return await self._request(
            "POST", f"/api/v1/agent-runtime/runs/{run_id}/lease-renew", body=request
        )

    async def send_event(self, request: dict[str, Any]) -> dict[str, Any]:
        run_id = quote(str(request["attempt_identity"]["run_id"]), safe="")
        return await self._request(
            "POST", f"/api/v1/agent-runtime/runs/{run_id}/events", body=request
        )

    async def send_result(self, request: dict[str, Any]) -> dict[str, Any]:
        run_id = quote(str(request["attempt_identity"]["run_id"]), safe="")
        return await self._request(
            "POST", f"/api/v1/agent-runtime/runs/{run_id}/result", body=request
        )

    async def resume(self, request: dict[str, Any]) -> list[dict[str, Any]]:
        value = await self._request("POST", "/api/v1/agent-runtime/runs/resume", body=request)
        decisions = value.get("decisions")
        if not isinstance(decisions, list):
            raise RuntimeProtocolError("Runtime resume response has no decisions")
        return [_require_dict(item, "Runtime resume decision") for item in decisions]

    async def poll_commands(
        self, runtime_session_id: str, wait_seconds: int
    ) -> list[dict[str, Any]]:
        value = await self._request(
            "GET",
            "/api/v1/agent-runtime/commands",
            query={"runtime_session_id": runtime_session_id, "wait": str(wait_seconds)},
        )
        commands = value.get("commands")
        if not isinstance(commands, list):
            raise RuntimeProtocolError("Runtime command response has no commands")
        return [_require_dict(item, "Runtime command") for item in commands]

    async def ack_cancel(self, request: dict[str, Any]) -> dict[str, Any]:
        run_id = quote(str(request["attempt_identity"]["run_id"]), safe="")
        return await self._request(
            "POST", f"/api/v1/agent-runtime/runs/{run_id}/cancel-ack", body=request
        )

    async def call_agent(
        self,
        request: dict[str, Any],
        *,
        node_envelope: str,
        invocation_token: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        body = wire_json_bytes(request)
        proof = build_invocation_proof(
            invocation_token,
            body=body,
            context=node_envelope,
            idempotency_key=idempotency_key,
        )
        return await self._request(
            "POST",
            "/api/v1/agent-runtime/call-agent",
            raw_body=body,
            token=invocation_token,
            headers={
                "Idempotency-Key": idempotency_key,
                "OpenLinker-Invocation-Context": node_envelope,
                "OpenLinker-Invocation-Proof": proof,
            },
            use_attachment=False,
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
        raw_body: bytes | None = None,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        allow_empty: bool = False,
        expect_empty: bool = False,
        use_attachment: bool = True,
    ) -> dict[str, Any] | None:
        url = self.runtime_origin + path
        if query:
            url += "?" + urlencode(query)
        request_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token or self._agent_token}",
            "X-OpenLinker-SDK": _SDK_AGENT,
        }
        attachment_id = ""
        attachment_generation: int | None = None
        if use_attachment:
            if not self._attachment_id:
                raise RuntimeProtocolError("Runtime attachment is not established")
            attachment_id = self._attachment_id
            attachment_generation = self._attachment_generation
            request_headers[_ATTACHMENT_HEADER] = attachment_id
        request_headers.update(headers or {})
        # Node identity is transport-owned; callers cannot select a different
        # Node by supplying an operation-specific header.
        request_headers[RUNTIME_NODE_ID_HEADER] = self.node_id
        content = (
            raw_body
            if raw_body is not None
            else (wire_json_bytes(body) if body is not None else None)
        )
        if self._credential_manager is not None:
            await self._credential_manager.ensure(False)
        try:
            response = await self._client.request(
                method, url, content=content, headers=request_headers, follow_redirects=False
            )
        except httpx.TransportError as exc:
            if self._credential_manager is None or not _credential_tls_failure(exc):
                raise
            await self._credential_manager.ensure(True)
            if self._owns_client:
                await self._client.aclose()
                verify: ssl.SSLContext | bool = (
                    build_runtime_ssl_context(self._credential_manager.mtls)
                    if self._mtls_required
                    else True
                )
                self._client = httpx.AsyncClient(
                    verify=verify,
                    timeout=httpx.Timeout(35.0, connect=10.0, write=10.0, pool=5.0),
                    follow_redirects=False,
                    trust_env=False,
                )
            response = await self._client.request(
                method, url, content=content, headers=request_headers, follow_redirects=False
            )
        if attachment_generation is not None and (
            attachment_generation != self._attachment_generation
            or attachment_id != self._attachment_id
        ):
            raise RuntimeProtocolError("Runtime attachment changed while a request was in flight")
        if 300 <= response.status_code < 400:
            raise RuntimeProtocolError("Runtime redirects are not allowed")
        if response.status_code < 200 or response.status_code >= 300:
            raise _remote_error(response)
        if response.status_code == 204:
            if expect_empty or allow_empty:
                return None
            raise RuntimeProtocolError("Runtime returned an unexpected empty response")
        if expect_empty:
            raise RuntimeProtocolError("Runtime close did not return 204")
        return _strict_object(response.content, "Runtime response")


class WebSocketRuntimeTransport:
    kind = "ws"

    def __init__(
        self,
        runtime_origin: str,
        agent_token: str,
        mtls: RuntimeMTLS,
        http_transport: HTTPRuntimeTransport,
        *,
        mtls_required: bool = True,
        credential_manager: Any = None,
    ) -> None:
        self.runtime_origin = validate_runtime_origin(
            runtime_origin, allow_loopback_http=not mtls_required
        )
        self._agent_token = agent_token
        self._mtls = mtls
        self._mtls_required = mtls_required
        self._credential_manager = credential_manager
        self._http = http_transport
        self._node_id = http_transport.node_id
        self._socket: Any = None
        self._hello: dict[str, Any] | None = None
        self._ready: RuntimeReady | None = None
        self._reader: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._assignments: asyncio.Queue[ClaimedAssignment] = asyncio.Queue()
        self._commands: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._offers: dict[str, str] = {}
        self._cancellations: dict[str, str] = {}
        self._closed = asyncio.Event()
        self._closed_error: Exception | None = None

    async def connect(self, hello: dict[str, Any], *, fallback_reason: str = "") -> RuntimeReady:
        if self._socket is not None:
            raise RuntimeError("Runtime WebSocket is already connected")
        _validate_fallback_reason(fallback_reason)
        parsed = urlparse(self.runtime_origin)
        uri = urlunparse(
            parsed._replace(
                scheme="wss" if self._mtls_required else "ws",
                path=RUNTIME_WEBSOCKET_PATH,
            )
        )
        if self._credential_manager is not None:
            await self._credential_manager.ensure(False)
        ssl_context = build_runtime_ssl_context(self._mtls) if self._mtls_required else None
        kwargs: dict[str, Any] = {
            "ssl": ssl_context,
            "max_size": RUNTIME_MAX_MESSAGE_BYTES,
            "open_timeout": 5,
            "ping_interval": 20,
            "ping_timeout": 20,
        }
        header_name = (
            "additional_headers"
            if "additional_headers" in inspect.signature(websockets.connect).parameters
            else "extra_headers"
        )
        kwargs[header_name] = {
            "Authorization": f"Bearer {self._agent_token}",
            "X-OpenLinker-SDK": _SDK_AGENT,
            RUNTIME_NODE_ID_HEADER: self._node_id,
        }
        if fallback_reason:
            kwargs[header_name][RUNTIME_FALLBACK_REASON_HEADER] = fallback_reason
        if self._mtls_required and self._mtls.server_name:
            kwargs["server_hostname"] = self._mtls.server_name
        try:
            self._socket = await websockets.connect(uri, **kwargs)
        except Exception as exc:
            if (
                self._credential_manager is not None
                and self._mtls_required
                and _credential_tls_failure(exc)
            ):
                try:
                    await self._credential_manager.ensure(True)
                    self._mtls = self._credential_manager.mtls
                    kwargs["ssl"] = build_runtime_ssl_context(self._mtls)
                    self._socket = await websockets.connect(uri, **kwargs)
                except Exception as retry_exc:
                    translated = _websocket_upgrade_error(retry_exc)
                    if translated is not None:
                        raise translated from retry_exc
                    raise
            else:
                translated = _websocket_upgrade_error(exc)
                if translated is not None:
                    raise translated from exc
                raise
        self._hello = dict(hello)
        hello_id = await self._send_envelope("runtime.hello", hello)
        try:
            raw = await asyncio.wait_for(self._socket.recv(), timeout=5)
            envelope = _decode_envelope(raw)
            if envelope.get("reply_to_message_id") != hello_id:
                raise RuntimeProtocolError("Runtime WebSocket did not return correlated ready")
            _raise_envelope_error(envelope)
            if envelope.get("type") != "runtime.ready":
                raise RuntimeProtocolError("Runtime WebSocket did not return ready")
            self._ready = RuntimeReady.from_dict(
                _require_dict(envelope.get("payload"), "ready payload")
            )
        except Exception:
            await self.close()
            raise
        self._reader = asyncio.create_task(self._read_loop())
        return self._ready

    async def create_session(
        self, hello: dict[str, Any], *, fallback_reason: str = ""
    ) -> RuntimeReady:
        _validate_fallback_reason(fallback_reason)
        if self._ready is None or self._hello != hello:
            raise RuntimeProtocolError("Runtime WebSocket is attached to another Session")
        return self._ready

    async def heartbeat_session(self, hello: dict[str, Any]) -> RuntimeReady:
        if self._closed.is_set():
            raise self._closed_error or ConnectionError("Runtime WebSocket is closed")
        if self._ready is None or self._hello is None:
            raise RuntimeProtocolError("Runtime WebSocket is not attached")
        for key in (
            "node_id",
            "agent_id",
            "worker_id",
            "runtime_session_id",
            "session_epoch",
        ):
            if hello.get(key) != self._hello.get(key):
                raise RuntimeProtocolError("Runtime WebSocket heartbeat identity mismatch")
        return self._ready

    async def drain_session(
        self, runtime_session_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        request = validate_runtime_drain_payload(request)
        runtime_session_id = _runtime_session_id(runtime_session_id)
        if self._hello is None or runtime_session_id != self._hello.get("runtime_session_id"):
            raise RuntimeProtocolError("Runtime WebSocket drain Session mismatch")
        response = await self._request_one("runtime.drain", request, "runtime.drain")
        return validate_runtime_drain_payload(response)

    async def close_session(self, request: dict[str, Any]) -> None:
        if self._hello is not None:
            expected = {
                key: self._hello[key]
                for key in (
                    "node_id",
                    "agent_id",
                    "worker_id",
                    "runtime_session_id",
                    "session_epoch",
                )
            }
            if any(request.get(key) != value for key, value in expected.items()):
                raise RuntimeProtocolError("Runtime WebSocket close identity mismatch")
        await self.close()

    async def claim_assignment(
        self, wait_seconds: int, request: dict[str, Any]
    ) -> ClaimedAssignment | None:
        if self._hello is None or request.get("runtime_session_id") != self._hello.get(
            "runtime_session_id"
        ):
            raise RuntimeProtocolError("Runtime WebSocket claim Session mismatch")
        return await self._queue_or_closed(self._assignments, timeout=max(wait_seconds, 0.1))

    async def ack_assignment(
        self, request: dict[str, Any], *, delivery_id: str = ""
    ) -> dict[str, Any]:
        identity = _require_dict(request.get("attempt_identity"), "Attempt identity")
        attempt_id = str(identity.get("attempt_id", ""))
        reply_to = delivery_id or self._offers.get(attempt_id, "")
        if not reply_to:
            raise RuntimeProtocolError("Runtime assignment has no delivery message")
        response = await self._request_one(
            "run.assignment.ack",
            request,
            "run.assignment.confirmed",
            reply_to=reply_to,
        )
        self._offers.pop(attempt_id, None)
        return response

    async def reject_assignment(
        self, request: dict[str, Any], *, delivery_id: str = ""
    ) -> dict[str, Any]:
        identity = _require_dict(request.get("attempt_identity"), "Attempt identity")
        attempt_id = str(identity.get("attempt_id", ""))
        reply_to = delivery_id or self._offers.get(attempt_id, "")
        response = await self._request_one(
            "run.assignment.reject",
            request,
            "run.assignment.rejected",
            reply_to=reply_to,
        )
        self._offers.pop(attempt_id, None)
        return response

    async def renew_lease(self, request: dict[str, Any]) -> dict[str, Any]:
        return await self._request_one("run.lease.renew", request, "run.lease.renewed")

    async def send_event(self, request: dict[str, Any]) -> dict[str, Any]:
        return await self._request_one("run.event", request, "run.event.ack")

    async def send_result(self, request: dict[str, Any]) -> dict[str, Any]:
        return await self._request_one("run.result", request, "run.result.ack")

    async def resume(self, request: dict[str, Any]) -> list[dict[str, Any]]:
        count = len(request.get("attempts", []))
        if count == 0:
            return []
        message_id, queue = await self._start_request("runtime.resume", request)
        decisions: list[dict[str, Any]] = []
        try:
            for _ in range(count):
                envelope = await asyncio.wait_for(queue.get(), timeout=20)
                _raise_envelope_error(envelope)
                if envelope.get("type") != "run.resume.accepted":
                    raise RuntimeProtocolError("unexpected Runtime resume response")
                decisions.append(_require_dict(envelope.get("payload"), "resume payload"))
        finally:
            self._pending.pop(message_id, None)
        return decisions

    async def poll_commands(
        self, runtime_session_id: str, wait_seconds: int
    ) -> list[dict[str, Any]]:
        if self._hello is None or runtime_session_id != self._hello.get("runtime_session_id"):
            raise RuntimeProtocolError("Runtime command Session mismatch")
        command = await self._queue_or_closed(self._commands, timeout=max(wait_seconds, 0.1))
        if command is None:
            return []
        commands = [command]
        while not self._commands.empty():
            commands.append(self._commands.get_nowait())
        return commands

    async def ack_cancel(self, request: dict[str, Any]) -> dict[str, Any]:
        key = _cancellation_key(request)
        reply_to = self._cancellations.get(key, "")
        if not reply_to:
            raise RuntimeProtocolError("Runtime cancellation has no WebSocket command correlation")
        await self._send_envelope("run.cancel.ack", request, reply_to=reply_to)
        if request.get("cancel_state") in {"stopped", "unsupported", "failed"}:
            self._cancellations.pop(key, None)
        response = {
            "cancellation_id": request["cancellation_id"],
            "cancel_state": request["cancel_state"],
            "updated_at": format_datetime(datetime.now(timezone.utc)),
        }
        if request.get("error_code"):
            response["error_code"] = request["error_code"]
        return response

    async def call_agent(
        self,
        request: dict[str, Any],
        *,
        node_envelope: str,
        invocation_token: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return await self._http.call_agent(
            request,
            node_envelope=node_envelope,
            invocation_token=invocation_token,
            idempotency_key=idempotency_key,
        )

    async def close(self) -> None:
        if self._socket is not None:
            socket, self._socket = self._socket, None
            await socket.close()
        if self._reader is not None and self._reader is not asyncio.current_task():
            self._reader.cancel()
            await asyncio.gather(self._reader, return_exceptions=True)
            self._reader = None
        self._closed.set()
        for queue in self._pending.values():
            queue.put_nowait({"type": "runtime.closed", "payload": {}})

    async def _request_one(
        self,
        message_type: str,
        payload: dict[str, Any],
        expected_type: str,
        *,
        reply_to: str = "",
    ) -> dict[str, Any]:
        message_id, queue = await self._start_request(message_type, payload, reply_to=reply_to)
        try:
            envelope = await asyncio.wait_for(queue.get(), timeout=20)
            _raise_envelope_error(envelope)
            if envelope.get("type") != expected_type:
                raise RuntimeProtocolError(f"unexpected Runtime response {envelope.get('type')!r}")
            return _require_dict(envelope.get("payload"), "Runtime payload")
        finally:
            self._pending.pop(message_id, None)

    async def _start_request(
        self,
        message_type: str,
        payload: dict[str, Any],
        *,
        reply_to: str = "",
    ) -> tuple[str, asyncio.Queue[dict[str, Any]]]:
        message_id = str(uuid.uuid4())
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._pending[message_id] = queue
        try:
            await self._send_envelope(
                message_type, payload, message_id=message_id, reply_to=reply_to
            )
        except Exception:
            self._pending.pop(message_id, None)
            raise
        return message_id, queue

    async def _send_envelope(
        self,
        message_type: str,
        payload: dict[str, Any],
        *,
        message_id: str = "",
        reply_to: str = "",
    ) -> str:
        if self._socket is None:
            raise ConnectionError("Runtime WebSocket is not open")
        message_id = message_id or str(uuid.uuid4())
        envelope = {
            "protocol_version": RUNTIME_PROTOCOL_VERSION,
            "runtime_contract_id": RUNTIME_CONTRACT_ID,
            "message_id": message_id,
            "type": message_type,
            "sent_at": format_datetime(datetime.now(timezone.utc)),
            "payload": payload,
        }
        if reply_to:
            envelope["reply_to_message_id"] = reply_to
        raw = wire_json_bytes(envelope).decode()
        async with self._send_lock:
            await self._socket.send(raw)
        return message_id

    async def _read_loop(self) -> None:
        try:
            async for raw in self._socket:
                envelope = _decode_envelope(raw)
                reply_to = str(envelope.get("reply_to_message_id", ""))
                if reply_to and reply_to in self._pending:
                    await self._pending[reply_to].put(envelope)
                    continue
                message_type = envelope.get("type")
                payload = _require_dict(envelope.get("payload"), "Runtime push payload")
                if message_type == "run.assigned":
                    assignment = RuntimeAssignment.from_dict(payload)
                    message_id = str(envelope["message_id"])
                    self._offers[assignment.attempt_identity.attempt_id] = message_id
                    await self._assignments.put(ClaimedAssignment(assignment, message_id))
                elif message_type in {
                    "run.cancel",
                    "runtime.drain",
                    "run.lease.revoked",
                }:
                    if message_type == "run.cancel":
                        self._cancellations[_cancellation_key(payload)] = str(
                            envelope["message_id"]
                        )
                    await self._commands.put({"type": message_type, "payload": payload})
                else:
                    raise RuntimeProtocolError(f"unexpected Runtime push {message_type!r}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._closed_error = exc
        finally:
            self._closed.set()
            closed = {"type": "runtime.closed", "payload": {}}
            for queue in self._pending.values():
                queue.put_nowait(closed)

    async def _queue_or_closed(
        self,
        queue: asyncio.Queue[Any],
        *,
        timeout: float,
    ) -> Any | None:
        if self._closed.is_set():
            raise self._closed_error or ConnectionError("Runtime WebSocket is closed")
        queued = asyncio.create_task(queue.get())
        closed = asyncio.create_task(self._closed.wait())
        try:
            done, _ = await asyncio.wait(
                {queued, closed}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if queued in done:
                return queued.result()
            if closed in done:
                raise self._closed_error or ConnectionError("Runtime WebSocket is closed")
            return None
        finally:
            queued.cancel()
            closed.cancel()
            await asyncio.gather(queued, closed, return_exceptions=True)


def _decode_envelope(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, str):
        raw = raw.encode()
    if len(raw) > RUNTIME_MAX_MESSAGE_BYTES:
        raise RuntimeProtocolError("Runtime WebSocket message exceeds 4 MiB")
    envelope = _strict_object(raw, "Runtime WebSocket message")
    if (
        envelope.get("protocol_version") != RUNTIME_PROTOCOL_VERSION
        or envelope.get("runtime_contract_id") != RUNTIME_CONTRACT_ID
    ):
        raise RuntimeProtocolError("Runtime WebSocket contract mismatch")
    try:
        parsed_id = uuid.UUID(str(envelope["message_id"]))
    except (KeyError, ValueError) as exc:
        raise RuntimeProtocolError("Runtime WebSocket message_id is invalid") from exc
    if str(parsed_id) != envelope["message_id"]:
        raise RuntimeProtocolError("Runtime WebSocket message_id is not canonical")
    reply_to = envelope.get("reply_to_message_id")
    if reply_to is not None:
        try:
            parsed_reply = uuid.UUID(str(reply_to))
        except ValueError as exc:
            raise RuntimeProtocolError("Runtime WebSocket reply_to_message_id is invalid") from exc
        if str(parsed_reply) != reply_to:
            raise RuntimeProtocolError("Runtime WebSocket reply_to_message_id is not canonical")
    try:
        parse_datetime(envelope.get("sent_at"))
    except (TypeError, ValueError) as exc:
        raise RuntimeProtocolError("Runtime WebSocket sent_at is invalid") from exc
    if not isinstance(envelope.get("type"), str) or not isinstance(envelope.get("payload"), dict):
        raise RuntimeProtocolError("Runtime WebSocket envelope is incomplete")
    return envelope


def _raise_envelope_error(envelope: dict[str, Any]) -> None:
    if envelope.get("type") == "runtime.closed":
        raise ConnectionError("Runtime WebSocket is closed")
    if envelope.get("type") != "runtime.error":
        return
    payload = _require_dict(envelope.get("payload"), "Runtime error")
    raise _parse_error_body(payload)


def _remote_error(response: httpx.Response) -> RuntimeError:
    if len(response.content) > RUNTIME_MAX_MESSAGE_BYTES:
        return RuntimeProtocolError("Runtime error response exceeds the contract message limit")
    try:
        envelope = _strict_object(response.content, "Runtime error response")
        if set(envelope) != {"error"}:
            raise RuntimeProtocolError("Runtime error response fields do not match the contract")
        error = _require_dict(envelope.get("error"), "Runtime error")
        return _parse_error_body(error, status_code=response.status_code)
    except RuntimeProtocolError as exc:
        return exc


def _parse_error_body(value: dict[str, Any], *, status_code: int = 0) -> RuntimeRemoteError:
    required = {"code", "message"}
    optional = {
        "retryable",
        "missing_event_ranges",
        "current_run_status",
        "current_dispatch_state",
    }
    if required - set(value) or set(value) - required - optional:
        raise RuntimeProtocolError("Runtime error fields do not match the contract")
    code = value["code"]
    message = value["message"]
    retryable = value.get("retryable", False)
    # Error bodies remain structurally strict, but the code enumeration is
    # deliberately forward-compatible. Worker fatality is a best-effort
    # classifier: only explicitly known permanent codes may terminate it.
    if not isinstance(code, str) or _ERROR_CODE.fullmatch(code) is None:
        raise RuntimeProtocolError("Runtime returned an invalid error code")
    if not isinstance(message, str) or not 1 <= len(message) <= 500:
        raise RuntimeProtocolError("Runtime error message is invalid")
    if not isinstance(retryable, bool):
        raise RuntimeProtocolError("Runtime error retryability is invalid")
    ranges_value = value.get("missing_event_ranges", [])
    if not isinstance(ranges_value, list):
        raise RuntimeProtocolError("Runtime missing Event ranges are invalid")
    ranges: list[tuple[int, int]] = []
    for item in ranges_value:
        if not isinstance(item, dict) or set(item) != {"start", "end"}:
            raise RuntimeProtocolError("Runtime missing Event range is invalid")
        start, end = item["start"], item["end"]
        if (
            not isinstance(start, int)
            or isinstance(start, bool)
            or not isinstance(end, int)
            or isinstance(end, bool)
            or start < 1
            or end < start
        ):
            raise RuntimeProtocolError("Runtime missing Event range is invalid")
        ranges.append((start, end))
    if "current_run_status" in value and value["current_run_status"] not in {
        "running",
        "success",
        "failed",
        "timeout",
        "canceled",
    }:
        raise RuntimeProtocolError("Runtime error run status is invalid")
    if "current_dispatch_state" in value and value["current_dispatch_state"] not in {
        "pending",
        "offered",
        "executing",
        "retry_wait",
        "terminal",
        "dead_letter",
    }:
        raise RuntimeProtocolError("Runtime error dispatch state is invalid")
    return RuntimeRemoteError(
        code,
        message,
        retryable=retryable,
        status_code=status_code,
        missing_event_ranges=ranges,
    )


def _strict_object(raw: bytes, label: str) -> dict[str, Any]:
    if not raw or len(raw) > RUNTIME_MAX_MESSAGE_BYTES:
        raise RuntimeProtocolError(f"{label} is empty or too large")
    try:
        text = raw.decode("utf-8")
        decoder = json.JSONDecoder()
        value, end = decoder.raw_decode(text)
        if text[end:].strip():
            raise RuntimeProtocolError(f"{label} contains trailing JSON")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeProtocolError(f"{label} is not valid JSON") from exc
    return _require_dict(value, label)


def _require_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeProtocolError(f"{label} must be an object")
    return dict(value)


def _cancellation_key(value: dict[str, Any]) -> str:
    cancellation_id = str(value.get("cancellation_id", ""))
    identity = _require_dict(value.get("attempt_identity"), "cancel Attempt identity")
    attempt_id = str(identity.get("attempt_id", ""))
    if not cancellation_id or not attempt_id:
        raise RuntimeProtocolError("Runtime cancellation identity is incomplete")
    return cancellation_id + "\x00" + attempt_id


def _validate_origin(value: str, *, runtime: bool) -> str:
    value = value.strip().rstrip("/")
    parsed = urlparse(value)
    if (
        not parsed.scheme
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("OpenLinker origin must contain only scheme, host, and optional port")
    if runtime:
        if parsed.scheme != "https":
            raise ValueError("Runtime origin must use HTTPS")
    elif parsed.scheme == "http":
        if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("HTTP platform origin is allowed only for loopback")
    elif parsed.scheme != "https":
        raise ValueError("platform origin must use HTTPS or loopback HTTP")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("OpenLinker origin has an invalid port") from exc
    if port is not None and not (1 <= port <= 65535):
        raise ValueError("OpenLinker origin has an invalid port")
    return urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))


def _validate_fallback_reason(reason: str) -> None:
    if reason and reason not in RUNTIME_FALLBACK_REASONS:
        raise ValueError(f"invalid Runtime fallback reason {reason!r}")


def _runtime_session_id(value: Any) -> str:
    if not isinstance(value, str):
        raise RuntimeProtocolError("Runtime Session identity is invalid")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise RuntimeProtocolError("Runtime Session identity is invalid") from exc
    if parsed.int == 0 or str(parsed) != value:
        raise RuntimeProtocolError("Runtime Session identity is invalid")
    return value


def _runtime_node_id(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("Runtime Node identity must be a lowercase non-zero UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise ValueError("Runtime Node identity must be a lowercase non-zero UUID") from exc
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError("Runtime Node identity must be a lowercase non-zero UUID")
    return value


def _websocket_upgrade_error(exc: BaseException) -> RuntimeRemoteError | None:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code is None:
        status_code = getattr(exc, "status_code", None)
    body = getattr(response, "body", None)
    if not isinstance(status_code, int) or body is None:
        return None
    if isinstance(body, str):
        raw = body.encode()
    elif isinstance(body, (bytes, bytearray, memoryview)):
        raw = bytes(body)
    else:
        return None
    if len(raw) > 64 * 1024:
        return None
    try:
        envelope = _strict_object(raw, "Runtime WebSocket upgrade error")
        if set(envelope) != {"error"}:
            return None
        return _parse_error_body(
            _require_dict(envelope.get("error"), "Runtime WebSocket upgrade error"),
            status_code=status_code,
        )
    except RuntimeProtocolError:
        return None


def _credential_tls_failure(exc: BaseException) -> bool:
    message = f"{type(exc).__name__}: {exc}".lower()
    return any(
        marker in message for marker in ("tls", "ssl", "x509", "certificate", "unknown authority")
    )


__all__ = [
    "ClaimedAssignment",
    "HTTPRuntimeTransport",
    "RuntimeTransport",
    "RuntimeDiscoveryConnection",
    "RuntimeTransportPolicy",
    "RUNTIME_FALLBACK_REASON_HEADER",
    "RUNTIME_FALLBACK_REASONS",
    "WebSocketRuntimeTransport",
    "decode_runtime_discovery_manifest",
    "decode_runtime_transport_policy",
    "discover_runtime_connection",
    "discover_runtime_origin",
    "resolve_runtime_transport_selection",
]
