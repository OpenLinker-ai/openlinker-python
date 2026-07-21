from __future__ import annotations

import asyncio
import inspect
import logging
import random
import time
import unicodedata
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from websockets.exceptions import ConnectionClosed

from .credentials import RuntimeCredentialManager

from .store import (
    ASSIGNMENT_ACK_SENT,
    ASSIGNMENT_CONFIRMED,
    ASSIGNMENT_FINISHED,
    ASSIGNMENT_RECEIVED,
    ASSIGNMENT_REJECTED,
    ASSIGNMENT_REJECT_SENT,
    ASSIGNMENT_RESULT_ACKED,
    ASSIGNMENT_REVOKED,
    ASSIGNMENT_STARTED,
    AssignmentRecord,
    FileRuntimeStore,
    LocalAttemptIdentity,
    MemoryRuntimeStore,
    RuntimeStore,
)
from .transport import (
    ClaimedAssignment,
    HTTPRuntimeTransport,
    RuntimeTransport,
    RuntimeTransportPolicy,
    WebSocketRuntimeTransport,
    discover_runtime_connection,
    resolve_runtime_transport_selection,
    validate_platform_origin,
    validate_runtime_origin,
)
from .types import (
    RUNTIME_MAX_CAPACITY,
    RuntimeAttemptIdentity,
    RuntimeDrainTimeoutError,
    RuntimeEvent,
    RuntimeHandlerError,
    RuntimeMTLS,
    RuntimeProtocolError,
    RuntimeReady,
    RuntimeRemoteError,
    RuntimeResult,
    RuntimeSpoolStatus,
    RuntimeStoreError,
    format_datetime,
    parse_datetime,
    runtime_hello,
    validate_idempotency_key,
    validate_runtime_drain_payload,
)


DEFAULT_CAPACITY = 1
DEFAULT_CLAIM_WAIT = 25.0
DEFAULT_COMMAND_WAIT = 25.0
DEFAULT_HEARTBEAT_INTERVAL = 5.0
DEFAULT_RETRY_MINIMUM = 0.25
DEFAULT_RETRY_MAXIMUM = 15.0
DEFAULT_SHUTDOWN_TIMEOUT = 10.0
DEFAULT_DRAIN_TIMEOUT = 10.0
MAXIMUM_DRAIN_TIMEOUT = 300.0
DEFAULT_DRAIN_REASON = "SDK_GRACEFUL_SHUTDOWN"
DEFAULT_NODE_VERSION = "openlinker-python/runtime-worker"

_PERMANENT_CODES = {
    "UNAUTHORIZED",
    "FORBIDDEN",
    "PERMISSION_DENIED",
    "RUNTIME_CLIENT_UPGRADE_REQUIRED",
    "RUNTIME_REQUIRED_FEATURE_MISSING",
    "RUNTIME_SESSION_CONFLICT",
    "RUNTIME_SPOOL_CORRUPT",
}
_LEASE_TERMINAL_CODES = {"STALE_LEASE", "LEASE_EXPIRED", "RUN_ALREADY_TERMINAL"}
_ASSIGNMENT_TERMINAL_CODES = _LEASE_TERMINAL_CODES | {"RUN_CANCEL_REQUESTED"}


class _RuntimeStoppedBeforeDrain(RuntimeError):
    def __init__(self) -> None:
        super().__init__("RuntimeWorker stopped before its durable drain completed")


class RuntimeHandler(Protocol):
    async def handle(self, context: RuntimeContext) -> RuntimeResult | dict[str, Any] | Any: ...


RuntimeHandlerCallable = Callable[
    ["RuntimeContext"],
    Awaitable[RuntimeResult | dict[str, Any] | Any] | RuntimeResult | dict[str, Any] | Any,
]


@dataclass
class _ActiveAttempt:
    assignment: AssignmentRecord
    cancel_event: asyncio.Event
    task: asyncio.Task[None] | None = None
    renew_task: asyncio.Task[None] | None = None
    lease_expires_at: datetime | None = None


class RuntimeContext:
    """A confirmed, assignment-scoped Runtime invocation."""

    def __init__(self, worker: RuntimeWorker, active: _ActiveAttempt) -> None:
        self._worker = worker
        self._active = active
        self._closed = asyncio.Event()
        attempt = active.assignment.identity.attempt
        self.run_id = attempt.run_id
        self.agent_id = attempt.agent_id
        self.input = dict(active.assignment.input)
        self.metadata = dict(active.assignment.metadata)

    @property
    def cancelled(self) -> bool:
        return self._active.cancel_event.is_set()

    async def emit(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        if self._closed.is_set() or self.cancelled or self._worker._force_cancel.is_set():
            raise asyncio.CancelledError
        self._worker._persist_event(self._active, RuntimeEvent(event_type, payload or {}))

    async def call_agent(
        self,
        target_agent_id: str,
        input: dict[str, Any],
        *,
        idempotency_key: str,
        reason: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        validate_idempotency_key(idempotency_key)
        _canonical_uuid(target_agent_id, "target_agent_id")
        if self._closed.is_set() or self.cancelled or self._worker._force_cancel.is_set():
            raise asyncio.CancelledError
        request = {
            "target_agent_id": target_agent_id,
            "input": _object(input, "delegated input"),
            "metadata": _object(metadata or {}, "delegated metadata"),
        }
        if reason:
            if len(reason) > 500:
                raise ValueError("delegated call reason exceeds 500 characters")
            request["reason"] = reason
        call = asyncio.create_task(
            self._worker._retry_call(
                lambda: self._worker._transport_required().call_agent(
                    request,
                    node_envelope=self._active.assignment.node_envelope,
                    invocation_token=self._active.assignment.agent_invocation_token,
                    idempotency_key=idempotency_key,
                ),
                deadline=self._active.assignment.attempt_deadline_at,
                continue_during_shutdown=True,
                cancellation=self._active.cancel_event,
            )
        )
        cancelled = asyncio.create_task(self._active.cancel_event.wait())
        closed = asyncio.create_task(self._closed.wait())
        try:
            done, _ = await asyncio.wait(
                {call, cancelled, closed}, return_when=asyncio.FIRST_COMPLETED
            )
            if cancelled in done or closed in done:
                call.cancel()
                await asyncio.gather(call, return_exceptions=True)
                raise asyncio.CancelledError
            return _validate_run_summary(await call)
        finally:
            cancelled.cancel()
            closed.cancel()
            await asyncio.gather(cancelled, closed, return_exceptions=True)

    def _close(self) -> None:
        self._closed.set()


class RuntimeWorker:
    """Run one reliable Runtime Session directly from a Python application.

    A worker is single-use. Construct a new instance after ``run()`` returns.
    """

    def __init__(
        self,
        *,
        platform_url: str,
        node_id: str = "",
        agent_id: str = "",
        agent_token: str,
        mtls: RuntimeMTLS | None = None,
        handler: RuntimeHandler | RuntimeHandlerCallable,
        data_dir: str | Path | None = None,
        store: RuntimeStore | None = None,
        allow_unsafe_memory_store: bool = False,
        runtime_url: str = "",
        transport: str = "auto",
        node_version: str = DEFAULT_NODE_VERSION,
        capacity: int = DEFAULT_CAPACITY,
        claim_wait: float = DEFAULT_CLAIM_WAIT,
        command_wait: float = DEFAULT_COMMAND_WAIT,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        retry_minimum: float = DEFAULT_RETRY_MINIMUM,
        retry_maximum: float = DEFAULT_RETRY_MAXIMUM,
        websocket_probe_interval: float | None = None,
        websocket_probe_timeout: float = 10.0,
        shutdown_timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
        on_ready: Callable[[RuntimeReady], Any] | None = None,
        on_fatal: Callable[[BaseException], Any] | None = None,
        on_drain: Callable[[], Any] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.platform_url = platform_url.strip()
        self.runtime_url = runtime_url.strip()
        self.node_id = node_id
        self.agent_id = agent_id
        self.agent_token = agent_token.strip()
        self.mtls = mtls or RuntimeMTLS()
        self.handler = handler
        self.data_dir = Path(data_dir).expanduser() if data_dir is not None else None
        self.store = store
        self.allow_unsafe_memory_store = allow_unsafe_memory_store
        self.transport_mode = transport.strip().lower()
        self._configured_transport_mode = self.transport_mode
        self.node_version = node_version.strip() or DEFAULT_NODE_VERSION
        self.capacity = capacity
        self.claim_wait = claim_wait
        self.command_wait = command_wait
        self.heartbeat_interval = heartbeat_interval
        self.retry_minimum = retry_minimum
        self.retry_maximum = retry_maximum
        self.websocket_probe_interval = websocket_probe_interval
        self.websocket_probe_timeout = websocket_probe_timeout
        self.shutdown_timeout = shutdown_timeout
        self.on_ready = on_ready
        self.on_fatal = on_fatal
        self.on_drain = on_drain
        self.logger = logger or logging.getLogger("openlinker.runtime")

        self._configured_timing = {
            "heartbeat_interval": self.heartbeat_interval,
            "retry_minimum": self.retry_minimum,
            "retry_maximum": self.retry_maximum,
            "websocket_probe_interval": self.websocket_probe_interval,
            "websocket_probe_timeout": self.websocket_probe_timeout,
        }

        self._validate_config()
        self._started = False
        self._completed = False
        self._done = asyncio.Event()
        self._stopping = asyncio.Event()
        self._force_cancel = asyncio.Event()
        self._draining = False
        self._drain_task: asyncio.Task[None] | None = None
        self._drain_before_stop: Callable[[], Awaitable[None] | None] | None = None
        self._stop_owner: str | None = None
        self._fatal: asyncio.Queue[BaseException] = asyncio.Queue(maxsize=1)
        self._spool_wakeup = asyncio.Event()
        self._store: RuntimeStore | None = store
        self._transport: RuntimeTransport | None = None
        self._http_transport: HTTPRuntimeTransport | None = None
        self._ready: RuntimeReady | None = None
        self._active: dict[str, _ActiveAttempt] = {}
        self._attempt_locks: dict[str, asyncio.Lock] = {}
        self._spool_permissions: dict[str, tuple[bool, bool]] = {}
        self._cancellations: set[tuple[str, str]] = set()
        self._terminal_attempts: set[str] = set()
        self._background: set[asyncio.Task[Any]] = set()
        self._websocket_probe_task: asyncio.Task[Any] | None = None
        self._claim_switch_lock = asyncio.Lock()
        self._transport_transitioning = False
        self._transport_order: tuple[str, ...] = ("ws", "pull")
        self._session_stale_after = 0.0
        self._test_transport_recovery: Callable[[], Awaitable[RuntimeTransport]] | None = None
        self._policy_recovery_lock = asyncio.Lock()
        self._policy_revision = 0
        self._policy_last_observed = -1
        self._policy_last_error: BaseException | None = None
        self._policy_terminal_error: RuntimePolicyRecoveryError | None = None
        self._attachment_reason = "explicit"
        self._credential_manager: RuntimeCredentialManager | None = None
        self._mtls_required = True

    async def run(self) -> None:
        if self._started:
            raise RuntimeError("RuntimeWorker is already running")
        if self._completed:
            raise RuntimeError("RuntimeWorker cannot be restarted")
        self._started = True
        run_error: BaseException | None = None
        try:
            await self._start()
            stop_waiter = asyncio.create_task(self._stopping.wait())
            fatal_waiter = asyncio.create_task(self._fatal.get())
            _, pending = await asyncio.wait(
                {stop_waiter, fatal_waiter}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if fatal_waiter.done() and not fatal_waiter.cancelled():
                run_error = fatal_waiter.result()
        except BaseException as exc:
            run_error = exc
        finally:
            try:
                await self._shutdown()
            except BaseException as exc:
                if run_error is None:
                    run_error = exc
            if run_error is None and not self._fatal.empty():
                run_error = self._fatal.get_nowait()
            self._started = False
            self._completed = True
            self._done.set()
        if run_error is not None:
            raise run_error

    async def stop(self) -> None:
        if not self._started:
            return
        self._request_stop("external")
        await self._done.wait()

    async def drain(
        self,
        *,
        timeout: float | None = None,
        reason_code: str = DEFAULT_DRAIN_REASON,
    ) -> None:
        """Fence admission and exit only after Core and the durable spool are empty."""

        timeout, reason_code = _normalize_drain_options(timeout, reason_code)
        if self._drain_task is None:
            if not self._started or self._completed:
                raise RuntimeError("RuntimeWorker must be running before it can drain")
            # Admission linearizes here, before the first network operation.
            self._draining = True
            self._drain_task = asyncio.create_task(
                self._run_drain(timeout=timeout, reason_code=reason_code)
            )
            self._drain_task.add_done_callback(_consume_task_exception)
        await asyncio.shield(self._drain_task)

    async def _run_drain(self, *, timeout: float, reason_code: str) -> None:
        operation = asyncio.create_task(
            self._perform_drain(timeout=timeout, reason_code=reason_code)
        )
        try:
            await asyncio.wait_for(operation, timeout=timeout)
        except asyncio.TimeoutError as exc:
            status = self._runtime_spool_status()
            self._request_stop("drain_failure")
            raise RuntimeDrainTimeoutError(timeout, status) from exc
        except BaseException:
            self._request_stop("drain_failure")
            raise

    async def _perform_drain(self, *, timeout: float, reason_code: str) -> None:
        deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)
        _, inflight = self._capacity_snapshot()
        request = {
            "deadline_at": format_datetime(deadline),
            "reason_code": reason_code,
            "capacity": 0,
            "inflight": inflight,
        }
        server_drain = await self._request_runtime_drain(request)
        while True:
            status = self._runtime_spool_status()
            if status.empty and not self._active:
                if server_drain["inflight"] > 0:
                    server_drain = await self._request_runtime_drain(request)
                    if server_drain["inflight"] > 0:
                        await self._wait_for_drain_progress()
                        continue
                if self._drain_before_stop is not None:
                    await _invoke_optional(self._drain_before_stop)
                if not self._request_stop("drain"):
                    raise _RuntimeStoppedBeforeDrain
                await self._done.wait()
                return
            await self._wait_for_drain_progress()

    async def _request_runtime_drain(self, request: dict[str, Any]) -> dict[str, Any]:
        attempt = 0
        while True:
            if self._stopping.is_set():
                raise _RuntimeStoppedBeforeDrain
            if self._ready is None or self._transport is None or self._store is None:
                await self._wait_for_drain_progress(min(self._retry_delay(attempt), 0.1))
                attempt += 1
                continue
            transport = self._transport
            try:

                async def operation() -> dict[str, Any]:
                    nonlocal transport
                    async with self._claim_switch_lock:
                        transport = self._transport_required()
                        method = getattr(transport, "drain_session", None)
                        if method is None:
                            raise RuntimeProtocolError(
                                "Runtime transport does not implement session drain"
                            )
                        value = method(
                            self._store_required().identity.runtime_session_id,
                            request,
                        )
                        if not inspect.isawaitable(value):
                            raise RuntimeProtocolError(
                                "Runtime transport session drain must be asynchronous"
                            )
                        return await self._drain_call_or_stop(value)

                response = await self._policy_operation(operation)
                return validate_runtime_drain_payload(response)
            except asyncio.CancelledError:
                raise
            except _RuntimeStoppedBeforeDrain:
                raise
            except Exception as exc:
                if _fatal_error(exc):
                    raise
                if transport.kind == "ws" and transport is self._transport:
                    try:
                        await self._recover_transport(transport)
                    except Exception as recovery_error:
                        if _fatal_error(recovery_error):
                            raise
                await self._wait_for_drain_progress(min(self._retry_delay(attempt), 0.1))
                attempt += 1

    async def _drain_call_or_stop(self, awaitable: Awaitable[Any]) -> Any:
        call = asyncio.ensure_future(awaitable)
        stopping = asyncio.create_task(self._stopping.wait())
        try:
            done, _ = await asyncio.wait({call, stopping}, return_when=asyncio.FIRST_COMPLETED)
            if stopping in done:
                call.cancel()
                await asyncio.gather(call, return_exceptions=True)
                raise _RuntimeStoppedBeforeDrain
            return await call
        finally:
            if not call.done():
                call.cancel()
                await asyncio.gather(call, return_exceptions=True)
            stopping.cancel()
            await asyncio.gather(stopping, return_exceptions=True)

    async def _wait_for_drain_progress(self, delay: float = 0.025) -> None:
        if self._stopping.is_set():
            raise _RuntimeStoppedBeforeDrain
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=max(delay, 0.001))
        except asyncio.TimeoutError:
            return
        raise _RuntimeStoppedBeforeDrain

    def _runtime_spool_status(self) -> RuntimeSpoolStatus:
        if self._store is None:
            return RuntimeSpoolStatus(0, 0, 0)
        provider = getattr(self._store, "spool_status", None)
        if provider is not None:
            status = provider()
            if not isinstance(status, RuntimeSpoolStatus):
                raise RuntimeStoreError("Runtime store returned an invalid spool status")
            return status
        assignments = self._store.assignments()
        events = 0
        results = 0
        for assignment in assignments:
            attempt_id = assignment.identity.attempt.attempt_id
            events += len(self._store.pending_events(attempt_id))
            if self._store.pending_result(attempt_id) is not None:
                results += 1
        return RuntimeSpoolStatus(len(assignments), events, results)

    def _request_stop(self, owner: str) -> bool:
        self._draining = True
        if self._stopping.is_set():
            return False
        self._stop_owner = owner
        self._stopping.set()
        return True

    async def _start(self) -> None:
        if self._store is None:
            if self.data_dir is None:
                raise ValueError("Runtime data_dir or store is required")
            self._store = FileRuntimeStore(self.data_dir)
        if self._store.unsafe_memory and not self.allow_unsafe_memory_store:
            raise ValueError(
                "MemoryRuntimeStore requires allow_unsafe_memory_store=True and is not production-safe"
            )
        observed_revision = self._policy_revision
        try:
            await self._setup_transport()
            self._ready = await self._attach_session(self._transport_required())
        except Exception as exc:
            if not is_runtime_policy_recovery_signal(exc):
                raise
            self._ready = await self._recover_runtime_policy(
                observed_revision, resume_durable=False
            )
        await _invoke_optional(self.on_ready, self._ready)
        await self._resume_durable_state(reconnect=False)
        for operation in (
            self._claim_loop,
            self._command_loop,
            self._heartbeat_loop,
            self._spool_loop,
        ):
            self._spawn(operation())
        self._ensure_websocket_probe_loop()

    async def _setup_transport(self) -> None:
        if self._transport is not None:
            return
        explicit_mtls = bool(self.mtls.cert_file and self.mtls.key_file and self.mtls.ca_file)
        connection = None
        origin = self.runtime_url
        if not origin or not explicit_mtls:
            if not self.platform_url:
                raise ValueError("automatic Runtime credentials require platform_url")
            connection = await discover_runtime_connection(self.platform_url)
            if not origin:
                origin = connection.runtime_origin
            self._apply_transport_policy(connection.policy)
        self._mtls_required = connection.mtls_required if connection is not None else True
        self.runtime_url = validate_runtime_origin(
            origin, allow_loopback_http=not self._mtls_required
        )
        if not self._mtls_required:
            if not self.node_id or not self.agent_id:
                raise ValueError(
                    "node_id and agent_id are required for token-only Runtime transport"
                )
        elif not explicit_mtls:
            if self.data_dir is None:
                raise ValueError("automatic Runtime credentials require data_dir")
            platform_origin = validate_platform_origin(self.platform_url)
            endpoint = (
                connection.credential_endpoint if connection is not None else ""
            ) or platform_origin + "/api/v1/runtime-credentials"
            manager = RuntimeCredentialManager(
                data_dir=self.data_dir,
                credential_endpoint=endpoint,
                agent_token=self.agent_token,
                node_id=self.node_id,
                agent_id=self.agent_id,
                node_version=self.node_version,
                capacity=self.capacity,
                logger=self.logger,
            )
            await manager.open()
            await manager.ensure()
            identity = manager.identity
            self.node_id, self.agent_id = identity.node_id, identity.agent_id
            self.mtls = manager.mtls
            self._credential_manager = manager
            manager.start()
        self._http_transport = HTTPRuntimeTransport(
            self.runtime_url,
            self.agent_token,
            self.mtls,
            node_id=self.node_id,
            mtls_required=self._mtls_required,
            credential_manager=self._credential_manager,
        )
        selected_reason = resolve_runtime_fallback_reason(
            self._configured_transport_mode, "policy_selected"
        )
        if self.transport_mode == "pull":
            self._transport = self._http_transport
            self._attachment_reason = selected_reason
            return
        if self.transport_mode == "ws":
            self._transport = await self._connect_websocket_with_retry(
                retry_all=True, fallback_reason=selected_reason
            )
            self._attachment_reason = selected_reason
            return
        if not self._auto_prefers_websocket():
            self._transport = self._http_transport
            self._attachment_reason = selected_reason
            return
        try:
            websocket = WebSocketRuntimeTransport(
                self.runtime_url,
                self.agent_token,
                self.mtls,
                self._http_transport,
                mtls_required=self._mtls_required,
                credential_manager=self._credential_manager,
            )
            await websocket.connect(self._hello(), fallback_reason=selected_reason)
            self._transport = websocket
            self._attachment_reason = selected_reason
        except Exception as exc:
            if (
                _fatal_error(exc) or is_runtime_policy_recovery_signal(exc)
            ) and not _session_conflict(exc):
                raise
            if _session_conflict(exc):
                try:
                    self._transport = await self._connect_websocket_with_retry(
                        retry_all=False, fallback_reason=selected_reason
                    )
                    self._attachment_reason = selected_reason
                    return
                except Exception as retry_error:
                    if _fatal_error(retry_error):
                        raise
            if not self._auto_allows_pull_fallback():
                self._transport = await self._connect_websocket_with_retry(
                    retry_all=True, fallback_reason=selected_reason
                )
                self._attachment_reason = selected_reason
                return
            self._transport = self._http_transport
            self._attachment_reason = "websocket_unavailable"

    async def _connect_websocket_with_retry(
        self,
        *,
        retry_all: bool,
        continue_during_shutdown: bool = False,
        fallback_reason: str = "explicit",
    ) -> WebSocketRuntimeTransport:
        attempt = 0
        while not self._force_cancel.is_set() and (
            continue_during_shutdown or not self._stopping.is_set()
        ):
            if self._http_transport is None:
                raise ConnectionError("HTTP Runtime transport is unavailable")
            websocket = WebSocketRuntimeTransport(
                self.runtime_url,
                self.agent_token,
                self.mtls,
                self._http_transport,
                mtls_required=self._mtls_required,
                credential_manager=self._credential_manager,
            )
            try:
                await websocket.connect(self._hello(), fallback_reason=fallback_reason)
                return websocket
            except asyncio.CancelledError:
                await websocket.close()
                raise
            except Exception as exc:
                await websocket.close()
                if (
                    _fatal_error(exc) or is_runtime_policy_recovery_signal(exc)
                ) and not _session_conflict(exc):
                    raise
                if not retry_all and not _session_conflict(exc):
                    raise
                if continue_during_shutdown:
                    await self._wait_force_or_cancel(None, self._retry_delay(attempt))
                else:
                    await self._wait_or_stop(self._retry_delay(attempt))
                attempt += 1
        raise asyncio.CancelledError

    async def _attach_session(
        self,
        transport: RuntimeTransport,
        *,
        continue_during_shutdown: bool = False,
        fallback_reason: str | None = None,
        policy_aware: bool = False,
    ) -> RuntimeReady:
        attempt = 0
        while not self._force_cancel.is_set() and (
            continue_during_shutdown or not self._stopping.is_set()
        ):
            try:

                async def operation() -> RuntimeReady:
                    return await transport.create_session(
                        self._hello(),
                        fallback_reason=fallback_reason or self._attachment_reason,
                    )

                if policy_aware:
                    return await self._policy_operation(operation)
                return await operation()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if (
                    _fatal_error(exc) or is_runtime_policy_recovery_signal(exc)
                ) and not _session_conflict(exc):
                    raise
                if continue_during_shutdown:
                    await self._wait_force_or_cancel(None, self._retry_delay(attempt))
                else:
                    await self._wait_or_stop(self._retry_delay(attempt))
                attempt += 1
        raise asyncio.CancelledError

    async def _claim_loop(self) -> None:
        attempt = 0
        while not self._stopping.is_set():
            if self._transport_transitioning:
                await self._wait_or_stop(self.retry_minimum)
                continue
            capacity, inflight = self._capacity_snapshot()
            if capacity == 0 or inflight >= capacity:
                await self._wait_or_stop(0.1)
                continue
            transport = self._transport_required()
            try:

                async def claim() -> ClaimedAssignment | None:
                    nonlocal transport
                    async with self._claim_switch_lock:
                        transport = self._transport_required()
                        return await transport.claim_assignment(
                            int(self.claim_wait),
                            {
                                "runtime_session_id": (
                                    self._store_required().identity.runtime_session_id
                                ),
                                "capacity": capacity,
                                "inflight": inflight,
                            },
                        )

                claimed = await self._policy_operation(claim)
                attempt = 0
                if claimed is not None:
                    await self._handle_assignment(claimed)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if _fatal_error(exc):
                    await self._report_fatal(exc)
                    return
                if transport.kind == "ws":
                    try:
                        await self._recover_transport(transport)
                        attempt = 0
                        continue
                    except Exception as recovery_error:
                        if self.transport_mode == "ws" and _fatal_error(recovery_error):
                            await self._report_fatal(recovery_error)
                            return
                await self._wait_or_stop(self._retry_delay(attempt))
                attempt += 1

    async def _handle_assignment(self, claimed: ClaimedAssignment) -> None:
        assignment = claimed.assignment
        attempt_id = assignment.attempt_identity.attempt_id
        if attempt_id in self._terminal_attempts:
            return
        local = self._local_identity(assignment.attempt_identity)
        record = AssignmentRecord(
            identity=local,
            input=assignment.input,
            metadata=assignment.metadata,
            node_envelope=assignment.node_envelope,
            agent_invocation_token=assignment.agent_invocation_token,
            offer_expires_at=assignment.offer_expires_at,
            attempt_deadline_at=assignment.attempt_deadline_at,
            run_deadline_at=assignment.run_deadline_at,
        )
        store = self._store_required()
        record = store.create_assignment(record)
        capacity, inflight = self._capacity_snapshot()
        if record.state == ASSIGNMENT_RECEIVED and (capacity == 0 or inflight >= capacity):
            await self._reject_assignment(record, claimed.delivery_id)
            return
        if record.state == ASSIGNMENT_RECEIVED:
            record = store.advance_assignment(local.assignment_message_id, ASSIGNMENT_ACK_SENT)
        if record.state == ASSIGNMENT_ACK_SENT:
            terminal, confirmation = await self._retry_assignment_operation(
                assignment.attempt_identity,
                lambda: self._transport_required().ack_assignment(
                    {"attempt_identity": assignment.attempt_identity.to_dict()},
                    delivery_id=claimed.delivery_id,
                ),
            )
            if terminal:
                return
            self._validate_confirmation(assignment.attempt_identity, confirmation)
            if attempt_id in self._terminal_attempts:
                return
            record = store.advance_assignment(local.assignment_message_id, ASSIGNMENT_CONFIRMED)
            self._spool_permissions[attempt_id] = (True, True)
            await self._start_confirmed_attempt(
                record, parse_datetime(confirmation["lease_expires_at"])
            )
            return
        if record.state == ASSIGNMENT_CONFIRMED:
            self._spool_permissions[assignment.attempt_identity.attempt_id] = (True, True)
            await self._start_confirmed_attempt(record, None)
            return
        if record.state in {ASSIGNMENT_STARTED, ASSIGNMENT_FINISHED}:
            terminal, confirmation = await self._retry_assignment_operation(
                assignment.attempt_identity,
                lambda: self._transport_required().ack_assignment(
                    {"attempt_identity": assignment.attempt_identity.to_dict()},
                    delivery_id=claimed.delivery_id,
                ),
            )
            if terminal:
                return
            self._validate_confirmation(assignment.attempt_identity, confirmation)
            active = self._active.get(attempt_id)
            if active is not None:
                active.lease_expires_at = parse_datetime(confirmation["lease_expires_at"])

    async def _reject_assignment(self, record: AssignmentRecord, delivery_id: str) -> None:
        store = self._store_required()
        if record.state == ASSIGNMENT_RECEIVED:
            record = store.advance_assignment(
                record.identity.assignment_message_id, ASSIGNMENT_REJECT_SENT
            )
        capacity, inflight = self._capacity_snapshot()
        reason = "NODE_DRAINING" if self._draining else "NODE_AT_CAPACITY"
        terminal, response = await self._retry_assignment_operation(
            record.identity.attempt,
            lambda: self._transport_required().reject_assignment(
                {
                    "attempt_identity": record.identity.attempt.to_dict(),
                    "reason_code": reason,
                    "capacity": capacity,
                    "inflight": inflight,
                },
                delivery_id=delivery_id,
            ),
        )
        if terminal:
            return
        _require_response_keys(
            response,
            required={"attempt_identity", "outcome", "dispatch_state"},
        )
        if response["attempt_identity"] != record.identity.attempt.to_dict():
            raise RuntimeProtocolError("Runtime assignment rejection identity mismatch")
        if response["outcome"] not in {"offer_rejected", "lease_revoked"}:
            raise RuntimeProtocolError("Runtime assignment rejection outcome is invalid")
        _validate_dispatch_state(response["dispatch_state"])
        store.advance_assignment(record.identity.assignment_message_id, ASSIGNMENT_REJECTED)
        store.delete_assignment(record.identity.assignment_message_id)

    async def _retry_assignment_operation(
        self,
        identity: RuntimeAttemptIdentity,
        operation: Callable[[], Awaitable[Any]],
    ) -> tuple[bool, Any]:
        if identity.attempt_id in self._terminal_attempts:
            return True, None
        try:
            value = await self._retry_call(operation)
        except RuntimeRemoteError as exc:
            if await self._converge_assignment_terminal_error(identity, exc):
                return True, None
            raise
        if identity.attempt_id in self._terminal_attempts:
            return True, None
        return False, value

    async def _converge_assignment_terminal_error(
        self,
        identity: RuntimeAttemptIdentity,
        error: RuntimeRemoteError,
    ) -> bool:
        if error.code not in _ASSIGNMENT_TERMINAL_CODES:
            return False
        try:
            record = self._store_required().assignment_for_attempt(identity.attempt_id)
        except RuntimeStoreError as exc:
            if str(exc) == "assignment not found":
                self._terminal_attempts.add(identity.attempt_id)
                return True
            raise
        if record.identity.attempt != identity:
            raise RuntimeProtocolError("Runtime assignment terminal identity mismatch")
        await self._revoke_attempt(record)
        return True

    async def _start_confirmed_attempt(
        self,
        record: AssignmentRecord,
        lease_expires_at: datetime | None,
    ) -> None:
        attempt_id = record.identity.attempt.attempt_id
        if attempt_id in self._active or attempt_id in self._terminal_attempts:
            return
        if record.state != ASSIGNMENT_CONFIRMED:
            raise RuntimeStoreError("handler requires a confirmed assignment")
        record = self._store_required().advance_assignment(
            record.identity.assignment_message_id, ASSIGNMENT_STARTED
        )
        active = _ActiveAttempt(record, asyncio.Event(), lease_expires_at=lease_expires_at)
        self._active[attempt_id] = active
        active.task = self._spawn(self._execute_attempt(active))
        active.renew_task = self._spawn(self._renew_lease_loop(active))

    async def _execute_attempt(self, active: _ActiveAttempt) -> None:
        started = time.monotonic()
        context = RuntimeContext(self, active)
        try:
            raw = await _invoke_handler(self.handler, context)
            context._close()
            result = _normalize_result(raw)
            for event in result.events:
                self._persist_event(active, event)
        except asyncio.CancelledError:
            if active.cancel_event.is_set() or self._force_cancel.is_set():
                return
            result = RuntimeResult.failed(
                "HANDLER_CANCELLED", "handler stopped without a Runtime cancellation"
            )
        except Exception as exc:
            result = RuntimeResult.failed(
                "HANDLER_ERROR", _bounded(str(exc), 500, "handler failed")
            )
        finally:
            context._close()
        if active.cancel_event.is_set() or self._force_cancel.is_set():
            return
        duration_ms = result.duration_ms or max(0, int((time.monotonic() - started) * 1000))
        payload = _result_payload(active.assignment.identity.attempt, result, duration_ms)
        attempt_id = active.assignment.identity.attempt.attempt_id
        async with self._attempt_lock(attempt_id):
            if active.cancel_event.is_set() or self._force_cancel.is_set():
                return
            self._store_required().store_result(attempt_id, payload)
            self._spool_permissions[attempt_id] = (True, True)
            self._spool_wakeup.set()

    def _persist_event(self, active: _ActiveAttempt, event: RuntimeEvent) -> None:
        event.validate()
        self._store_required().append_event(
            active.assignment.identity.attempt.attempt_id,
            event.event_type,
            event.payload,
        )
        self._spool_wakeup.set()

    def _remove_active_attempt(self, attempt_id: str) -> _ActiveAttempt | None:
        active = self._active.pop(attempt_id, None)
        if (
            active is not None
            and active.renew_task is not None
            and active.renew_task is not asyncio.current_task()
        ):
            active.renew_task.cancel()
        return active

    async def _renew_lease_loop(self, active: _ActiveAttempt) -> None:
        retry = 0
        while not self._force_cancel.is_set() and not active.cancel_event.is_set():
            interval = max(0.25, (self._ready.lease_ttl_seconds if self._ready else 60) / 3)
            await self._wait_attempt(active, interval)
            if self._force_cancel.is_set() or active.cancel_event.is_set():
                return
            if active.lease_expires_at and datetime.now(timezone.utc) >= active.lease_expires_at:
                await self._revoke_attempt(active.assignment)
                return
            try:
                if self._transport_transitioning:
                    await self._wait_attempt(active, self.retry_minimum)
                    continue
                record = self._store_required().assignment(
                    active.assignment.identity.assignment_message_id
                )
                capacity, inflight = self._capacity_snapshot()
                transport = self._transport_required()

                async def renew() -> dict[str, Any]:
                    nonlocal transport
                    transport = self._transport_required()
                    return await transport.renew_lease(
                        {
                            "attempt_identity": record.identity.attempt.to_dict(),
                            "last_client_event_seq": record.last_client_event_seq,
                            "capacity": capacity,
                            "inflight": inflight,
                        }
                    )

                renewed = await self._policy_operation(renew)
                _require_response_keys(
                    renewed,
                    required={"attempt_identity", "lease_expires_at"},
                    optional={"pending_command"},
                )
                if renewed.get("attempt_identity") != record.identity.attempt.to_dict():
                    raise RuntimeProtocolError("Runtime lease ACK identity mismatch")
                renewed_until = parse_datetime(renewed["lease_expires_at"])
                if renewed_until <= datetime.now(timezone.utc):
                    raise RuntimeProtocolError("Runtime lease ACK is already expired")
                active.lease_expires_at = renewed_until
                command = renewed.get("pending_command")
                if command is not None:
                    await self._handle_command(_protocol_object(command, "pending command"))
                retry = 0
            except asyncio.CancelledError:
                raise
            except RuntimeRemoteError as exc:
                if is_runtime_policy_recovery_signal(exc):
                    await self._report_fatal(exc)
                    return
                if self._transport_transitioning or (
                    "transport" in locals() and transport is not self._transport_required()
                ):
                    retry = 0
                    continue
                if exc.code in _LEASE_TERMINAL_CODES or exc.code == "RUN_CANCEL_REQUESTED":
                    await self._revoke_attempt(active.assignment)
                    return
                if _fatal_error(exc):
                    await self._report_fatal(exc)
                    return
                await self._wait_attempt(active, self._retry_delay(retry))
                retry += 1
            except Exception as exc:
                if is_runtime_policy_recovery_signal(exc):
                    await self._report_fatal(exc)
                    return
                if self._transport_transitioning or (
                    "transport" in locals() and transport is not self._transport_required()
                ):
                    retry = 0
                    continue
                if _fatal_error(exc):
                    await self._report_fatal(exc)
                    return
                failed = self._transport_required()
                if failed.kind == "ws":
                    try:
                        await self._recover_transport(failed, continue_during_shutdown=True)
                        retry = 0
                        continue
                    except Exception as recovery_error:
                        if _fatal_error(recovery_error):
                            await self._report_fatal(recovery_error)
                            return
                await self._wait_attempt(active, self._retry_delay(retry))
                retry += 1

    async def _heartbeat_loop(self) -> None:
        attempt = 0
        while not self._stopping.is_set():
            await self._wait_or_stop(self.heartbeat_interval)
            if self._stopping.is_set():
                return
            if self._transport_transitioning:
                continue
            transport = self._transport_required()
            try:

                async def heartbeat() -> RuntimeReady:
                    nonlocal transport
                    transport = self._transport_required()
                    return await transport.heartbeat_session(self._hello())

                self._ready = await self._policy_operation(heartbeat)
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_runtime_policy_recovery_signal(exc):
                    await self._report_fatal(exc)
                    return
                if self._transport_transitioning or transport is not self._transport_required():
                    attempt = 0
                    continue
                if _fatal_error(exc):
                    await self._report_fatal(exc)
                    return
                if transport.kind == "ws":
                    try:
                        await self._recover_transport(transport)
                        attempt = 0
                        continue
                    except Exception as recovery_error:
                        if _fatal_error(recovery_error):
                            await self._report_fatal(recovery_error)
                            return
                await self._wait_or_stop(self._retry_delay(attempt))
                attempt += 1

    async def _command_loop(self) -> None:
        attempt = 0
        while not self._stopping.is_set():
            if self._transport_transitioning:
                await self._wait_or_stop(self.retry_minimum)
                continue
            transport = self._transport_required()
            try:

                async def poll_commands() -> list[dict[str, Any]]:
                    nonlocal transport
                    transport = self._transport_required()
                    return await transport.poll_commands(
                        self._store_required().identity.runtime_session_id,
                        int(self.command_wait),
                    )

                commands = await self._policy_operation(poll_commands)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_runtime_policy_recovery_signal(exc):
                    await self._report_fatal(exc)
                    return
                if self._transport_transitioning or transport is not self._transport_required():
                    attempt = 0
                    continue
                if _fatal_error(exc):
                    await self._report_fatal(exc)
                    return
                if transport.kind == "ws":
                    try:
                        await self._recover_transport(transport)
                        attempt = 0
                        continue
                    except Exception as recovery_error:
                        if _fatal_error(recovery_error):
                            await self._report_fatal(recovery_error)
                            return
                await self._wait_or_stop(self._retry_delay(attempt))
                attempt += 1
                continue
            attempt = 0
            for command in commands:
                try:
                    await self._handle_command(command)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self._report_fatal(exc)
                    return

    async def _handle_command(self, command: dict[str, Any]) -> None:
        command_type = str(command.get("type", ""))
        payload = _protocol_object(command.get("payload"), "Runtime command payload")
        if command_type == "run.cancel":
            _require_response_keys(
                payload,
                required={
                    "cancellation_id",
                    "attempt_identity",
                    "reason_code",
                    "deadline_at",
                },
            )
            cancellation_id = str(payload.get("cancellation_id", ""))
            _canonical_protocol_uuid(cancellation_id, "cancellation_id")
            reason_code = payload["reason_code"]
            if not isinstance(reason_code, str) or not 1 <= len(reason_code) <= 120:
                raise RuntimeProtocolError("Runtime cancellation reason is invalid")
            try:
                parse_datetime(payload["deadline_at"])
            except (TypeError, ValueError) as exc:
                raise RuntimeProtocolError("Runtime cancellation deadline is invalid") from exc
            identity = RuntimeAttemptIdentity.from_dict(
                _protocol_object(payload.get("attempt_identity"), "cancel Attempt")
            )
            cancellation_key = (cancellation_id, identity.attempt_id)
            if cancellation_key in self._cancellations:
                return
            self._cancellations.add(cancellation_key)
            self._spawn(self._handle_cancel(payload, cancellation_key))
        elif command_type == "runtime.drain":
            validate_runtime_drain_payload(payload)
            self._draining = True
            await _invoke_optional(self.on_drain)
        elif command_type == "run.lease.revoked":
            _require_response_keys(
                payload,
                required={"attempt_identity", "reason_code", "dispatch_state", "run_status"},
            )
            if (
                not isinstance(payload["reason_code"], str)
                or not 1 <= len(payload["reason_code"]) <= 120
            ):
                raise RuntimeProtocolError("Runtime lease revocation reason is invalid")
            _validate_dispatch_state(payload["dispatch_state"])
            _validate_run_status(payload["run_status"])
            identity = RuntimeAttemptIdentity.from_dict(
                _protocol_object(payload.get("attempt_identity"), "revoked Attempt")
            )
            try:
                record = self._store_required().assignment_for_attempt(identity.attempt_id)
            except RuntimeStoreError as exc:
                if str(exc) != "assignment not found":
                    raise
                return
            if record.identity.attempt == identity:
                await self._revoke_attempt(record)
        else:
            raise RuntimeProtocolError(f"unknown Runtime command {command_type!r}")

    async def _handle_cancel(
        self,
        payload: dict[str, Any],
        cancellation_key: tuple[str, str],
    ) -> None:
        cancellation_id = str(payload.get("cancellation_id", ""))
        _canonical_protocol_uuid(cancellation_id, "cancellation_id")
        identity = RuntimeAttemptIdentity.from_dict(
            _protocol_object(payload.get("attempt_identity"), "cancel Attempt")
        )
        try:
            deadline = parse_datetime(payload["deadline_at"])
            try:
                record = self._store_required().assignment_for_attempt(identity.attempt_id)
            except RuntimeStoreError as exc:
                if str(exc) != "assignment not found":
                    raise
                await self._ack_cancel_best_effort(
                    payload,
                    "failed",
                    "ATTEMPT_IDENTITY_MISMATCH",
                    deadline,
                )
                return
            if record.identity.attempt != identity:
                await self._ack_cancel_best_effort(
                    payload,
                    "failed",
                    "ATTEMPT_IDENTITY_MISMATCH",
                    deadline,
                )
                return

            self._terminal_attempts.add(identity.attempt_id)
            now = datetime.now(timezone.utc)
            initial_remaining = max(0.0, (deadline - now).total_seconds())
            stopping_budget = max(0.001, min(2.0, initial_remaining / 2))
            stopping_deadline = min(deadline, now + timedelta(seconds=stopping_budget))
            stopping_ack = asyncio.create_task(
                self._ack_cancel(payload, "stopping", "", deadline=stopping_deadline)
            )

            active = self._active.get(identity.attempt_id)
            if active is not None:
                active.cancel_event.set()
                if active.task is not None:
                    active.task.cancel()
            async with self._attempt_lock(identity.attempt_id):
                self._spool_permissions.pop(identity.attempt_id, None)

            handler_stopped = True
            if active is not None and active.task is not None:
                timeout = max(0.0, (deadline - datetime.now(timezone.utc)).total_seconds())
                done, _ = await asyncio.wait({active.task}, timeout=timeout)
                handler_stopped = active.task in done

            try:
                await stopping_ack
            except Exception as exc:
                self.logger.warning("Runtime cancel stopping ACK was not confirmed: %s", exc)

            if not handler_stopped:
                await self._ack_cancel_best_effort(
                    payload,
                    "failed",
                    "CANCEL_DEADLINE_EXCEEDED",
                    deadline,
                )
                return
            if not await self._ack_cancel_best_effort(
                payload,
                "stopped",
                "",
                deadline,
            ):
                return
            await self._finalize_revoked_attempt(record)
            self.logger.debug("Runtime cancellation %s stopped", cancellation_id)
        finally:
            self._cancellations.discard(cancellation_key)

    async def _ack_cancel_best_effort(
        self,
        payload: dict[str, Any],
        state: str,
        error_code: str,
        deadline: datetime,
    ) -> bool:
        try:
            await self._ack_cancel(payload, state, error_code, deadline=deadline)
            return True
        except Exception as exc:
            self.logger.warning("Runtime cancel %s ACK was not confirmed: %s", state, exc)
            return False

    async def _ack_cancel(
        self,
        payload: dict[str, Any],
        state: str,
        error_code: str,
        *,
        deadline: datetime | None = None,
    ) -> None:
        request = {
            "cancellation_id": payload["cancellation_id"],
            "attempt_identity": payload["attempt_identity"],
            "cancel_state": state,
        }
        if error_code:
            request["error_code"] = error_code
        deadline = deadline or parse_datetime(payload["deadline_at"])

        async def send() -> dict[str, Any]:
            remaining = max(0.001, (deadline - datetime.now(timezone.utc)).total_seconds())
            return await asyncio.wait_for(
                self._transport_required().ack_cancel(request),
                timeout=remaining,
            )

        response = await self._retry_call(
            send,
            deadline=deadline,
        )
        if response.get("cancellation_id") != payload["cancellation_id"]:
            raise RuntimeProtocolError("Runtime cancellation ACK identity mismatch")
        _require_response_keys(
            response,
            required={"cancellation_id", "cancel_state", "updated_at"},
            optional={"error_code"},
        )
        if response["cancel_state"] != state:
            raise RuntimeProtocolError("Runtime cancellation ACK state mismatch")
        parse_datetime(response["updated_at"])

    async def _spool_loop(self) -> None:
        attempt = 0
        while not self._stopping.is_set():
            if attempt:
                await self._wait_or_stop(self._retry_delay(attempt - 1))
            else:
                try:
                    await asyncio.wait_for(self._spool_wakeup.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
            self._spool_wakeup.clear()
            transport = self._transport_required()
            try:
                if self._transport_transitioning:
                    attempt = max(attempt, 1)
                    continue

                async def flush() -> None:
                    nonlocal transport
                    transport = self._transport_required()
                    await self._flush_spool(transport)

                await self._policy_operation(flush)
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_runtime_policy_recovery_signal(exc):
                    await self._report_fatal(exc)
                    return
                if self._transport_transitioning or transport is not self._transport_required():
                    attempt = max(attempt, 1)
                    continue
                if _fatal_error(exc):
                    await self._report_fatal(exc)
                    return
                attempt += 1

    async def _flush_spool(self, transport: RuntimeTransport) -> None:
        store = self._store_required()
        for assignment in store.assignments():
            attempt_id = assignment.identity.attempt.attempt_id
            lease_terminal = False
            async with self._attempt_lock(attempt_id):
                try:
                    await self._flush_attempt_spool(transport, assignment)
                except RuntimeRemoteError as exc:
                    if exc.code not in _LEASE_TERMINAL_CODES and exc.code != "RUN_CANCEL_REQUESTED":
                        raise
                    lease_terminal = True
            if lease_terminal:
                await self._revoke_attempt(assignment)

    async def _flush_attempt_spool(
        self,
        transport: RuntimeTransport,
        assignment: AssignmentRecord,
    ) -> None:
        store = self._store_required()
        attempt_id = assignment.identity.attempt.attempt_id
        if attempt_id not in self._spool_permissions:
            return
        events_allowed, result_allowed = self._spool_permissions.get(attempt_id, (False, False))
        if events_allowed:
            for event in store.pending_events(attempt_id):
                ack = await transport.send_event(
                    {
                        "attempt_identity": event.identity.attempt.to_dict(),
                        "client_event_id": event.client_event_id,
                        "client_event_seq": event.client_event_seq,
                        "event_type": event.event_type,
                        "payload": event.payload,
                    }
                )
                _validate_event_ack(ack, event.client_event_id, event.client_event_seq)
                store.ack_event(attempt_id, event.client_event_id, event.client_event_seq)
        if not result_allowed or store.pending_events(attempt_id):
            return
        result = store.pending_result(attempt_id)
        if result is None:
            return
        repairs = 0
        while True:
            try:
                ack = await transport.send_result(result.payload)
                break
            except RuntimeRemoteError as exc:
                if exc.code != "EVENTS_MISSING" or repairs >= 1 or not exc.missing_event_ranges:
                    raise
                await self._replay_events(transport, assignment, exc.missing_event_ranges)
                repairs += 1
        _validate_result_ack(ack, result.result_id)
        store.ack_result(attempt_id, result.result_id)
        store.clear_terminal_events(attempt_id)
        store.delete_assignment(assignment.identity.assignment_message_id)
        self._spool_permissions.pop(attempt_id, None)
        self._remove_active_attempt(attempt_id)

    async def _replay_events(
        self,
        transport: RuntimeTransport,
        assignment: AssignmentRecord,
        ranges: list[tuple[int, int]],
    ) -> None:
        attempt_id = assignment.identity.attempt.attempt_id
        for event in self._store_required().events_in_ranges(attempt_id, ranges):
            ack = await transport.send_event(
                {
                    "attempt_identity": event.identity.attempt.to_dict(),
                    "client_event_id": event.client_event_id,
                    "client_event_seq": event.client_event_seq,
                    "event_type": event.event_type,
                    "payload": event.payload,
                }
            )
            _validate_event_ack(ack, event.client_event_id, event.client_event_seq)
            self._store_required().ack_event(
                attempt_id, event.client_event_id, event.client_event_seq
            )

    async def _resume_durable_state(
        self,
        *,
        reconnect: bool,
        transport: RuntimeTransport | None = None,
        continue_during_shutdown: bool = False,
        policy_aware: bool = True,
    ) -> None:
        store = self._store_required()
        records = sorted(store.assignments(), key=lambda item: item.identity.attempt.attempt_id)
        if not records:
            return
        attempts: list[dict[str, Any]] = []
        for record in records:
            pending = store.pending_events(record.identity.attempt.attempt_id)
            result = store.pending_result(record.identity.attempt.attempt_id)
            item: dict[str, Any] = {
                "attempt_identity": record.identity.attempt.to_dict(),
                "last_acked_client_event_seq": record.acked_client_event_seq,
                "pending_client_event_ranges": [
                    {"start": start, "end": end} for start, end in _event_ranges(pending)
                ],
            }
            if result is not None:
                item["pending_result_id"] = result.result_id
                item["final_client_event_seq"] = result.final_client_event_seq
            attempts.append(item)
        decisions = await self._retry_call(
            lambda: (transport or self._transport_required()).resume(
                {
                    "node_id": self.node_id,
                    "agent_id": self.agent_id,
                    "worker_id": store.identity.worker_id,
                    "runtime_session_id": store.identity.runtime_session_id,
                    "attempts": attempts,
                }
            ),
            tolerate_transport_switch=transport is None,
            continue_during_shutdown=continue_during_shutdown,
            policy_aware=policy_aware,
        )
        if len(decisions) != len(records):
            raise RuntimeProtocolError("Runtime resume response count mismatch")
        decisions_by_attempt: dict[str, dict[str, Any]] = {}
        for decision in decisions:
            _require_response_keys(
                decision,
                required={"attempt_identity", "decision", "allowed_actions"},
                optional={"lease_expires_at"},
            )
            identity = RuntimeAttemptIdentity.from_dict(
                _protocol_object(decision.get("attempt_identity"), "resume Attempt identity")
            )
            allowed_actions = decision["allowed_actions"]
            if (
                not isinstance(allowed_actions, list)
                or any(not isinstance(action, str) for action in allowed_actions)
                or len(set(allowed_actions)) != len(allowed_actions)
                or any(
                    action
                    not in {
                        "continue_execution",
                        "upload_events",
                        "upload_result",
                        "stop_execution",
                        "clear_spool",
                    }
                    for action in allowed_actions
                )
            ):
                raise RuntimeProtocolError("Runtime resume actions are invalid")
            if decision.get("lease_expires_at") is not None:
                try:
                    parse_datetime(decision["lease_expires_at"])
                except (TypeError, ValueError) as exc:
                    raise RuntimeProtocolError("Runtime resume lease is invalid") from exc
            if identity.attempt_id in decisions_by_attempt:
                raise RuntimeProtocolError("Runtime resume response contains a duplicate Attempt")
            decisions_by_attempt[identity.attempt_id] = decision
        for record in records:
            decision = decisions_by_attempt.get(record.identity.attempt.attempt_id)
            if decision is None:
                raise RuntimeProtocolError("Runtime resume response is missing an Attempt")
            if decision.get("attempt_identity") != record.identity.attempt.to_dict():
                raise RuntimeProtocolError("Runtime resume response identity mismatch")
            action = decision.get("decision")
            attempt_id = record.identity.attempt.attempt_id
            if action == "continue_execution":
                if reconnect and record.state in {ASSIGNMENT_STARTED, ASSIGNMENT_FINISHED}:
                    active = self._active.get(attempt_id)
                    if active is None:
                        raise RuntimeStoreError(
                            "unsafe reconnect refused: owned Attempt has no live lease owner"
                        )
                    expires = decision.get("lease_expires_at")
                    if expires:
                        active.lease_expires_at = parse_datetime(expires)
                    self._spool_permissions[attempt_id] = (True, True)
                    continue
                if record.state in {ASSIGNMENT_STARTED, ASSIGNMENT_FINISHED}:
                    raise RuntimeStoreError(
                        "unsafe resume refused: a previous process started this Attempt"
                    )
                if record.state == ASSIGNMENT_ACK_SENT:
                    record = store.advance_assignment(
                        record.identity.assignment_message_id, ASSIGNMENT_CONFIRMED
                    )
                if record.state != ASSIGNMENT_CONFIRMED:
                    raise RuntimeStoreError("Runtime resume state is not executable")
                self._spool_permissions[attempt_id] = (True, True)
                expires = decision.get("lease_expires_at")
                await self._start_confirmed_attempt(
                    record, parse_datetime(expires) if expires else None
                )
            elif action == "upload_spool_only":
                allowed = set(decision.get("allowed_actions", []))
                self._spool_permissions[attempt_id] = (
                    "upload_events" in allowed,
                    "upload_result" in allowed,
                )
            elif action in {"result_already_acked", "lease_revoked"}:
                await self._clear_from_resume(record, revoked=action == "lease_revoked")
            else:
                raise RuntimeProtocolError(f"unknown Runtime resume decision {action!r}")
        self._spool_wakeup.set()

    async def _clear_from_resume(self, record: AssignmentRecord, *, revoked: bool) -> None:
        if revoked:
            await self._revoke_attempt(record)
            return
        store = self._store_required()
        attempt_id = record.identity.attempt.attempt_id
        active = self._active.get(attempt_id)
        if active is not None:
            active.cancel_event.set()
            if active.task is not None:
                active.task.cancel()
        async with self._attempt_lock(attempt_id):
            result = store.pending_result(attempt_id)
            if result is not None:
                store.ack_result(attempt_id, result.result_id)
                record = store.assignment(record.identity.assignment_message_id)
            if record.state != ASSIGNMENT_RESULT_ACKED:
                raise RuntimeProtocolError(
                    "Runtime reported an acknowledged Result with no matching durable Result"
                )
            store.clear_terminal_events(attempt_id)
            store.delete_assignment(record.identity.assignment_message_id)
            self._remove_active_attempt(attempt_id)
            self._spool_permissions.pop(attempt_id, None)

    async def _recover_transport(
        self,
        failed: RuntimeTransport,
        *,
        continue_during_shutdown: bool = False,
    ) -> None:
        observed_revision = self._policy_revision
        try:
            await self._recover_transport_once(
                failed, continue_during_shutdown=continue_during_shutdown
            )
        except Exception as exc:
            if not is_runtime_policy_recovery_signal(exc):
                raise
            await self._recover_runtime_policy(observed_revision)

    async def _recover_transport_once(
        self,
        failed: RuntimeTransport,
        *,
        continue_during_shutdown: bool = False,
    ) -> None:
        async with self._claim_switch_lock:
            if failed is not self._transport_required():
                return
            previous = self._transport
            replacement: RuntimeTransport | None = None
            self._transport_transitioning = True
            try:
                if self._test_transport_recovery is not None:
                    replacement = await self._test_transport_recovery()
                elif self._auto_allows_pull_fallback():
                    if self._http_transport is None:
                        raise ConnectionError("HTTP Runtime transport is unavailable")
                    replacement = self._http_transport
                    self._attachment_reason = "websocket_unavailable"
                else:
                    if self._http_transport is None:
                        raise ConnectionError("Runtime transport cannot reconnect")
                    reconnect_reason = resolve_runtime_fallback_reason(
                        self._configured_transport_mode, "same_transport_reconnect"
                    )
                    replacement = await self._connect_websocket_with_retry(
                        retry_all=True,
                        continue_during_shutdown=continue_during_shutdown,
                        fallback_reason=reconnect_reason,
                    )
                    self._attachment_reason = reconnect_reason
                replacement_ready = await self._attach_session(
                    replacement,
                    continue_during_shutdown=continue_during_shutdown,
                )
                await self._resume_durable_state(
                    reconnect=True,
                    transport=replacement,
                    continue_during_shutdown=continue_during_shutdown,
                    policy_aware=False,
                )
                self._transport = replacement
                self._ready = replacement_ready
            except Exception:
                if replacement is not None and replacement is not previous:
                    await replacement.close()
                raise
            finally:
                self._transport_transitioning = False
            if previous is not replacement and previous is not self._http_transport:
                await previous.close()
            self._ensure_websocket_probe_loop()

    async def _websocket_probe_loop(self) -> None:
        attempt = 0
        while not self._stopping.is_set():
            if self._transport_required().kind != "pull":
                return
            delay = (
                self.websocket_probe_interval
                if self.websocket_probe_interval is not None
                else self._retry_delay(attempt)
            )
            await self._wait_or_stop(delay)
            if self._stopping.is_set():
                return
            observed_revision = self._policy_revision
            try:
                self._transport_transitioning = True
                async with self._claim_switch_lock:
                    if self._transport_required().kind != "pull":
                        return
                    previous = self._transport
                    replacement: RuntimeTransport | None = None
                    try:
                        replacement = await asyncio.wait_for(
                            self._probe_websocket_transport(),
                            timeout=self.websocket_probe_timeout,
                        )
                        replacement_ready = await self._attach_session(replacement)
                        await self._resume_durable_state(
                            reconnect=True, transport=replacement, policy_aware=False
                        )
                        self._transport = replacement
                        self._ready = replacement_ready
                        self._attachment_reason = "recovery"
                    except Exception as exc:
                        if replacement is not None:
                            await replacement.close()
                        if previous is not None and not is_runtime_policy_recovery_signal(exc):
                            restored = await self._attach_session(
                                previous, fallback_reason="websocket_unavailable"
                            )
                            await self._resume_durable_state(
                                reconnect=True, transport=previous, policy_aware=False
                            )
                            self._ready = restored
                            self._attachment_reason = "websocket_unavailable"
                        raise
                return
            except Exception as exc:
                if is_runtime_policy_recovery_signal(exc):
                    await self._recover_runtime_policy(observed_revision)
                    if self._transport_required().kind != "pull":
                        return
                    attempt = 0
                    continue
                if _fatal_error(exc):
                    await self._report_fatal(exc)
                    return
                attempt += 1
            finally:
                self._transport_transitioning = False

    async def _revoke_attempt(self, record: AssignmentRecord) -> None:
        attempt_id = record.identity.attempt.attempt_id
        self._terminal_attempts.add(attempt_id)
        active = self._active.get(attempt_id)
        if active is not None:
            active.cancel_event.set()
            if active.task is not None:
                active.task.cancel()
        await self._finalize_revoked_attempt(record)

    async def _finalize_revoked_attempt(self, record: AssignmentRecord) -> None:
        store = self._store_required()
        attempt_id = record.identity.attempt.attempt_id
        async with self._attempt_lock(attempt_id):
            self._spool_permissions.pop(attempt_id, None)
            try:
                current = store.assignment(record.identity.assignment_message_id)
            except RuntimeStoreError as exc:
                if str(exc) != "assignment not found":
                    raise
                self._remove_active_attempt(attempt_id)
                return
            if current.state not in {
                ASSIGNMENT_RESULT_ACKED,
                ASSIGNMENT_REJECTED,
                ASSIGNMENT_REVOKED,
            }:
                current = store.advance_assignment(
                    current.identity.assignment_message_id, ASSIGNMENT_REVOKED
                )
            store.discard_terminal_spool(attempt_id)
            store.delete_assignment(current.identity.assignment_message_id)
            self._remove_active_attempt(attempt_id)

    async def _shutdown(self) -> None:
        self._request_stop("lifecycle")
        if self._store is not None and self._transport is not None:
            try:
                await asyncio.wait_for(
                    self._transport.heartbeat_session(self._hello(capacity=0)), timeout=2
                )
            except Exception:
                pass
        active_tasks = [item.task for item in self._active.values() if item.task is not None]
        if active_tasks:
            _, pending = await asyncio.wait(active_tasks, timeout=self.shutdown_timeout)
            if pending:
                self._force_cancel.set()
                for active in self._active.values():
                    active.cancel_event.set()
                    if active.task is not None:
                        active.task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
        self._force_cancel.set()
        for task in list(self._background):
            if task is not asyncio.current_task():
                task.cancel()
        if self._background:
            await asyncio.gather(*self._background, return_exceptions=True)
        if self._store is not None and self._transport is not None:
            try:
                await asyncio.wait_for(
                    self._flush_spool(self._transport),
                    timeout=min(2.0, max(self.shutdown_timeout, 0.1)),
                )
            except BaseException:
                # Durable records remain available for the next process to resume.
                pass
        if self._store is not None and self._transport is not None:
            identity = self._store.identity
            try:
                await self._transport.close_session(
                    {
                        "node_id": self.node_id,
                        "agent_id": self.agent_id,
                        "worker_id": identity.worker_id,
                        "runtime_session_id": identity.runtime_session_id,
                        "session_epoch": identity.session_epoch,
                        "status": "closed",
                        "reason": "worker_shutdown",
                    }
                )
            except Exception:
                pass
        transports = {id(item): item for item in (self._transport, self._http_transport) if item}
        for transport in transports.values():
            await transport.close()
        if self._credential_manager is not None:
            await self._credential_manager.close()
        if self._store is not None:
            self._store.close()

    def _hello(self, *, capacity: int | None = None) -> dict[str, Any]:
        identity = self._store_required().identity
        effective_capacity, _ = self._capacity_snapshot()
        return runtime_hello(
            node_id=self.node_id,
            agent_id=self.agent_id,
            worker_id=identity.worker_id,
            runtime_session_id=identity.runtime_session_id,
            session_epoch=identity.session_epoch,
            node_version=self.node_version,
            capacity=effective_capacity if capacity is None else capacity,
        )

    def _capacity_snapshot(self) -> tuple[int, int]:
        inflight = len(self._active)
        accepting = self._store is None or self._store.accepts_new_runs()
        capacity = self.capacity if not self._draining and accepting else 0
        return capacity, inflight

    def _local_identity(self, identity: RuntimeAttemptIdentity) -> LocalAttemptIdentity:
        current = self._store_required().identity
        if (
            identity.node_id != self.node_id
            or identity.agent_id != self.agent_id
            or identity.worker_id != current.worker_id
            or identity.runtime_session_id != current.runtime_session_id
        ):
            raise RuntimeProtocolError("Runtime assignment identity conflicts with this Worker")
        return LocalAttemptIdentity.from_attempt(identity, current.session_epoch)

    @staticmethod
    def _validate_confirmation(
        identity: RuntimeAttemptIdentity, confirmation: dict[str, Any]
    ) -> None:
        _require_response_keys(
            confirmation,
            required={"attempt_identity", "attempt_no", "lease_expires_at"},
        )
        if confirmation.get("attempt_identity") != identity.to_dict():
            raise RuntimeProtocolError("Runtime assignment confirmation identity mismatch")
        attempt_no = confirmation["attempt_no"]
        if not isinstance(attempt_no, int) or isinstance(attempt_no, bool) or attempt_no < 1:
            raise RuntimeProtocolError("Runtime assignment confirmation has no Attempt number")
        try:
            lease_expires_at = parse_datetime(confirmation["lease_expires_at"])
        except (TypeError, ValueError) as exc:
            raise RuntimeProtocolError("Runtime assignment confirmation lease is invalid") from exc
        if lease_expires_at <= datetime.now(timezone.utc):
            raise RuntimeProtocolError("Runtime assignment confirmation lease is expired")

    async def _policy_operation(self, operation: Callable[[], Awaitable[Any]]) -> Any:
        # New operations wait for an in-flight recovery before observing the
        # policy revision. Operations already in flight retain the old revision
        # and therefore join the same single-flight incident.
        async with self._policy_recovery_lock:
            if self._policy_terminal_error is not None:
                raise self._policy_terminal_error
            observed_revision = self._policy_revision
        try:
            return await operation()
        except Exception as exc:
            if not is_runtime_policy_recovery_signal(exc):
                raise
            await self._recover_runtime_policy(observed_revision)
            # Exactly one direct retry. A second policy signal becomes a shared
            # terminal error and cannot recurse or reach the transport again.
            try:
                return await operation()
            except Exception as retry_error:
                if not is_runtime_policy_recovery_signal(retry_error):
                    raise
                raise await self._fail_runtime_policy_recovery(retry_error)

    async def _recover_runtime_policy(
        self,
        observed_revision: int,
        *,
        resume_durable: bool = True,
    ) -> RuntimeReady:
        async with self._policy_recovery_lock:
            if self._policy_terminal_error is not None:
                raise self._policy_terminal_error
            if observed_revision != self._policy_revision:
                if self._policy_last_observed == observed_revision:
                    if self._policy_last_error is not None:
                        raise self._policy_last_error
                    if self._ready is None:
                        raise RuntimePolicyRecoveryError(
                            RuntimeError("Runtime policy recovery returned no Ready state")
                        )
                    return self._ready
                if self._ready is None:
                    raise RuntimePolicyRecoveryError(
                        RuntimeError("Runtime policy revision advanced without Ready state")
                    )
                return self._ready
            self._policy_revision += 1
            self._policy_last_observed = observed_revision
            self._policy_last_error = None
            try:
                ready = await self._recover_runtime_policy_once(resume_durable=resume_durable)
                self._ready = ready
                return ready
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                error = (
                    exc
                    if isinstance(exc, RuntimePolicyRecoveryError)
                    else RuntimePolicyRecoveryError(exc)
                )
                self._policy_last_error = error
                self._policy_terminal_error = error
                raise error

    async def _fail_runtime_policy_recovery(
        self, cause: BaseException
    ) -> RuntimePolicyRecoveryError:
        async with self._policy_recovery_lock:
            if self._policy_terminal_error is not None:
                return self._policy_terminal_error
            error = RuntimePolicyRecoveryError(
                RuntimeError("policy signal persisted after one canonical rediscovery")
            )
            error.__cause__ = cause
            self._policy_last_error = error
            self._policy_terminal_error = error
            return error

    async def _recover_runtime_policy_once(self, *, resume_durable: bool) -> RuntimeReady:
        if not self.platform_url:
            raise RuntimePolicyRecoveryError(
                RuntimeError(
                    "canonical rediscovery requires platform_url; "
                    "an explicit runtime_url alone fails closed"
                )
            )
        connection = await discover_runtime_connection(self.platform_url)
        if connection.mtls_required != self._mtls_required:
            raise RuntimePolicyRecoveryError(
                RuntimeError(
                    "Runtime mTLS requirement changed; restart the Worker to apply the new security mode"
                )
            )
        self._apply_transport_policy(connection.policy)
        runtime_url = validate_runtime_origin(
            connection.runtime_origin, allow_loopback_http=not self._mtls_required
        )
        replacement_http = HTTPRuntimeTransport(
            runtime_url,
            self.agent_token,
            self.mtls,
            node_id=self.node_id,
            mtls_required=self._mtls_required,
            credential_manager=self._credential_manager,
        )
        previous_transport = self._transport
        previous_http = self._http_transport
        replacement: RuntimeTransport = replacement_http
        reason = resolve_runtime_fallback_reason(
            self._configured_transport_mode, "policy_rediscovery"
        )
        self.runtime_url = runtime_url
        self._http_transport = replacement_http
        try:
            if self.transport_mode == "ws" or self._auto_prefers_websocket():
                websocket = WebSocketRuntimeTransport(
                    runtime_url,
                    self.agent_token,
                    self.mtls,
                    replacement_http,
                    mtls_required=self._mtls_required,
                    credential_manager=self._credential_manager,
                )
                try:
                    await websocket.connect(self._hello(), fallback_reason=reason)
                    replacement = websocket
                except Exception as exc:
                    await websocket.close()
                    if _fatal_error(exc) or is_runtime_policy_recovery_signal(exc):
                        raise
                    if self.transport_mode == "ws" or not self._auto_allows_pull_fallback():
                        replacement = await self._connect_websocket_with_retry(
                            retry_all=True,
                            fallback_reason=reason,
                        )
                    else:
                        replacement = replacement_http
                        reason = "websocket_unavailable"

            self._transport_transitioning = True
            async with self._claim_switch_lock:
                ready = await self._attach_session(
                    replacement,
                    fallback_reason=reason,
                    policy_aware=False,
                )
                if resume_durable:
                    await self._resume_durable_state(
                        reconnect=True,
                        transport=replacement,
                        policy_aware=False,
                    )
                self._transport = replacement
                self._attachment_reason = reason
        except BaseException:
            await replacement.close()
            if replacement_http is not replacement:
                await replacement_http.close()
            self._http_transport = previous_http
            raise
        finally:
            self._transport_transitioning = False
        for item in {
            id(value): value for value in (previous_transport, previous_http) if value
        }.values():
            if item is not replacement and item is not replacement_http:
                await item.close()
        self._spool_wakeup.set()
        if resume_durable:
            self._ensure_websocket_probe_loop()
        self.logger.debug("Runtime transport policy recovered through canonical rediscovery")
        return ready

    async def _retry_call(
        self,
        operation: Callable[[], Awaitable[Any]],
        *,
        deadline: datetime | None = None,
        tolerate_transport_switch: bool = True,
        continue_during_shutdown: bool = False,
        cancellation: asyncio.Event | None = None,
        policy_aware: bool = True,
    ) -> Any:
        attempt = 0
        while not self._force_cancel.is_set() and (
            continue_during_shutdown or not self._stopping.is_set()
        ):
            if cancellation is not None and cancellation.is_set():
                break
            transport = self._transport_required()
            try:
                if policy_aware:
                    return await self._policy_operation(operation)
                return await operation()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_runtime_policy_recovery_signal(exc):
                    raise
                if tolerate_transport_switch and (
                    self._transport_transitioning or transport is not self._transport_required()
                ):
                    if continue_during_shutdown:
                        await self._wait_force_or_cancel(cancellation, self.retry_minimum)
                    else:
                        await self._wait_or_stop(self.retry_minimum)
                    continue
                if _fatal_error(exc) or (
                    isinstance(exc, RuntimeRemoteError) and exc.code in _LEASE_TERMINAL_CODES
                ):
                    raise
                if deadline is not None and datetime.now(timezone.utc) >= deadline:
                    raise
                if continue_during_shutdown:
                    await self._wait_force_or_cancel(cancellation, self._retry_delay(attempt))
                else:
                    await self._wait_or_stop(self._retry_delay(attempt))
                attempt += 1
        raise asyncio.CancelledError

    async def _report_fatal(self, exc: BaseException) -> None:
        first = self._fatal.empty()
        if first:
            self._fatal.put_nowait(exc)
        self._force_cancel.set()
        self._request_stop("fatal")
        for active in self._active.values():
            active.cancel_event.set()
            if active.task is not None:
                active.task.cancel()
        if first and self.on_fatal is not None:
            try:
                await _invoke_optional(self.on_fatal, exc)
            except BaseException as callback_error:
                self.logger.error("Runtime on_fatal callback failed: %s", callback_error)

    def _spawn(self, awaitable: Awaitable[Any]) -> asyncio.Task[Any]:
        task = asyncio.create_task(awaitable)
        self._background.add(task)
        task.add_done_callback(self._background_task_done)
        return task

    def _ensure_websocket_probe_loop(self) -> None:
        if not self._auto_allows_pull_fallback() or self._transport_required().kind != "pull":
            return
        if self._websocket_probe_task is not None and not self._websocket_probe_task.done():
            return
        task = self._spawn(self._websocket_probe_loop())
        self._websocket_probe_task = task

        def clear(completed: asyncio.Task[Any]) -> None:
            if self._websocket_probe_task is completed:
                self._websocket_probe_task = None

        task.add_done_callback(clear)

    def _background_task_done(self, task: asyncio.Task[Any]) -> None:
        self._background.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and not self._force_cancel.is_set():
            self._spawn(self._report_fatal(error))

    def _transport_required(self) -> RuntimeTransport:
        if self._transport is None:
            raise RuntimeError("Runtime transport is not initialized")
        return self._transport

    def _store_required(self) -> RuntimeStore:
        if self._store is None:
            raise RuntimeError("Runtime store is not initialized")
        return self._store

    def _attempt_lock(self, attempt_id: str) -> asyncio.Lock:
        lock = self._attempt_locks.get(attempt_id)
        if lock is None:
            lock = asyncio.Lock()
            self._attempt_locks[attempt_id] = lock
        return lock

    async def _wait_or_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=max(delay, 0.001))
        except asyncio.TimeoutError:
            pass

    async def _wait_attempt(self, active: _ActiveAttempt, delay: float) -> None:
        stopping = asyncio.create_task(self._force_cancel.wait())
        canceled = asyncio.create_task(active.cancel_event.wait())
        try:
            await asyncio.wait(
                {stopping, canceled}, timeout=max(delay, 0.001), return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            stopping.cancel()
            canceled.cancel()
            await asyncio.gather(stopping, canceled, return_exceptions=True)

    async def _wait_force_or_cancel(
        self,
        cancellation: asyncio.Event | None,
        delay: float,
    ) -> None:
        stopping = asyncio.create_task(self._force_cancel.wait())
        waits = {stopping}
        canceled: asyncio.Task[bool] | None = None
        if cancellation is not None:
            canceled = asyncio.create_task(cancellation.wait())
            waits.add(canceled)
        try:
            await asyncio.wait(
                waits,
                timeout=max(delay, 0.001),
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            stopping.cancel()
            if canceled is not None:
                canceled.cancel()
            await asyncio.gather(*waits, return_exceptions=True)

    def _retry_delay(self, attempt: int) -> float:
        delay = min(self.retry_maximum, self.retry_minimum * (2 ** min(attempt, 16)))
        return random.uniform(delay * 0.8, delay * 1.2)

    async def _probe_websocket_transport(self) -> RuntimeTransport:
        if self._test_transport_recovery is not None:
            return await self._test_transport_recovery()
        if self._http_transport is None:
            raise ConnectionError("HTTP Runtime transport is unavailable")
        websocket = WebSocketRuntimeTransport(
            self.runtime_url,
            self.agent_token,
            self.mtls,
            self._http_transport,
            mtls_required=self._mtls_required,
            credential_manager=self._credential_manager,
        )
        try:
            await websocket.connect(self._hello(), fallback_reason="recovery")
            return websocket
        except BaseException:
            await websocket.close()
            raise

    def _apply_transport_policy(self, policy: RuntimeTransportPolicy) -> None:
        _, order = resolve_runtime_transport_selection(self._configured_transport_mode, policy)
        self._transport_order = order
        self.heartbeat_interval = (
            policy.heartbeat_interval
            if policy.heartbeat_interval is not None
            else self._configured_timing["heartbeat_interval"]
        )
        self.retry_minimum = (
            policy.retry_minimum
            if policy.retry_minimum is not None
            else self._configured_timing["retry_minimum"]
        )
        self.retry_maximum = (
            policy.retry_maximum
            if policy.retry_maximum is not None
            else self._configured_timing["retry_maximum"]
        )
        self.websocket_probe_interval = (
            policy.websocket_probe_interval
            if policy.websocket_probe_interval is not None
            else self._configured_timing["websocket_probe_interval"]
        )
        self.websocket_probe_timeout = (
            policy.websocket_probe_timeout
            if policy.websocket_probe_timeout is not None
            else self._configured_timing["websocket_probe_timeout"]
        )
        self._session_stale_after = policy.session_stale_after or 0.0
        self._validate_config()
        if self._session_stale_after > 0 and self.heartbeat_interval >= self._session_stale_after:
            raise RuntimeProtocolError(
                "OpenLinker Runtime heartbeat interval must be below the Session stale interval"
            )

    def _auto_prefers_websocket(self) -> bool:
        return (
            self.transport_mode == "auto"
            and bool(self._transport_order)
            and self._transport_order[0] == "ws"
        )

    def _auto_allows_pull_fallback(self) -> bool:
        return self._auto_prefers_websocket() and "pull" in self._transport_order[1:]

    def _validate_config(self) -> None:
        if self.node_id:
            _canonical_uuid(self.node_id, "node_id")
        if self.agent_id:
            _canonical_uuid(self.agent_id, "agent_id")
        if not self.agent_token or self.agent_token.startswith("ol_user_"):
            raise ValueError("Agent Token is required and must not be a User Token")
        mtls_fields = sum(
            bool(value) for value in (self.mtls.cert_file, self.mtls.key_file, self.mtls.ca_file)
        )
        if mtls_fields not in {0, 3}:
            raise ValueError("Runtime mTLS files must be configured together")
        if mtls_fields == 3 and (not self.node_id or not self.agent_id):
            raise ValueError("node_id and agent_id are required with explicit mTLS files")
        if mtls_fields == 0 and not self.platform_url:
            raise ValueError("automatic Runtime credentials require platform_url")
        if self.runtime_url:
            # The discovery manifest remains authoritative. This preliminary
            # validation permits only loopback HTTP until discovery confirms
            # that the operator explicitly disabled mTLS.
            validate_runtime_origin(self.runtime_url, allow_loopback_http=mtls_fields == 0)
        else:
            validate_platform_origin(self.platform_url)
        if self._configured_transport_mode not in {"auto", "ws", "pull"}:
            raise ValueError("transport must be 'auto', 'ws', or 'pull'")
        if not callable(self.handler) and not callable(getattr(self.handler, "handle", None)):
            raise ValueError("Runtime handler is required")
        if self.store is None and self.data_dir is None:
            raise ValueError("Runtime data_dir or store is required")
        if isinstance(self.store, MemoryRuntimeStore) and not self.allow_unsafe_memory_store:
            raise ValueError("MemoryRuntimeStore requires allow_unsafe_memory_store=True")
        if self.capacity < 1 or self.capacity > RUNTIME_MAX_CAPACITY:
            raise ValueError(f"capacity must be between 1 and {RUNTIME_MAX_CAPACITY}")
        if (
            min(
                self.claim_wait,
                self.command_wait,
                self.heartbeat_interval,
                self.retry_minimum,
                self.retry_maximum,
                self.websocket_probe_timeout,
                self.shutdown_timeout,
            )
            <= 0
        ):
            raise ValueError("Runtime timing values must be positive")
        if self.retry_maximum < self.retry_minimum:
            raise ValueError("retry_maximum must not be less than retry_minimum")
        if self.websocket_probe_interval is not None and self.websocket_probe_interval <= 0:
            raise ValueError("websocket_probe_interval must be positive")
        if self.claim_wait > 30 or self.command_wait > 30:
            raise ValueError("Runtime claim_wait and command_wait must not exceed 30 seconds")
        if len(self.node_version) > 100:
            raise ValueError("Runtime node_version must not exceed 100 characters")


async def _invoke_handler(
    handler: RuntimeHandler | RuntimeHandlerCallable, context: RuntimeContext
) -> Any:
    target = getattr(handler, "handle", handler)
    result = target(context)
    return await result if inspect.isawaitable(result) else result


async def _invoke_optional(callback: Callable[..., Any] | None, *args: Any) -> None:
    if callback is None:
        return
    result = callback(*args)
    if inspect.isawaitable(result):
        await result


def _normalize_result(value: Any) -> RuntimeResult:
    if isinstance(value, RuntimeResult):
        result = value
    elif value is None:
        result = RuntimeResult.success({})
    elif isinstance(value, dict):
        result = RuntimeResult.success(value)
    else:
        result = RuntimeResult.success({"value": value})
    if result.status == "success":
        if result.error is not None or result.output is None:
            return RuntimeResult.failed(
                "RESULT_INVALID", "successful Runtime result requires output only"
            )
        _object(result.output, "Runtime result output")
    elif result.status == "failed":
        if result.output is not None or result.error is None:
            return RuntimeResult.failed(
                "RESULT_INVALID", "failed Runtime result requires error only"
            )
    else:
        return RuntimeResult.failed("RESULT_INVALID", "Runtime result status is invalid")
    return result


def _result_payload(
    identity: RuntimeAttemptIdentity,
    result: RuntimeResult,
    duration_ms: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "attempt_identity": identity.to_dict(),
        "status": result.status,
        "duration_ms": min(max(duration_ms, 0), 2_147_483_647),
    }
    if result.status == "success":
        payload["output"] = result.output or {}
    else:
        error = result.error or RuntimeHandlerError("HANDLER_ERROR", "handler failed")
        payload["error"] = {
            "error_code": _bounded(error.code, 120, "HANDLER_ERROR"),
            "message": _bounded(error.message, 500, "handler failed"),
            "retryable_hint": False,
        }
    return payload


def _event_ranges(events: list[Any]) -> list[tuple[int, int]]:
    sequences = sorted(item.client_event_seq for item in events)
    if not sequences:
        return []
    ranges: list[tuple[int, int]] = []
    start = end = sequences[0]
    for sequence in sequences[1:]:
        if sequence == end + 1:
            end = sequence
        else:
            ranges.append((start, end))
            start = end = sequence
    ranges.append((start, end))
    return ranges


def _fatal_error(exc: BaseException) -> bool:
    if isinstance(exc, RuntimePolicyRecoveryError) or is_runtime_policy_recovery_signal(exc):
        return True
    if isinstance(exc, (RuntimeProtocolError, RuntimeStoreError)):
        return True
    return isinstance(exc, RuntimeRemoteError) and (
        exc.code in _PERMANENT_CODES or (not exc.retryable and 400 <= exc.status_code < 500)
    )


def _session_conflict(exc: BaseException) -> bool:
    return isinstance(exc, RuntimeRemoteError) and exc.code == "RUNTIME_SESSION_CONFLICT"


class RuntimePolicyRecoveryError(RuntimeError):
    def __init__(self, cause: BaseException) -> None:
        super().__init__(f"OpenLinker Runtime policy recovery failed: {cause}")
        self.cause = cause


def is_runtime_policy_recovery_signal(exc: BaseException) -> bool:
    if isinstance(exc, RuntimePolicyRecoveryError):
        return False
    if isinstance(exc, RuntimeRemoteError):
        return (
            exc.status_code == 403
            and exc.code == "FORBIDDEN"
            and exc.message in {"RUNTIME_TRANSPORT_FORBIDDEN", "RUNTIME_POLICY_CHANGED"}
        )
    if not isinstance(exc, ConnectionClosed):
        return False
    received = getattr(exc, "rcvd", None)
    if received is not None:
        code = getattr(received, "code", None)
        reason = getattr(received, "reason", None)
    else:
        code = getattr(exc, "code", None)
        reason = getattr(exc, "reason", None)
    return code == 1008 and reason == "RUNTIME_POLICY_CHANGED"


def resolve_runtime_fallback_reason(configured: str, transition: str) -> str:
    if configured not in {"auto", "ws", "pull"}:
        raise ValueError(f"invalid configured Runtime transport {configured!r}")
    if transition in {
        "websocket_to_long_poll",
        "failed_websocket_probe_restore_long_poll",
    }:
        return "websocket_unavailable"
    if transition == "long_poll_to_websocket":
        return "recovery"
    if transition in {
        "policy_selected",
        "same_transport_reconnect",
        "policy_rediscovery",
    }:
        return "policy_forced" if configured == "auto" else "explicit"
    raise ValueError(f"invalid Runtime fallback transition {transition!r}")


async def runtime_policy_recover_once(
    operation: Callable[[], Awaitable[Any]],
    recover_policy: Callable[[BaseException], Awaitable[None]],
) -> Any:
    try:
        return await operation()
    except Exception as exc:
        if not is_runtime_policy_recovery_signal(exc):
            raise
        await recover_policy(exc)
        try:
            return await operation()
        except Exception as retry_error:
            if not is_runtime_policy_recovery_signal(retry_error):
                raise
            error = RuntimePolicyRecoveryError(
                RuntimeError("policy signal persisted after one canonical rediscovery")
            )
            raise error from retry_error


def _require_response_keys(
    value: Any,
    *,
    required: set[str],
    optional: set[str] | None = None,
) -> None:
    if not isinstance(value, dict):
        raise RuntimeProtocolError("Runtime response must be a JSON object")
    keys = set(value)
    if required - keys or keys - required - (optional or set()):
        raise RuntimeProtocolError("Runtime response fields do not match the contract")


def _validate_event_ack(value: Any, event_id: str, event_seq: int) -> None:
    _require_response_keys(
        value,
        required={"client_event_id", "client_event_seq", "sequence", "replayed"},
    )
    assert isinstance(value, dict)
    if value["client_event_id"] != event_id or value["client_event_seq"] != event_seq:
        raise RuntimeProtocolError("Runtime Event ACK identity mismatch")
    sequence = value["sequence"]
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise RuntimeProtocolError("Runtime Event ACK sequence is invalid")
    if not isinstance(value["replayed"], bool):
        raise RuntimeProtocolError("Runtime Event ACK replay state is invalid")


def _validate_result_ack(value: Any, result_id: str) -> None:
    _require_response_keys(
        value,
        required={"result_id", "classification", "run_status", "dispatch_state", "replayed"},
        optional={"next_attempt_at"},
    )
    assert isinstance(value, dict)
    if value["result_id"] != result_id:
        raise RuntimeProtocolError("Runtime Result ACK identity mismatch")
    if value["classification"] not in {
        "success",
        "retryable_failure",
        "non_retryable_failure",
        "timeout",
        "canceled",
        "dead_letter",
    }:
        raise RuntimeProtocolError("Runtime Result ACK classification is invalid")
    _validate_run_status(value["run_status"])
    _validate_dispatch_state(value["dispatch_state"])
    if not isinstance(value["replayed"], bool):
        raise RuntimeProtocolError("Runtime Result ACK replay state is invalid")
    if value.get("next_attempt_at") is not None:
        try:
            parse_datetime(value["next_attempt_at"])
        except (TypeError, ValueError) as exc:
            raise RuntimeProtocolError("Runtime Result ACK retry time is invalid") from exc


def _validate_run_status(value: Any) -> None:
    if value not in {"running", "success", "failed", "timeout", "canceled"}:
        raise RuntimeProtocolError("Runtime run status is invalid")


def _validate_dispatch_state(value: Any) -> None:
    if value not in {"pending", "offered", "executing", "retry_wait", "terminal", "dead_letter"}:
        raise RuntimeProtocolError("Runtime dispatch state is invalid")


def _nonnegative_protocol_integer(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RuntimeProtocolError(f"{label} is invalid")
    return value


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return dict(value)


def _protocol_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeProtocolError(f"{label} must be a JSON object")
    return dict(value)


def _validate_run_summary(value: Any) -> dict[str, Any]:
    summary = _protocol_object(value, "delegated Agent response")
    if set(summary) != {"run_id", "status", "dispatch_state"}:
        raise RuntimeProtocolError("delegated Agent response fields do not match the contract")
    _canonical_uuid(str(summary["run_id"]), "delegated run_id")
    status = summary["status"]
    dispatch = summary["dispatch_state"]
    allowed = {
        "running": {"pending", "offered", "executing", "retry_wait"},
        "success": {"terminal"},
        "failed": {"terminal", "dead_letter"},
        "timeout": {"terminal"},
        "canceled": {"terminal"},
    }
    if status not in allowed or dispatch not in allowed[status]:
        raise RuntimeProtocolError("delegated Agent response has inconsistent state")
    return summary


def _canonical_uuid(value: str, label: str) -> None:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"{label} must be a UUID") from exc
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError(f"{label} must be a lowercase non-zero UUID")


def _canonical_protocol_uuid(value: str, label: str) -> None:
    try:
        _canonical_uuid(value, label)
    except ValueError as exc:
        raise RuntimeProtocolError(str(exc)) from exc


def _bounded(value: str, maximum: int, fallback: str) -> str:
    value = value.strip() or fallback
    return value[:maximum]


def _normalize_drain_options(timeout: float | None, reason_code: str) -> tuple[float, str]:
    if timeout is None:
        timeout = DEFAULT_DRAIN_TIMEOUT
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not 0.001 <= timeout <= MAXIMUM_DRAIN_TIMEOUT
    ):
        raise ValueError("RuntimeWorker drain timeout must be between 1ms and 5m")
    if reason_code == "":
        reason_code = DEFAULT_DRAIN_REASON
    if (
        not isinstance(reason_code, str)
        or not 1 <= len(reason_code) <= 120
        or reason_code.strip() != reason_code
        or any(unicodedata.category(char) == "Cc" for char in reason_code)
    ):
        raise ValueError("RuntimeWorker drain reason_code is invalid")
    return float(timeout), reason_code


def _consume_task_exception(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


__all__ = [
    "DEFAULT_CAPACITY",
    "DEFAULT_DRAIN_REASON",
    "RuntimeContext",
    "RuntimeHandler",
    "RuntimeHandlerCallable",
    "RuntimeWorker",
]
