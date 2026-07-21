from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from openlinker import runtime
import openlinker.runtime.worker as runtime_worker_module
from openlinker.runtime.store import AssignmentRecord
from openlinker.runtime.transport import (
    ClaimedAssignment,
    RuntimeDiscoveryConnection,
    RuntimeTransport,
    RuntimeTransportPolicy,
)


NODE_ID = "11111111-1111-4111-8111-111111111111"
AGENT_ID = "22222222-2222-4222-8222-222222222222"
RUN_ID = "33333333-3333-4333-8333-333333333333"
ATTEMPT_ID = "44444444-4444-4444-8444-444444444444"
LEASE_ID = "55555555-5555-4555-8555-555555555555"
TARGET_AGENT_ID = "66666666-6666-4666-8666-666666666666"
CANCELLATION_ID = "77777777-7777-4777-8777-777777777777"
ATTACHMENT_ID = "88888888-8888-4888-8888-888888888888"


def ready(*, lease_ttl_seconds: int = 60) -> runtime.RuntimeReady:
    return runtime.RuntimeReady(
        core_instance_id="core-a",
        attachment_id=ATTACHMENT_ID,
        features=runtime.RUNTIME_REQUIRED_FEATURES,
        offer_ttl_seconds=30,
        lease_ttl_seconds=lease_ttl_seconds,
        database_time=datetime.now(timezone.utc),
    )


def assignment(store: runtime.MemoryRuntimeStore) -> runtime.RuntimeAssignment:
    now = datetime.now(timezone.utc)
    identity = runtime.RuntimeAttemptIdentity(
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        lease_id=LEASE_ID,
        fencing_token=1,
        node_id=NODE_ID,
        agent_id=AGENT_ID,
        worker_id=store.identity.worker_id,
        runtime_session_id=store.identity.runtime_session_id,
    )
    return runtime.RuntimeAssignment(
        attempt_identity=identity,
        offer_no=1,
        offer_expires_at=now + timedelta(seconds=30),
        attempt_deadline_at=now + timedelta(minutes=2),
        run_deadline_at=now + timedelta(minutes=3),
        input={"text": "hello"},
        metadata={"source": "test"},
        node_envelope="ol_ctx_v2.current.payload.signature",
        agent_invocation_token="ol_inv_v2.current.payload.signature",
    )


class FakeTransport:
    def __init__(self, *, kind: str = "pull") -> None:
        self.kind = kind
        self.assignment: runtime.RuntimeAssignment | None = None
        self.claimed = False
        self.commands: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.ack_entered = asyncio.Event()
        self.ack_release = asyncio.Event()
        self.ack_release.set()
        self.event_acked = asyncio.Event()
        self.result_acked = asyncio.Event()
        self.result_entered = asyncio.Event()
        self.result_release = asyncio.Event()
        self.result_release.set()
        self.closed = False
        self.session_closed = False
        self.create_calls = 0
        self.session_conflicts = 0
        self.ack_failures = 0
        self.ack_error: Exception | None = None
        self.reject_error: Exception | None = None
        self.event_failures = 0
        self.result_failures = 0
        self.event_upload_available = True
        self.result_upload_available = True
        self.lease_ttl_seconds = 60
        self.renew_error: Exception | None = None
        self.ack_attempts: list[dict[str, Any]] = []
        self.event_attempts: list[dict[str, Any]] = []
        self.result_attempts: list[dict[str, Any]] = []
        self.renew_attempts: list[dict[str, Any]] = []
        self.cancel_states: list[str] = []
        self.resume_decisions: list[dict[str, Any]] = []
        self.claim_error: Exception | None = None
        self.claim_seen = asyncio.Event()
        self.call_entered = asyncio.Event()
        self.call_release = asyncio.Event()
        self.call_release.set()
        self.call_cancelled = asyncio.Event()
        self.fallback_reasons: list[str] = []

        self.drain_calls: list[dict[str, Any]] = []
        self.drain_entered = asyncio.Event()
        self.drain_release = asyncio.Event()
        self.drain_release.set()
        self.drain_cancelled = asyncio.Event()
        self.drain_handler: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]] | None = None

    async def create_session(
        self, hello: dict[str, Any], *, fallback_reason: str = ""
    ) -> runtime.RuntimeReady:
        del hello
        self.fallback_reasons.append(fallback_reason)
        self.create_calls += 1
        if self.create_calls <= self.session_conflicts:
            raise runtime.RuntimeRemoteError(
                "RUNTIME_SESSION_CONFLICT", "old Session is still active", status_code=409
            )
        return ready(lease_ttl_seconds=self.lease_ttl_seconds)

    async def heartbeat_session(self, hello: dict[str, Any]) -> runtime.RuntimeReady:
        del hello
        error = getattr(self, "heartbeat_error", None)
        if error is not None:
            raise error
        return ready(lease_ttl_seconds=self.lease_ttl_seconds)

    async def drain_session(
        self, runtime_session_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        self.drain_calls.append(dict(request))
        self.drain_entered.set()
        try:
            await self.drain_release.wait()
        except asyncio.CancelledError:
            self.drain_cancelled.set()
            raise
        if self.drain_handler is not None:
            return await self.drain_handler(runtime_session_id, request)
        return {**request, "capacity": 0, "inflight": 0}

    async def close_session(self, request: dict[str, Any]) -> None:
        del request
        self.session_closed = True

    async def claim_assignment(
        self, wait_seconds: int, request: dict[str, Any]
    ) -> ClaimedAssignment | None:
        del wait_seconds, request
        self.claim_seen.set()
        if self.claim_error is not None:
            raise self.claim_error
        if self.assignment is not None and not self.claimed:
            self.claimed = True
            return ClaimedAssignment(self.assignment, "delivery-1")
        await asyncio.sleep(0.005)
        return None

    async def ack_assignment(
        self, request: dict[str, Any], *, delivery_id: str = ""
    ) -> dict[str, Any]:
        del delivery_id
        self.ack_attempts.append(request)
        self.ack_entered.set()
        await self.ack_release.wait()
        if self.ack_error is not None:
            raise self.ack_error
        if len(self.ack_attempts) <= self.ack_failures:
            raise ConnectionError("assignment ACK response was lost")
        return {
            "attempt_identity": request["attempt_identity"],
            "attempt_no": 1,
            "lease_expires_at": (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
        }

    async def reject_assignment(
        self, request: dict[str, Any], *, delivery_id: str = ""
    ) -> dict[str, Any]:
        del delivery_id
        if self.reject_error is not None:
            raise self.reject_error
        return {
            "attempt_identity": request["attempt_identity"],
            "outcome": "offer_rejected",
            "dispatch_state": "pending",
        }

    async def renew_lease(self, request: dict[str, Any]) -> dict[str, Any]:
        self.renew_attempts.append(request)
        if self.renew_error is not None:
            raise self.renew_error
        return {
            "attempt_identity": request["attempt_identity"],
            "lease_expires_at": (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
        }

    async def send_event(self, request: dict[str, Any]) -> dict[str, Any]:
        self.event_attempts.append(request)
        if not self.event_upload_available:
            raise ConnectionError("Event upload is temporarily unavailable")
        if len(self.event_attempts) <= self.event_failures:
            raise ConnectionError("Event ACK response was lost")
        self.event_acked.set()
        return {
            "client_event_id": request["client_event_id"],
            "client_event_seq": request["client_event_seq"],
            "sequence": request["client_event_seq"],
            "replayed": len(self.event_attempts) > 1,
        }

    async def send_result(self, request: dict[str, Any]) -> dict[str, Any]:
        self.result_attempts.append(request)
        self.result_entered.set()
        await self.result_release.wait()
        if not self.result_upload_available:
            raise ConnectionError("Result upload is temporarily unavailable")
        if len(self.result_attempts) <= self.result_failures:
            raise ConnectionError("Result ACK response was lost")
        self.result_acked.set()
        return {
            "result_id": request["result_id"],
            "classification": "success",
            "run_status": "success",
            "dispatch_state": "terminal",
            "replayed": len(self.result_attempts) > 1,
        }

    async def resume(self, request: dict[str, Any]) -> list[dict[str, Any]]:
        if self.resume_decisions:
            return list(self.resume_decisions)
        return [
            {
                "attempt_identity": item["attempt_identity"],
                "decision": "continue_execution",
                "allowed_actions": ["continue_execution", "upload_events", "upload_result"],
                "lease_expires_at": (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
            }
            for item in request["attempts"]
        ]

    async def poll_commands(
        self, runtime_session_id: str, wait_seconds: int
    ) -> list[dict[str, Any]]:
        del runtime_session_id, wait_seconds
        try:
            first = self.commands.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.005)
            return []
        commands = [first]
        while not self.commands.empty():
            commands.append(self.commands.get_nowait())
        return commands

    async def ack_cancel(self, request: dict[str, Any]) -> dict[str, Any]:
        self.cancel_states.append(request["cancel_state"])
        return {
            "cancellation_id": request["cancellation_id"],
            "cancel_state": request["cancel_state"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def call_agent(
        self,
        request: dict[str, Any],
        *,
        node_envelope: str,
        invocation_token: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        del request, node_envelope, invocation_token, idempotency_key
        self.call_entered.set()
        try:
            await self.call_release.wait()
        except asyncio.CancelledError:
            self.call_cancelled.set()
            raise
        return {"run_id": RUN_ID, "status": "running", "dispatch_state": "pending"}

    async def close(self) -> None:
        self.closed = True


def make_worker(
    store: runtime.MemoryRuntimeStore,
    transport: FakeTransport,
    handler: Callable[[runtime.RuntimeContext], Awaitable[Any]],
    *,
    mode: str = "pull",
    shutdown_timeout: float = 0.2,
) -> runtime.RuntimeWorker:
    worker = runtime.RuntimeWorker(
        platform_url="https://platform.example.test",
        node_id=NODE_ID,
        agent_id=AGENT_ID,
        agent_token="ol_agent_test",
        mtls=runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
        store=store,
        allow_unsafe_memory_store=True,
        handler=handler,
        transport=mode,
        claim_wait=0.02,
        command_wait=0.02,
        heartbeat_interval=0.02,
        retry_minimum=0.005,
        retry_maximum=0.01,
        shutdown_timeout=shutdown_timeout,
    )
    worker._transport = transport
    return worker


def make_policy_worker(
    store: runtime.MemoryRuntimeStore,
    *,
    platform_url: str = "https://platform.example.test",
    runtime_url: str = "",
    mode: str = "auto",
) -> runtime.RuntimeWorker:
    return runtime.RuntimeWorker(
        platform_url=platform_url,
        runtime_url=runtime_url,
        node_id=NODE_ID,
        agent_id=AGENT_ID,
        agent_token="ol_agent_test",
        mtls=runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
        store=store,
        allow_unsafe_memory_store=True,
        handler=lambda _context: {},
        transport=mode,
        claim_wait=0.02,
        command_wait=0.02,
        heartbeat_interval=10.0,
        retry_minimum=0.005,
        retry_maximum=0.01,
        websocket_probe_interval=10.0,
        shutdown_timeout=0.2,
    )


@pytest.mark.asyncio
async def test_worker_token_only_mode_skips_runtime_credentials(monkeypatch):
    captured: dict[str, Any] = {}
    transport = FakeTransport()

    async def discover(_platform_url):
        return RuntimeDiscoveryConnection(
            "https://runtime.example.test",
            RuntimeTransportPolicy(("pull",), "pull"),
            mtls_required=False,
        )

    def reject_credentials(*_args, **_kwargs):
        raise AssertionError("token-only startup opened Runtime mTLS credentials")

    def build_http(*_args, **kwargs):
        captured.update(kwargs)
        return transport

    monkeypatch.setattr(runtime_worker_module, "discover_runtime_connection", discover)
    monkeypatch.setattr(runtime_worker_module, "RuntimeCredentialManager", reject_credentials)
    monkeypatch.setattr(runtime_worker_module, "HTTPRuntimeTransport", build_http)
    worker = runtime.RuntimeWorker(
        platform_url="https://platform.example.test",
        node_id=NODE_ID,
        agent_id=AGENT_ID,
        agent_token="ol_agent_token_only",
        store=runtime.MemoryRuntimeStore(),
        allow_unsafe_memory_store=True,
        handler=lambda _context: {},
        transport="pull",
    )

    await worker._setup_transport()
    assert worker._credential_manager is None
    assert captured["node_id"] == NODE_ID
    assert captured["mtls_required"] is False
    assert captured["credential_manager"] is None


@pytest.mark.asyncio
async def test_worker_token_only_mode_requires_configured_identity(monkeypatch):
    async def discover(_platform_url):
        return RuntimeDiscoveryConnection(
            "https://runtime.example.test",
            RuntimeTransportPolicy(("pull",), "pull"),
            mtls_required=False,
        )

    monkeypatch.setattr(runtime_worker_module, "discover_runtime_connection", discover)
    worker = runtime.RuntimeWorker(
        platform_url="https://platform.example.test",
        agent_token="ol_agent_token_only",
        store=runtime.MemoryRuntimeStore(),
        allow_unsafe_memory_store=True,
        handler=lambda _context: {},
        transport="pull",
    )

    with pytest.raises(ValueError, match="required for token-only"):
        await worker._setup_transport()


@pytest.mark.asyncio
async def test_assignment_is_durable_and_confirmed_before_handler_runs():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.assignment = assignment(store)
    transport.ack_release.clear()
    handler_started = asyncio.Event()

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        handler_started.set()
        await context.emit("run.progress", {"step": 1})
        return {"answer": "ok"}

    worker = make_worker(store, transport, handler)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(transport.ack_entered.wait(), timeout=1)
        assert not handler_started.is_set()
        assert store.assignments()[0].state == "ack_sent"
        transport.ack_release.set()
        await asyncio.wait_for(transport.result_acked.wait(), timeout=1)
        assert handler_started.is_set()
    finally:
        await worker.stop()
        await running
    assert transport.session_closed


@pytest.mark.asyncio
async def test_lost_acks_replay_the_same_assignment_event_and_result_ids():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.assignment = assignment(store)
    transport.ack_failures = 1
    transport.event_failures = 1
    transport.result_failures = 1

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        await context.emit("run.progress", {"step": 1})
        return {"answer": 42}

    worker = make_worker(store, transport, handler)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(transport.result_acked.wait(), timeout=2)
    finally:
        await worker.stop()
        await running

    assert len(transport.ack_attempts) == 2
    assert transport.ack_attempts[0] == transport.ack_attempts[1]
    assert len(transport.event_attempts) == 2
    assert (
        transport.event_attempts[0]["client_event_id"]
        == transport.event_attempts[1]["client_event_id"]
    )
    assert len(transport.result_attempts) == 2
    assert transport.result_attempts[0]["result_id"] == transport.result_attempts[1]["result_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    ["RUN_CANCEL_REQUESTED", "STALE_LEASE", "LEASE_EXPIRED", "RUN_ALREADY_TERMINAL"],
)
async def test_assignment_confirmation_terminal_errors_converge_one_attempt(code):
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    offered = assignment(store)
    transport.ack_error = runtime.RuntimeRemoteError(code, "terminal", status_code=409)
    handler_calls = 0

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        nonlocal handler_calls
        del context
        handler_calls += 1
        return {}

    worker = make_worker(store, transport, handler)
    await worker._handle_assignment(ClaimedAssignment(offered, "delivery-terminal"))

    assert handler_calls == 0
    assert store.assignments() == []
    assert offered.attempt_identity.attempt_id in worker._terminal_attempts
    assert worker._fatal.empty()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["started", "finished"])
async def test_duplicate_owned_assignment_terminal_reack_is_benign(state):
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    offered = assignment(store)
    worker = make_worker(store, transport, lambda _context: {})
    local = worker._local_identity(offered.attempt_identity)
    record = store.create_assignment(
        AssignmentRecord(
            identity=local,
            input=offered.input,
            metadata=offered.metadata,
            node_envelope=offered.node_envelope,
            agent_invocation_token=offered.agent_invocation_token,
            offer_expires_at=offered.offer_expires_at,
            attempt_deadline_at=offered.attempt_deadline_at,
            run_deadline_at=offered.run_deadline_at,
        )
    )
    for next_state in ("ack_sent", "confirmed", "started"):
        record = store.advance_assignment(record.identity.assignment_message_id, next_state)
    if state == "finished":
        store.store_result(
            offered.attempt_identity.attempt_id,
            {
                "attempt_identity": offered.attempt_identity.to_dict(),
                "result_id": CANCELLATION_ID,
                "duration_ms": 1,
                "final_client_event_seq": 0,
                "status": "success",
                "output": {},
            },
        )
    transport.ack_error = runtime.RuntimeRemoteError(
        "RUN_ALREADY_TERMINAL", "terminal", status_code=409
    )

    await worker._handle_assignment(ClaimedAssignment(offered, "delivery-duplicate"))

    assert store.assignments() == []
    assert offered.attempt_identity.attempt_id in worker._terminal_attempts


@pytest.mark.asyncio
async def test_assignment_rejection_terminal_error_is_benign():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    offered = assignment(store)
    transport.reject_error = runtime.RuntimeRemoteError(
        "RUN_CANCEL_REQUESTED", "canceled before rejection", status_code=409
    )
    worker = make_worker(store, transport, lambda _context: {})
    worker._draining = True

    await worker._handle_assignment(ClaimedAssignment(offered, "delivery-reject"))

    assert store.assignments() == []
    assert offered.attempt_identity.attempt_id in worker._terminal_attempts


@pytest.mark.asyncio
async def test_finished_handler_keeps_lease_and_capacity_until_result_ack():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.assignment = assignment(store)
    transport.lease_ttl_seconds = 1
    transport.event_upload_available = False
    transport.result_upload_available = False
    handler_calls = 0
    handler_returned = asyncio.Event()
    contexts: list[runtime.RuntimeContext] = []

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        nonlocal handler_calls
        handler_calls += 1
        contexts.append(context)
        await context.emit("run.progress", {"step": 1})
        handler_returned.set()
        return {"answer": "durable"}

    worker = make_worker(store, transport, handler)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(handler_returned.wait(), timeout=1)
        for _ in range(200):
            records = store.assignments()
            if records and records[0].state == "finished":
                break
            await asyncio.sleep(0.005)
        assert store.assignments()[0].state == "finished"

        with pytest.raises(asyncio.CancelledError):
            await contexts[0].emit("run.progress", {"step": "too-late"})

        for _ in range(400):
            if len(transport.renew_attempts) >= 2:
                break
            await asyncio.sleep(0.005)
        assert len(transport.renew_attempts) >= 2
        assert all(request["inflight"] == 1 for request in transport.renew_attempts)
        assert worker._capacity_snapshot() == (1, 1)
        active = worker._active[ATTEMPT_ID]
        assert active.task is not None and active.task.done()
        assert active.renew_task is not None and not active.renew_task.done()

        transport.event_upload_available = True
        await asyncio.wait_for(transport.event_acked.wait(), timeout=1)
        await asyncio.wait_for(transport.result_entered.wait(), timeout=1)

        renewals_before_result_retry = len(transport.renew_attempts)
        for _ in range(400):
            if len(transport.renew_attempts) >= renewals_before_result_retry + 2:
                break
            await asyncio.sleep(0.005)
        assert len(transport.renew_attempts) >= renewals_before_result_retry + 2
        assert worker._capacity_snapshot() == (1, 1)

        transport.result_upload_available = True
        await asyncio.wait_for(transport.result_acked.wait(), timeout=1)
        for _ in range(200):
            if ATTEMPT_ID not in worker._active:
                break
            await asyncio.sleep(0.005)
        assert ATTEMPT_ID not in worker._active
        assert worker._capacity_snapshot() == (1, 0)
        assert store.assignments() == []
        assert handler_calls == 1
        assert len({item["result_id"] for item in transport.result_attempts}) == 1
    finally:
        transport.event_upload_available = True
        transport.result_upload_available = True
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_lease_expiry_releases_finished_attempt_without_retrying_handler():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.assignment = assignment(store)
    transport.lease_ttl_seconds = 1
    transport.result_upload_available = False
    transport.renew_error = runtime.RuntimeRemoteError(
        "LEASE_EXPIRED", "lease expired", status_code=409
    )
    handler_calls = 0

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        nonlocal handler_calls
        del context
        handler_calls += 1
        return {"answer": "too-late"}

    worker = make_worker(store, transport, handler)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(transport.result_entered.wait(), timeout=1)
        for _ in range(400):
            if ATTEMPT_ID not in worker._active:
                break
            await asyncio.sleep(0.005)
        assert ATTEMPT_ID not in worker._active
        assert store.assignments() == []
        assert handler_calls == 1
        assert not transport.result_acked.is_set()
        assert not running.done()
    finally:
        transport.result_upload_available = True
        await worker.stop()
        await running


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,kind", [("pull", "pull"), ("ws", "ws"), ("auto", "ws")])
async def test_session_conflict_retries_only_during_attach(mode: str, kind: str):
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport(kind=kind)
    transport.session_conflicts = 2

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        del context
        return {}

    worker = make_worker(store, transport, handler, mode=mode)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(transport.claim_seen.wait(), timeout=1)
        assert transport.create_calls == 3
    finally:
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_session_conflict_remains_fatal_after_attach():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.heartbeat_error = runtime.RuntimeRemoteError(
        "RUNTIME_SESSION_CONFLICT", "Session was taken over", status_code=409
    )

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        del context
        return {}

    worker = make_worker(store, transport, handler)
    with pytest.raises(runtime.RuntimeRemoteError) as error:
        await worker.run()
    assert error.value.code == "RUNTIME_SESSION_CONFLICT"
    assert transport.create_calls == 1


@pytest.mark.asyncio
async def test_websocket_failure_switches_to_pull_and_probes_websocket_again():
    store = runtime.MemoryRuntimeStore()
    failed_ws = FakeTransport(kind="ws")
    failed_ws.claim_error = ConnectionError("socket closed")
    pull = FakeTransport(kind="pull")
    restored_ws = FakeTransport(kind="ws")
    replacements = iter((pull, restored_ws))

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        del context
        return {}

    worker = make_worker(store, failed_ws, handler, mode="auto")

    async def recover() -> FakeTransport:
        return next(replacements)

    worker._test_transport_recovery = recover
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(restored_ws.claim_seen.wait(), timeout=2)
        assert pull.create_calls >= 1
        assert restored_ws.create_calls >= 1
        assert failed_ws.closed
    finally:
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_finished_attempt_survives_transport_recovery_without_reexecuting_handler():
    store = runtime.MemoryRuntimeStore()
    failed = FakeTransport(kind="ws")
    failed.assignment = assignment(store)
    failed.result_upload_available = False
    restored = FakeTransport(kind="ws")
    handler_calls = 0

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        nonlocal handler_calls
        del context
        handler_calls += 1
        return {"answer": "once"}

    worker = make_worker(store, failed, handler, mode="auto")

    async def recover() -> FakeTransport:
        return restored

    worker._test_transport_recovery = recover
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(failed.result_entered.wait(), timeout=1)
        active = worker._active[ATTEMPT_ID]
        assert active.task is not None and active.task.done()

        await asyncio.wait_for(worker._recover_transport(failed), timeout=1)
        assert worker._active[ATTEMPT_ID] is active
        assert handler_calls == 1

        await asyncio.wait_for(restored.result_acked.wait(), timeout=1)
        assert handler_calls == 1
        assert store.assignments() == []
    finally:
        failed.result_upload_available = True
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_result_already_acked_resume_releases_finished_attempt_without_reexecution():
    store = runtime.MemoryRuntimeStore()
    failed = FakeTransport(kind="ws")
    offered = assignment(store)
    failed.assignment = offered
    failed.result_upload_available = False
    restored = FakeTransport(kind="ws")
    restored.resume_decisions = [
        {
            "attempt_identity": offered.attempt_identity.to_dict(),
            "decision": "result_already_acked",
            "allowed_actions": ["stop_execution", "clear_spool"],
        }
    ]
    handler_calls = 0

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        nonlocal handler_calls
        del context
        handler_calls += 1
        return {"answer": "ack was lost"}

    worker = make_worker(store, failed, handler, mode="auto")

    async def recover() -> FakeTransport:
        return restored

    worker._test_transport_recovery = recover
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(failed.result_entered.wait(), timeout=1)
        await asyncio.wait_for(worker._recover_transport(failed), timeout=1)
        assert ATTEMPT_ID not in worker._active
        assert store.assignments() == []
        assert handler_calls == 1
        assert not restored.result_attempts
    finally:
        failed.result_upload_available = True
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_started_attempt_is_never_reexecuted_after_process_resume():
    from openlinker.runtime.store import (
        ASSIGNMENT_ACK_SENT,
        ASSIGNMENT_CONFIRMED,
        ASSIGNMENT_STARTED,
        AssignmentRecord,
        LocalAttemptIdentity,
    )

    store = runtime.MemoryRuntimeStore()
    offered = assignment(store)
    record = AssignmentRecord(
        identity=LocalAttemptIdentity.from_attempt(offered.attempt_identity, 1),
        input=offered.input,
        metadata=offered.metadata,
        node_envelope=offered.node_envelope,
        agent_invocation_token=offered.agent_invocation_token,
        offer_expires_at=offered.offer_expires_at,
        attempt_deadline_at=offered.attempt_deadline_at,
        run_deadline_at=offered.run_deadline_at,
    )
    store.create_assignment(record)
    message_id = record.identity.assignment_message_id
    for state in (ASSIGNMENT_ACK_SENT, ASSIGNMENT_CONFIRMED, ASSIGNMENT_STARTED):
        store.advance_assignment(message_id, state)
    called = False

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        nonlocal called
        del context
        called = True
        return {}

    transport = FakeTransport()
    worker = make_worker(store, transport, handler)
    with pytest.raises(runtime.RuntimeStoreError, match="previous process started"):
        await worker.run()
    assert not called


@pytest.mark.asyncio
async def test_resume_decisions_are_correlated_by_attempt_not_array_position():
    from openlinker.runtime.store import AssignmentRecord, LocalAttemptIdentity

    store = runtime.MemoryRuntimeStore()
    first = assignment(store)
    second_identity = runtime.RuntimeAttemptIdentity(
        run_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        attempt_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        lease_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        fencing_token=2,
        node_id=first.attempt_identity.node_id,
        agent_id=first.attempt_identity.agent_id,
        worker_id=first.attempt_identity.worker_id,
        runtime_session_id=first.attempt_identity.runtime_session_id,
    )
    records = []
    for offered in (
        first,
        runtime.RuntimeAssignment(**{**first.__dict__, "attempt_identity": second_identity}),
    ):
        record = AssignmentRecord(
            identity=LocalAttemptIdentity.from_attempt(offered.attempt_identity, 1),
            input=offered.input,
            metadata=offered.metadata,
            node_envelope=offered.node_envelope,
            agent_invocation_token=offered.agent_invocation_token,
            offer_expires_at=offered.offer_expires_at,
            attempt_deadline_at=offered.attempt_deadline_at,
            run_deadline_at=offered.run_deadline_at,
        )
        records.append(store.create_assignment(record))

    transport = FakeTransport()
    transport.resume_decisions = [
        {
            "attempt_identity": record.identity.attempt.to_dict(),
            "decision": "lease_revoked",
            "allowed_actions": [],
        }
        for record in reversed(records)
    ]

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        del context
        raise AssertionError("revoked resume must not execute a handler")

    worker = make_worker(store, transport, handler)
    await worker._resume_durable_state(reconnect=False)
    assert store.assignments() == []


@pytest.mark.asyncio
async def test_cancel_is_scoped_to_the_attempt_and_acknowledged():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    offered = assignment(store)
    transport.assignment = offered
    handler_started = asyncio.Event()
    handler_stopped = asyncio.Event()

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        handler_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            assert context.cancelled
            handler_stopped.set()
        return {}

    worker = make_worker(store, transport, handler)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(handler_started.wait(), timeout=1)
        await transport.commands.put(
            {
                "type": "run.cancel",
                "payload": {
                    "cancellation_id": CANCELLATION_ID,
                    "attempt_identity": offered.attempt_identity.to_dict(),
                    "deadline_at": (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat(),
                    "reason_code": "caller_requested",
                },
            }
        )
        await asyncio.wait_for(handler_stopped.wait(), timeout=1)
        for _ in range(100):
            if transport.cancel_states == ["stopping", "stopped"]:
                break
            await asyncio.sleep(0.005)
        assert transport.cancel_states == ["stopping", "stopped"]
        for _ in range(100):
            if not worker._cancellations:
                break
            await asyncio.sleep(0.005)
        assert worker._cancellations == set()
    finally:
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_cancel_deadline_failure_does_not_leak_dedupe_or_stop_worker():
    class SlowStoppingAckTransport(FakeTransport):
        async def ack_cancel(self, request: dict[str, Any]) -> dict[str, Any]:
            self.cancel_states.append(request["cancel_state"])
            if request["cancel_state"] == "stopping":
                await asyncio.Event().wait()
            return {
                "cancellation_id": request["cancellation_id"],
                "cancel_state": request["cancel_state"],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

    store = runtime.MemoryRuntimeStore()
    transport = SlowStoppingAckTransport()
    offered = assignment(store)
    transport.assignment = offered
    handler_started = asyncio.Event()
    ignored_cancel = asyncio.Event()
    release_handler = asyncio.Event()

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        del context
        handler_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            ignored_cancel.set()
            await release_handler.wait()
        return {"late": True}

    worker = make_worker(store, transport, handler)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(handler_started.wait(), timeout=1)
        await transport.commands.put(
            {
                "type": "run.cancel",
                "payload": {
                    "cancellation_id": CANCELLATION_ID,
                    "attempt_identity": offered.attempt_identity.to_dict(),
                    "deadline_at": (
                        datetime.now(timezone.utc) + timedelta(seconds=0.05)
                    ).isoformat(),
                    "reason_code": "caller_requested",
                },
            }
        )
        await asyncio.wait_for(ignored_cancel.wait(), timeout=1)
        for _ in range(200):
            if transport.cancel_states == ["stopping", "failed"]:
                break
            await asyncio.sleep(0.005)

        assert transport.cancel_states == ["stopping", "failed"]
        assert worker._cancellations == set()
        assert not running.done()
        assert store.assignments()
    finally:
        release_handler.set()
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_result_ack_and_cancel_race_is_serialized_per_attempt():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    offered = assignment(store)
    transport.assignment = offered
    transport.result_release.clear()

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        del context
        return {"answer": "done"}

    worker = make_worker(store, transport, handler)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(transport.result_entered.wait(), timeout=1)
        await transport.commands.put(
            {
                "type": "run.cancel",
                "payload": {
                    "cancellation_id": CANCELLATION_ID,
                    "attempt_identity": offered.attempt_identity.to_dict(),
                    "deadline_at": (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat(),
                    "reason_code": "caller_requested",
                },
            }
        )
        for _ in range(100):
            if transport.cancel_states == ["stopping"]:
                break
            await asyncio.sleep(0.005)
        assert transport.cancel_states == ["stopping"]

        transport.result_release.set()
        await asyncio.wait_for(transport.result_acked.wait(), timeout=1)
        for _ in range(100):
            if transport.cancel_states == ["stopping", "stopped"]:
                break
            await asyncio.sleep(0.005)
        assert transport.cancel_states == ["stopping", "stopped"]
        assert not running.done()
    finally:
        transport.result_release.set()
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_runtime_context_call_agent_requires_idempotency_and_validates_summary():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.assignment = assignment(store)
    delegated = asyncio.Event()

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        with pytest.raises(ValueError):
            await context.call_agent(TARGET_AGENT_ID, {}, idempotency_key=" bad ")
        summary = await context.call_agent(
            TARGET_AGENT_ID,
            {"question": "hello"},
            idempotency_key="delegation-1",
        )
        assert summary["status"] == "running"
        delegated.set()
        return {}

    worker = make_worker(store, transport, handler)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(delegated.wait(), timeout=1)
        await asyncio.wait_for(transport.result_acked.wait(), timeout=1)
    finally:
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_handler_return_closes_background_runtime_context_calls():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.assignment = assignment(store)
    transport.result_upload_available = False
    transport.call_release.clear()
    background_calls: list[asyncio.Task[dict[str, Any]]] = []

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        background_calls.append(
            asyncio.create_task(
                context.call_agent(
                    TARGET_AGENT_ID,
                    {"question": "must be scoped"},
                    idempotency_key="background-delegation",
                )
            )
        )
        await transport.call_entered.wait()
        return {"answer": "handler returned"}

    worker = make_worker(store, transport, handler)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(transport.result_entered.wait(), timeout=1)
        await asyncio.wait_for(transport.call_cancelled.wait(), timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await background_calls[0]

        transport.result_upload_available = True
        await asyncio.wait_for(transport.result_acked.wait(), timeout=1)
    finally:
        transport.call_release.set()
        transport.result_upload_available = True
        await worker.stop()
        await running


def test_worker_applies_discovered_transport_selection_and_timings():
    worker = make_worker(
        runtime.MemoryRuntimeStore(),
        FakeTransport(),
        lambda context: {},
        mode="auto",
    )
    worker._apply_transport_policy(
        RuntimeTransportPolicy(
            allowed_transports=("pull", "ws"),
            default_transport="auto",
            heartbeat_interval=20.0,
            session_stale_after=45.0,
            retry_minimum=0.25,
            retry_maximum=15.0,
            websocket_probe_interval=15.0,
            websocket_probe_timeout=10.0,
        )
    )
    assert worker.transport_mode == "auto"
    assert worker._transport_order == ("pull", "ws")
    assert not worker._auto_prefers_websocket()
    assert worker.heartbeat_interval == 20.0
    assert worker.retry_minimum == 0.25
    assert worker.retry_maximum == 15.0
    assert worker.websocket_probe_interval == 15.0
    assert worker.websocket_probe_timeout == 10.0

    worker._apply_transport_policy(
        RuntimeTransportPolicy(
            allowed_transports=("ws", "pull"),
            default_transport="pull",
        )
    )
    assert worker.transport_mode == "auto"
    assert worker._transport_order == ("pull",)
    assert not worker._auto_allows_pull_fallback()
    assert worker.heartbeat_interval == 0.02
    assert worker.retry_minimum == 0.005
    assert worker.retry_maximum == 0.01
    assert worker._session_stale_after == 0.0


@pytest.mark.asyncio
async def test_worker_coalesces_concurrent_policy_signals_into_one_rediscovery(
    monkeypatch,
):
    entered = asyncio.Event()
    release = asyncio.Event()
    replacement_used = asyncio.Event()
    failing_calls = 0
    replacement_calls = 0
    discovery_calls = 0
    factory_calls = 0

    def policy_signal() -> runtime.RuntimeRemoteError:
        return runtime.RuntimeRemoteError("FORBIDDEN", "RUNTIME_POLICY_CHANGED", status_code=403)

    class InitialPolicyTransport(FakeTransport):
        async def _fail_together(self):
            nonlocal failing_calls
            failing_calls += 1
            if failing_calls == 2:
                entered.set()
            await release.wait()
            raise policy_signal()

        async def claim_assignment(self, _wait, _request):
            return await self._fail_together()

        async def poll_commands(self, _session, _wait):
            return await self._fail_together()

    class ReplacementTransport(FakeTransport):
        async def claim_assignment(self, wait_seconds, request):
            nonlocal replacement_calls
            replacement_calls += 1
            if replacement_calls >= 2:
                replacement_used.set()
            return await super().claim_assignment(wait_seconds, request)

        async def poll_commands(self, runtime_session_id, wait_seconds):
            nonlocal replacement_calls
            replacement_calls += 1
            if replacement_calls >= 2:
                replacement_used.set()
            return await super().poll_commands(runtime_session_id, wait_seconds)

    transports = [InitialPolicyTransport(), ReplacementTransport()]

    async def discover(_platform_url):
        nonlocal discovery_calls
        discovery_calls += 1
        return RuntimeDiscoveryConnection(
            f"https://runtime-{discovery_calls}.example.test",
            RuntimeTransportPolicy(("pull",), "auto"),
        )

    def build_http(*_args, **_kwargs):
        nonlocal factory_calls
        transport = transports[factory_calls]
        factory_calls += 1
        return transport

    monkeypatch.setattr(runtime_worker_module, "discover_runtime_connection", discover)
    monkeypatch.setattr(runtime_worker_module, "HTTPRuntimeTransport", build_http)
    store = runtime.MemoryRuntimeStore()
    worker = make_policy_worker(store)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        identity = store.identity
        release.set()
        await asyncio.wait_for(replacement_used.wait(), timeout=1)
        assert discovery_calls == 2
        assert factory_calls == 2
        assert store.identity == identity
        assert transports[0].fallback_reasons == ["policy_forced"]
        assert transports[1].fallback_reasons == ["policy_forced"]
    finally:
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_worker_returns_second_policy_signal_without_another_rediscovery(monkeypatch):
    discovery_calls = 0
    factory_calls = 0

    class AlwaysPolicyTransport(FakeTransport):
        async def claim_assignment(self, _wait, _request):
            raise runtime.RuntimeRemoteError(
                "FORBIDDEN", "RUNTIME_TRANSPORT_FORBIDDEN", status_code=403
            )

    transports = [AlwaysPolicyTransport(), AlwaysPolicyTransport()]

    async def discover(_platform_url):
        nonlocal discovery_calls
        discovery_calls += 1
        return RuntimeDiscoveryConnection(
            f"https://runtime-{discovery_calls}.example.test",
            RuntimeTransportPolicy(("pull",), "auto"),
        )

    def build_http(*_args, **_kwargs):
        nonlocal factory_calls
        transport = transports[factory_calls]
        factory_calls += 1
        return transport

    monkeypatch.setattr(runtime_worker_module, "discover_runtime_connection", discover)
    monkeypatch.setattr(runtime_worker_module, "HTTPRuntimeTransport", build_http)
    worker = make_policy_worker(runtime.MemoryRuntimeStore(), mode="pull")
    with pytest.raises(
        RuntimeError,
        match=(
            "OpenLinker Runtime policy recovery failed: "
            "policy signal persisted after one canonical rediscovery"
        ),
    ) as terminal:
        await asyncio.wait_for(worker.run(), timeout=1)
    assert discovery_calls == 2
    assert factory_calls == 2
    later_operation_calls = 0

    async def later_operation():
        nonlocal later_operation_calls
        later_operation_calls += 1

    with pytest.raises(RuntimeError) as repeated:
        await worker._policy_operation(later_operation)
    assert repeated.value is terminal.value
    assert later_operation_calls == 0


@pytest.mark.asyncio
async def test_worker_policy_recovery_fails_closed_without_platform_or_allowed_explicit_transport(
    monkeypatch,
):
    class PolicyTransport(FakeTransport):
        async def claim_assignment(self, _wait, _request):
            raise runtime.RuntimeRemoteError("FORBIDDEN", "RUNTIME_POLICY_CHANGED", status_code=403)

    without_platform = PolicyTransport()
    monkeypatch.setattr(
        runtime_worker_module,
        "HTTPRuntimeTransport",
        lambda *_args, **_kwargs: without_platform,
    )
    worker = make_policy_worker(
        runtime.MemoryRuntimeStore(),
        platform_url="",
        runtime_url="https://runtime.example.test",
        mode="pull",
    )
    with pytest.raises(RuntimeError, match="canonical rediscovery requires platform_url"):
        await asyncio.wait_for(worker.run(), timeout=1)

    discovery_calls = 0
    factory_calls = 0

    async def discover(_platform_url):
        nonlocal discovery_calls
        discovery_calls += 1
        allowed = ("pull",) if discovery_calls == 1 else ("ws",)
        return RuntimeDiscoveryConnection(
            f"https://runtime-{discovery_calls}.example.test",
            RuntimeTransportPolicy(allowed, "auto"),
        )

    def build_http(*_args, **_kwargs):
        nonlocal factory_calls
        factory_calls += 1
        return PolicyTransport()

    monkeypatch.setattr(runtime_worker_module, "discover_runtime_connection", discover)
    monkeypatch.setattr(runtime_worker_module, "HTTPRuntimeTransport", build_http)
    incompatible = make_policy_worker(runtime.MemoryRuntimeStore(), mode="pull")
    with pytest.raises(RuntimeError, match="configured Runtime transport 'pull' is not allowed"):
        await asyncio.wait_for(incompatible.run(), timeout=1)
    assert discovery_calls == 2
    assert factory_calls == 1


@pytest.mark.asyncio
async def test_worker_policy_recovery_rejects_mtls_requirement_change(monkeypatch):
    discovery_calls = 0
    factory_calls = 0

    class PolicyTransport(FakeTransport):
        async def claim_assignment(self, _wait, _request):
            raise runtime.RuntimeRemoteError(
                "FORBIDDEN", "RUNTIME_POLICY_CHANGED", status_code=403
            )

    async def discover(_platform_url):
        nonlocal discovery_calls
        discovery_calls += 1
        return RuntimeDiscoveryConnection(
            f"https://runtime-{discovery_calls}.example.test",
            RuntimeTransportPolicy(("pull",), "pull"),
            mtls_required=discovery_calls == 1,
        )

    def build_http(*_args, **_kwargs):
        nonlocal factory_calls
        factory_calls += 1
        return PolicyTransport()

    monkeypatch.setattr(runtime_worker_module, "discover_runtime_connection", discover)
    monkeypatch.setattr(runtime_worker_module, "HTTPRuntimeTransport", build_http)
    worker = make_policy_worker(runtime.MemoryRuntimeStore(), mode="pull")

    with pytest.raises(RuntimeError, match="mTLS requirement changed"):
        await asyncio.wait_for(worker.run(), timeout=1)
    assert discovery_calls == 2
    assert factory_calls == 1


@pytest.mark.asyncio
async def test_worker_rediscovers_once_on_established_websocket_policy_close(monkeypatch):
    discovery_calls = 0
    socket_calls = 0
    close_socket = asyncio.Event()
    recovered = asyncio.Event()
    connect_reasons: list[str] = []

    class PolicyWebSocket(FakeTransport):
        def __init__(self, *_args, **_kwargs):
            super().__init__(kind="ws")
            nonlocal socket_calls
            self.index = socket_calls
            socket_calls += 1

        async def connect(self, _hello, *, fallback_reason=""):
            connect_reasons.append(fallback_reason)
            if self.index == 1:
                recovered.set()
            return ready()

        async def claim_assignment(self, wait_seconds, request):
            if self.index == 0:
                await close_socket.wait()
                raise ConnectionClosedError(Close(1008, "RUNTIME_POLICY_CHANGED"), None)
            return await super().claim_assignment(wait_seconds, request)

    async def discover(_platform_url):
        nonlocal discovery_calls
        discovery_calls += 1
        return RuntimeDiscoveryConnection(
            f"https://runtime-{discovery_calls}.example.test",
            RuntimeTransportPolicy(("ws",), "auto"),
        )

    monkeypatch.setattr(runtime_worker_module, "discover_runtime_connection", discover)
    monkeypatch.setattr(
        runtime_worker_module,
        "HTTPRuntimeTransport",
        lambda *_args, **_kwargs: FakeTransport(),
    )
    monkeypatch.setattr(runtime_worker_module, "WebSocketRuntimeTransport", PolicyWebSocket)
    worker = make_policy_worker(runtime.MemoryRuntimeStore())
    running = asyncio.create_task(worker.run())
    try:
        while socket_calls < 1:
            await asyncio.sleep(0)
        close_socket.set()
        await asyncio.wait_for(recovered.wait(), timeout=1)
        assert discovery_calls == 2
        assert socket_calls == 2
        assert connect_reasons == ["policy_forced", "policy_forced"]
    finally:
        await worker.stop()
        await running


@pytest.mark.asyncio
async def test_graceful_stop_finishes_active_handler_and_flushes_its_spool():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.assignment = assignment(store)
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        handler_started.set()
        await release_handler.wait()
        await context.emit("run.progress", {"phase": "shutdown"})
        return {"answer": "completed while draining"}

    worker = make_worker(store, transport, handler, shutdown_timeout=1)
    running = asyncio.create_task(worker.run())
    await asyncio.wait_for(handler_started.wait(), timeout=1)

    stopping = asyncio.create_task(worker.stop())
    await asyncio.wait_for(worker._stopping.wait(), timeout=1)
    assert not stopping.done()
    release_handler.set()

    await asyncio.wait_for(stopping, timeout=1)
    await running
    assert transport.event_acked.is_set()
    assert transport.result_acked.is_set()
    assert transport.event_attempts[0]["payload"] == {"phase": "shutdown"}
    assert transport.result_attempts[0]["output"] == {"answer": "completed while draining"}


@pytest.mark.asyncio
async def test_shutdown_timeout_cancels_without_fabricating_a_result(tmp_path: Path):
    data_dir = tmp_path / "runtime"
    store = runtime.FileRuntimeStore(data_dir)
    transport = FakeTransport()
    transport.assignment = assignment(store)
    handler_started = asyncio.Event()
    handler_stopped = asyncio.Event()

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        del context
        handler_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            handler_stopped.set()
        return {"must_not": "be submitted"}

    worker = make_worker(store, transport, handler, shutdown_timeout=0.02)
    running = asyncio.create_task(worker.run())
    await asyncio.wait_for(handler_started.wait(), timeout=1)
    await asyncio.wait_for(worker.stop(), timeout=1)
    await running

    assert handler_stopped.is_set()
    assert not transport.result_attempts
    reopened = runtime.FileRuntimeStore(data_dir)
    try:
        assert reopened.assignments()[0].state == "started"
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_drain_waits_for_core_fence_handler_and_durable_spool_ack():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.assignment = assignment(store)
    transport.drain_release.clear()
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()
    transport.result_release.clear()
    drain_count = 0

    async def authoritative_drain(
        runtime_session_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        nonlocal drain_count
        assert runtime_session_id == store.identity.runtime_session_id
        drain_count += 1
        return {
            **request,
            "deadline_at": (
                datetime.fromisoformat(str(request["deadline_at"]).replace("Z", "+00:00"))
                - timedelta(seconds=1)
            ).isoformat(),
            "reason_code": "FIRST_WRITER_REASON",
            "capacity": 0,
            "inflight": 1 if drain_count == 1 else 0,
        }

    transport.drain_handler = authoritative_drain

    async def handler(_context: runtime.RuntimeContext) -> dict[str, Any]:
        handler_started.set()
        await release_handler.wait()
        return {"drained": True}

    worker = make_worker(store, transport, handler, shutdown_timeout=1)
    running = asyncio.create_task(worker.run())
    await asyncio.wait_for(handler_started.wait(), timeout=1)
    draining = asyncio.create_task(worker.drain(timeout=2, reason_code="DEPLOYMENT"))
    await asyncio.wait_for(transport.drain_entered.wait(), timeout=1)
    assert worker._draining
    assert worker._capacity_snapshot() == (0, 1)
    assert not draining.done()
    transport.drain_release.set()
    await asyncio.sleep(0.02)
    assert not draining.done()
    release_handler.set()
    await asyncio.wait_for(transport.result_entered.wait(), timeout=1)
    assert not draining.done()
    transport.result_release.set()
    await asyncio.wait_for(draining, timeout=2)
    await running

    assert drain_count >= 2
    assert transport.drain_calls[0]["reason_code"] == "DEPLOYMENT"
    assert transport.drain_calls[0]["capacity"] == 0
    assert transport.drain_calls[0]["inflight"] == 1
    assert transport.session_closed


@pytest.mark.asyncio
async def test_drain_timeout_fails_closed_and_preserves_durable_spool(tmp_path: Path):
    data_dir = tmp_path / "runtime-drain"
    store = runtime.FileRuntimeStore(data_dir)
    transport = FakeTransport()
    transport.assignment = assignment(store)
    transport.result_upload_available = False
    result_started = asyncio.Event()

    async def handler(context: runtime.RuntimeContext) -> dict[str, Any]:
        await context.emit("run.progress", {"phase": "durable"})
        result_started.set()
        return {"pending": True}

    worker = make_worker(store, transport, handler, shutdown_timeout=0.05)
    running = asyncio.create_task(worker.run())
    await asyncio.wait_for(result_started.wait(), timeout=1)
    await asyncio.wait_for(transport.result_entered.wait(), timeout=1)
    with pytest.raises(runtime.RuntimeDrainTimeoutError) as raised:
        await worker.drain(timeout=0.04)
    assert raised.value.spool == runtime.RuntimeSpoolStatus(assignments=1, events=1, results=1)
    await asyncio.wait_for(running, timeout=2)

    reopened = runtime.FileRuntimeStore(data_dir)
    try:
        assert reopened.spool_status() == runtime.RuntimeSpoolStatus(
            assignments=1, events=1, results=1
        )
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_drain_timeout_cancels_blocked_core_request_and_fails_closed():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.drain_release.clear()
    worker = make_worker(store, transport, lambda _context: {"unused": True})
    running = asyncio.create_task(worker.run())
    while worker._ready is None:
        await asyncio.sleep(0)
    with pytest.raises(runtime.RuntimeDrainTimeoutError) as raised:
        await worker.drain(timeout=0.02)
    assert raised.value.spool.empty
    await asyncio.wait_for(transport.drain_cancelled.wait(), timeout=1)
    await running


@pytest.mark.asyncio
async def test_cancelled_drain_caller_does_not_cancel_singleflight_operation():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    transport.drain_release.clear()
    worker = make_worker(store, transport, lambda _context: {"unused": True})
    running = asyncio.create_task(worker.run())
    while worker._ready is None:
        await asyncio.sleep(0)
    first = asyncio.create_task(worker.drain(timeout=1))
    await asyncio.wait_for(transport.drain_entered.wait(), timeout=1)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert not worker._stopping.is_set()
    follower = asyncio.create_task(worker.drain(timeout=1))
    transport.drain_release.set()
    await follower
    await running


@pytest.mark.asyncio
async def test_drain_waits_for_attach_and_retries_transport_switch():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    attach_entered = asyncio.Event()
    release_attach = asyncio.Event()
    original_create = transport.create_session
    attempts = 0

    async def blocked_create(
        hello: dict[str, Any], *, fallback_reason: str = ""
    ) -> runtime.RuntimeReady:
        attach_entered.set()
        await release_attach.wait()
        return await original_create(hello, fallback_reason=fallback_reason)

    async def switched_drain(_session_id: str, request: dict[str, Any]) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("Runtime transport is switching")
        return {**request, "capacity": 0, "inflight": 0}

    transport.create_session = blocked_create  # type: ignore[method-assign]
    transport.drain_handler = switched_drain
    worker = make_worker(store, transport, lambda _context: {"unused": True})
    running = asyncio.create_task(worker.run())
    await asyncio.wait_for(attach_entered.wait(), timeout=1)
    draining = asyncio.create_task(worker.drain(timeout=1))
    await asyncio.sleep(0.02)
    assert not draining.done()
    release_attach.set()
    await asyncio.wait_for(draining, timeout=2)
    await running
    assert attempts == 2


@pytest.mark.asyncio
async def test_drain_cannot_succeed_when_stop_wins_after_final_evidence():
    store = runtime.MemoryRuntimeStore()
    transport = FakeTransport()
    worker = make_worker(store, transport, lambda _context: {"unused": True})
    proof_reached = asyncio.Event()
    release_proof = asyncio.Event()

    async def barrier() -> None:
        proof_reached.set()
        await release_proof.wait()

    worker._drain_before_stop = barrier
    running = asyncio.create_task(worker.run())
    while worker._ready is None:
        await asyncio.sleep(0)
    draining = asyncio.create_task(worker.drain(timeout=1))
    await asyncio.wait_for(proof_reached.wait(), timeout=1)
    stopping = asyncio.create_task(worker.stop())
    await asyncio.wait_for(worker._stopping.wait(), timeout=1)
    assert worker._stop_owner == "external"
    release_proof.set()
    with pytest.raises(RuntimeError, match="stopped before its durable drain completed"):
        await draining
    await stopping
    await running


@pytest.mark.asyncio
async def test_drain_rejects_every_malformed_core_ack():
    invalid_responses = [
        {
            "deadline_at": datetime.now(timezone.utc).isoformat(),
            "reason_code": "DEPLOYMENT",
            "capacity": 0,
            "inflight": 0,
            "extra": True,
        },
        {
            "deadline_at": datetime.now(timezone.utc).isoformat(),
            "reason_code": "DEPLOYMENT",
            "capacity": 1,
            "inflight": 0,
        },
        {
            "deadline_at": datetime.now(timezone.utc).isoformat(),
            "reason_code": "DEPLOYMENT",
            "capacity": 0,
            "inflight": True,
        },
        {
            "deadline_at": datetime.now(timezone.utc).isoformat(),
            "reason_code": "",
            "capacity": 0,
            "inflight": 0,
        },
        {
            "deadline_at": "not-a-timestamp",
            "reason_code": "DEPLOYMENT",
            "capacity": 0,
            "inflight": 0,
        },
    ]
    for invalid in invalid_responses:
        store = runtime.MemoryRuntimeStore()
        transport = FakeTransport()

        async def invalid_ack(
            _runtime_session_id: str,
            _request: dict[str, Any],
            response: dict[str, Any] = invalid,
        ) -> dict[str, Any]:
            return response

        transport.drain_handler = invalid_ack
        worker = make_worker(store, transport, lambda _context: {"unused": True})
        running = asyncio.create_task(worker.run())
        while worker._ready is None:
            await asyncio.sleep(0)
        with pytest.raises(runtime.RuntimeProtocolError):
            await worker.drain(timeout=1)
        await running


@pytest.mark.asyncio
async def test_drain_preserves_legacy_custom_store_and_transport_extensions():
    assert "drain_session" not in RuntimeTransport.__dict__

    class LegacyStore:
        def __init__(self) -> None:
            self.inner = runtime.MemoryRuntimeStore()

        def __getattr__(self, name: str) -> Any:
            if name == "spool_status":
                raise AttributeError(name)
            return getattr(self.inner, name)

    class LegacyTransport:
        def __init__(self) -> None:
            self.inner = FakeTransport()
            self.kind = self.inner.kind

        def __getattr__(self, name: str) -> Any:
            if name == "drain_session":
                raise AttributeError(name)
            return getattr(self.inner, name)

    legacy_store = LegacyStore()
    transport = FakeTransport()
    worker = make_worker(legacy_store, transport, lambda _context: {"unused": True})  # type: ignore[arg-type]
    running = asyncio.create_task(worker.run())
    while worker._ready is None:
        await asyncio.sleep(0)
    await worker.drain(timeout=1)
    await running

    store = runtime.MemoryRuntimeStore()
    legacy_transport = LegacyTransport()
    legacy_worker = make_worker(
        store,
        legacy_transport,  # type: ignore[arg-type]
        lambda _context: {"unused": True},
    )
    legacy_running = asyncio.create_task(legacy_worker.run())
    while legacy_worker._ready is None:
        await asyncio.sleep(0)
    with pytest.raises(runtime.RuntimeProtocolError, match="does not implement session drain"):
        await legacy_worker.drain(timeout=1)
    await legacy_running


def test_memory_store_requires_an_explicit_unsafe_opt_in():
    with pytest.raises(ValueError, match="allow_unsafe_memory_store"):
        runtime.RuntimeWorker(
            platform_url="https://platform.example.test",
            node_id=NODE_ID,
            agent_id=AGENT_ID,
            agent_token="ol_agent_test",
            mtls=runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
            store=runtime.MemoryRuntimeStore(),
            handler=lambda context: {},
        )


def test_worker_rejects_user_token_and_partial_explicit_mtls():
    common = {
        "platform_url": "https://platform.example.test",
        "node_id": NODE_ID,
        "agent_id": AGENT_ID,
        "store": runtime.MemoryRuntimeStore(),
        "allow_unsafe_memory_store": True,
        "handler": lambda context: {},
    }
    with pytest.raises(ValueError, match="Agent Token"):
        runtime.RuntimeWorker(
            **common,
            agent_token="ol_user_wrong",
            mtls=runtime.RuntimeMTLS("client.crt", "client.key", "ca.crt"),
        )
    with pytest.raises(ValueError, match="mTLS"):
        runtime.RuntimeWorker(
            **common,
            agent_token="ol_agent_test",
            mtls=runtime.RuntimeMTLS("client.crt", "", ""),
        )
