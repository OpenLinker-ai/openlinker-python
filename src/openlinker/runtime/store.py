from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import stat
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .types import (
    RuntimeAttemptIdentity,
    RuntimeStoreCapacity,
    RuntimeStoreCorrupt,
    RuntimeStoreError,
    RuntimeStoreLocked,
    RuntimeSpoolStatus,
    deterministic_uuid,
    format_datetime,
    parse_datetime,
    wire_json_bytes,
)


ASSIGNMENT_RECEIVED = "received"
ASSIGNMENT_ACK_SENT = "ack_sent"
ASSIGNMENT_CONFIRMED = "confirmed"
ASSIGNMENT_STARTED = "started"
ASSIGNMENT_FINISHED = "finished"
ASSIGNMENT_RESULT_ACKED = "result_acked"
ASSIGNMENT_REJECT_SENT = "reject_sent"
ASSIGNMENT_REJECTED = "rejected"
ASSIGNMENT_REVOKED = "revoked"

_TRANSITIONS = {
    ASSIGNMENT_RECEIVED: {ASSIGNMENT_ACK_SENT, ASSIGNMENT_REJECT_SENT, ASSIGNMENT_REVOKED},
    ASSIGNMENT_ACK_SENT: {ASSIGNMENT_CONFIRMED, ASSIGNMENT_REVOKED},
    ASSIGNMENT_CONFIRMED: {ASSIGNMENT_STARTED, ASSIGNMENT_REVOKED},
    ASSIGNMENT_STARTED: {ASSIGNMENT_FINISHED, ASSIGNMENT_REVOKED},
    ASSIGNMENT_FINISHED: {ASSIGNMENT_RESULT_ACKED, ASSIGNMENT_REVOKED},
    ASSIGNMENT_REJECT_SENT: {ASSIGNMENT_REJECTED, ASSIGNMENT_REVOKED},
    ASSIGNMENT_REJECTED: set(),
    ASSIGNMENT_RESULT_ACKED: set(),
    ASSIGNMENT_REVOKED: set(),
}

_STORE_VERSION = 1
_KEY_BYTES = 32
_MAX_BYTES = 512 * 1024 * 1024
_MAX_RECORDS = 10_000
_CONTROL_RESERVE_BYTES = 16 * 1024 * 1024
_ADMISSION_RATIO = 0.80


@dataclass(frozen=True)
class RuntimeIdentity:
    worker_id: str
    runtime_session_id: str
    session_epoch: int


@dataclass(frozen=True)
class LocalAttemptIdentity:
    attempt: RuntimeAttemptIdentity
    session_epoch: int
    assignment_message_id: str
    offer_id: str

    @classmethod
    def from_attempt(
        cls,
        attempt: RuntimeAttemptIdentity,
        session_epoch: int,
    ) -> LocalAttemptIdentity:
        return cls(
            attempt=attempt,
            session_epoch=session_epoch,
            assignment_message_id=deterministic_uuid(
                "assignment", attempt.attempt_id, attempt.lease_id
            ),
            offer_id=deterministic_uuid("offer", attempt.attempt_id, attempt.lease_id),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt": self.attempt.to_dict(),
            "session_epoch": self.session_epoch,
            "assignment_message_id": self.assignment_message_id,
            "offer_id": self.offer_id,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> LocalAttemptIdentity:
        try:
            identity = cls(
                attempt=RuntimeAttemptIdentity.from_dict(value["attempt"]),
                session_epoch=int(value["session_epoch"]),
                assignment_message_id=str(value["assignment_message_id"]),
                offer_id=str(value["offer_id"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeStoreCorrupt("invalid durable Attempt identity") from exc
        if identity.session_epoch < 1:
            raise RuntimeStoreCorrupt("invalid durable Session epoch")
        _validate_uuid(identity.assignment_message_id)
        _validate_uuid(identity.offer_id)
        expected = cls.from_attempt(identity.attempt, identity.session_epoch)
        if expected != identity:
            raise RuntimeStoreCorrupt("durable Attempt identity digest mismatch")
        return identity


@dataclass
class AssignmentRecord:
    identity: LocalAttemptIdentity
    input: dict[str, Any]
    metadata: dict[str, Any]
    node_envelope: str
    agent_invocation_token: str
    offer_expires_at: datetime
    attempt_deadline_at: datetime
    run_deadline_at: datetime
    state: str = ASSIGNMENT_RECEIVED
    last_client_event_seq: int = 0
    acked_client_event_seq: int = 0
    acked_out_of_order_event_seqs: list[int] = field(default_factory=list)
    result_id: str = ""
    final_client_event_seq: int = 0
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity.to_dict(),
            "input": self.input,
            "metadata": self.metadata,
            "node_envelope": self.node_envelope,
            "agent_invocation_token": self.agent_invocation_token,
            "offer_expires_at": format_datetime(self.offer_expires_at),
            "attempt_deadline_at": format_datetime(self.attempt_deadline_at),
            "run_deadline_at": format_datetime(self.run_deadline_at),
            "state": self.state,
            "last_client_event_seq": self.last_client_event_seq,
            "acked_client_event_seq": self.acked_client_event_seq,
            "acked_out_of_order_event_seqs": self.acked_out_of_order_event_seqs,
            "result_id": self.result_id,
            "final_client_event_seq": self.final_client_event_seq,
            "updated_at": format_datetime(self.updated_at),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> AssignmentRecord:
        try:
            record = cls(
                identity=LocalAttemptIdentity.from_dict(value["identity"]),
                input=_json_object(value["input"]),
                metadata=_json_object(value.get("metadata", {})),
                node_envelope=str(value["node_envelope"]),
                agent_invocation_token=str(value["agent_invocation_token"]),
                offer_expires_at=parse_datetime(value["offer_expires_at"]),
                attempt_deadline_at=parse_datetime(value["attempt_deadline_at"]),
                run_deadline_at=parse_datetime(value["run_deadline_at"]),
                state=str(value["state"]),
                last_client_event_seq=int(value.get("last_client_event_seq", 0)),
                acked_client_event_seq=int(value.get("acked_client_event_seq", 0)),
                acked_out_of_order_event_seqs=[
                    int(item) for item in value.get("acked_out_of_order_event_seqs", [])
                ],
                result_id=str(value.get("result_id", "")),
                final_client_event_seq=int(value.get("final_client_event_seq", 0)),
                updated_at=parse_datetime(value["updated_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeStoreCorrupt("invalid durable assignment") from exc
        if record.state not in _TRANSITIONS:
            raise RuntimeStoreCorrupt("invalid durable assignment state")
        if record.last_client_event_seq < 0 or record.acked_client_event_seq < 0:
            raise RuntimeStoreCorrupt("invalid durable Event sequence")
        return record


@dataclass(frozen=True)
class EventRecord:
    identity: LocalAttemptIdentity
    client_event_id: str
    client_event_seq: int
    event_type: str
    payload: dict[str, Any]
    created_at: datetime

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["identity"] = self.identity.to_dict()
        value["created_at"] = format_datetime(self.created_at)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> EventRecord:
        try:
            record = cls(
                identity=LocalAttemptIdentity.from_dict(value["identity"]),
                client_event_id=str(value["client_event_id"]),
                client_event_seq=int(value["client_event_seq"]),
                event_type=str(value["event_type"]),
                payload=_json_object(value["payload"]),
                created_at=parse_datetime(value["created_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeStoreCorrupt("invalid durable Event") from exc
        _validate_uuid(record.client_event_id)
        if record.client_event_seq < 1:
            raise RuntimeStoreCorrupt("invalid durable Event sequence")
        return record


@dataclass(frozen=True)
class ResultRecord:
    identity: LocalAttemptIdentity
    result_id: str
    final_client_event_seq: int
    payload: dict[str, Any]
    created_at: datetime

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["identity"] = self.identity.to_dict()
        value["created_at"] = format_datetime(self.created_at)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ResultRecord:
        try:
            record = cls(
                identity=LocalAttemptIdentity.from_dict(value["identity"]),
                result_id=str(value["result_id"]),
                final_client_event_seq=int(value["final_client_event_seq"]),
                payload=_json_object(value["payload"]),
                created_at=parse_datetime(value["created_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeStoreCorrupt("invalid durable Result") from exc
        _validate_uuid(record.result_id)
        if record.final_client_event_seq < 0:
            raise RuntimeStoreCorrupt("invalid durable Result sequence")
        return record


@runtime_checkable
class RuntimeStore(Protocol):
    @property
    def identity(self) -> RuntimeIdentity: ...

    @property
    def unsafe_memory(self) -> bool: ...

    def accepts_new_runs(self) -> bool: ...

    def create_assignment(self, record: AssignmentRecord) -> AssignmentRecord: ...

    def advance_assignment(self, message_id: str, state: str) -> AssignmentRecord: ...

    def assignment(self, message_id: str) -> AssignmentRecord: ...

    def assignment_for_attempt(self, attempt_id: str) -> AssignmentRecord: ...

    def assignments(self) -> list[AssignmentRecord]: ...

    def delete_assignment(self, message_id: str) -> None: ...

    def append_event(
        self, attempt_id: str, event_type: str, payload: dict[str, Any]
    ) -> EventRecord: ...

    def pending_events(self, attempt_id: str) -> list[EventRecord]: ...

    def events_in_ranges(
        self, attempt_id: str, ranges: list[tuple[int, int]]
    ) -> list[EventRecord]: ...

    def ack_event(self, attempt_id: str, event_id: str, event_seq: int) -> None: ...

    def store_result(self, attempt_id: str, payload: dict[str, Any]) -> ResultRecord: ...

    def pending_result(self, attempt_id: str) -> ResultRecord | None: ...

    def ack_result(self, attempt_id: str, result_id: str) -> None: ...

    def clear_terminal_events(self, attempt_id: str) -> None: ...

    def discard_terminal_spool(self, attempt_id: str) -> None: ...

    def close(self) -> None: ...


class MemoryRuntimeStore:
    unsafe_memory = True

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._identity = RuntimeIdentity(str(uuid.uuid4()), str(uuid.uuid4()), 1)
        self._assignments: dict[str, AssignmentRecord] = {}
        self._attempts: dict[str, str] = {}
        self._events: dict[str, dict[int, EventRecord]] = {}
        self._results: dict[str, ResultRecord] = {}
        self._closed = False

    @property
    def identity(self) -> RuntimeIdentity:
        return self._identity

    def accepts_new_runs(self) -> bool:
        return not self._closed

    def create_assignment(self, record: AssignmentRecord) -> AssignmentRecord:
        with self._lock:
            self._ready()
            message_id = record.identity.assignment_message_id
            existing = self._assignments.get(message_id)
            if existing is not None:
                if existing.identity == record.identity and existing.input == record.input:
                    return existing
                raise RuntimeStoreCorrupt("assignment identity conflict")
            if record.identity.attempt.attempt_id in self._attempts:
                raise RuntimeStoreCorrupt("Attempt already has a durable assignment")
            record.state = ASSIGNMENT_RECEIVED
            record.updated_at = datetime.now(timezone.utc)
            self._assignments[message_id] = _copy_assignment(record)
            self._attempts[record.identity.attempt.attempt_id] = message_id
            return _copy_assignment(record)

    def advance_assignment(self, message_id: str, state: str) -> AssignmentRecord:
        with self._lock:
            record = self.assignment(message_id)
            if state == record.state:
                return record
            if state not in _TRANSITIONS.get(record.state, set()):
                raise RuntimeStoreCorrupt(
                    f"invalid assignment transition {record.state} -> {state}"
                )
            record.state = state
            record.updated_at = datetime.now(timezone.utc)
            self._assignments[message_id] = _copy_assignment(record)
            return record

    def assignment(self, message_id: str) -> AssignmentRecord:
        with self._lock:
            self._ready()
            try:
                return _copy_assignment(self._assignments[message_id])
            except KeyError as exc:
                raise RuntimeStoreError("assignment not found") from exc

    def assignment_for_attempt(self, attempt_id: str) -> AssignmentRecord:
        with self._lock:
            try:
                return self.assignment(self._attempts[attempt_id])
            except KeyError as exc:
                raise RuntimeStoreError("assignment not found") from exc

    def assignments(self) -> list[AssignmentRecord]:
        with self._lock:
            self._ready()
            return [_copy_assignment(item) for item in self._assignments.values()]

    def spool_status(self) -> RuntimeSpoolStatus:
        """Return a lock-consistent snapshot without expanding RuntimeStore."""

        with self._lock:
            self._ready()
            return RuntimeSpoolStatus(
                assignments=len(self._assignments),
                events=sum(len(records) for records in self._events.values()),
                results=len(self._results),
            )

    def delete_assignment(self, message_id: str) -> None:
        with self._lock:
            record = self.assignment(message_id)
            if record.state not in {
                ASSIGNMENT_REJECTED,
                ASSIGNMENT_RESULT_ACKED,
                ASSIGNMENT_REVOKED,
            }:
                raise RuntimeStoreError("cannot delete a non-terminal assignment")
            attempt_id = record.identity.attempt.attempt_id
            if self._events.get(attempt_id) or self._results.get(attempt_id):
                raise RuntimeStoreError("cannot delete assignment with durable spool records")
            self._assignments.pop(message_id, None)
            self._attempts.pop(attempt_id, None)

    def append_event(
        self, attempt_id: str, event_type: str, payload: dict[str, Any]
    ) -> EventRecord:
        with self._lock:
            assignment = self.assignment_for_attempt(attempt_id)
            if assignment.state != ASSIGNMENT_STARTED:
                raise RuntimeStoreError("events require a started assignment")
            sequence = assignment.last_client_event_seq + 1
            record = EventRecord(
                assignment.identity,
                str(uuid.uuid4()),
                sequence,
                event_type,
                _json_object(payload),
                datetime.now(timezone.utc),
            )
            self._events.setdefault(attempt_id, {})[sequence] = record
            assignment.last_client_event_seq = sequence
            assignment.updated_at = datetime.now(timezone.utc)
            self._assignments[assignment.identity.assignment_message_id] = assignment
            return record

    def pending_events(self, attempt_id: str) -> list[EventRecord]:
        with self._lock:
            assignment = self.assignment_for_attempt(attempt_id)
            return [
                record
                for sequence, record in sorted(self._events.get(attempt_id, {}).items())
                if not _event_acked(assignment, sequence)
            ]

    def events_in_ranges(self, attempt_id: str, ranges: list[tuple[int, int]]) -> list[EventRecord]:
        with self._lock:
            self.assignment_for_attempt(attempt_id)
            records: list[EventRecord] = []
            previous = 0
            for start, end in ranges:
                if start < 1 or end < start or start <= previous:
                    raise RuntimeStoreCorrupt("invalid Event replay ranges")
                for sequence in range(start, end + 1):
                    try:
                        records.append(self._events[attempt_id][sequence])
                    except KeyError as exc:
                        raise RuntimeStoreCorrupt("requested Event is unavailable") from exc
                previous = end
            return records

    def ack_event(self, attempt_id: str, event_id: str, event_seq: int) -> None:
        with self._lock:
            assignment = self.assignment_for_attempt(attempt_id)
            record = self._events.get(attempt_id, {}).get(event_seq)
            if record is None or record.client_event_id != event_id:
                raise RuntimeStoreCorrupt("Event ACK identity mismatch")
            _apply_event_ack(assignment, event_seq)
            assignment.updated_at = datetime.now(timezone.utc)
            self._assignments[assignment.identity.assignment_message_id] = assignment

    def store_result(self, attempt_id: str, payload: dict[str, Any]) -> ResultRecord:
        with self._lock:
            assignment = self.assignment_for_attempt(attempt_id)
            if assignment.state not in {ASSIGNMENT_STARTED, ASSIGNMENT_FINISHED}:
                raise RuntimeStoreError("Result requires a started assignment")
            existing = self._results.get(attempt_id)
            if existing is not None:
                if existing.payload != payload:
                    raise RuntimeStoreCorrupt("Result payload conflict")
                return existing
            result_id = str(payload.get("result_id", "")) or str(uuid.uuid4())
            final_sequence = int(
                payload.get("final_client_event_seq", assignment.last_client_event_seq)
            )
            payload = dict(payload)
            payload["result_id"] = result_id
            payload["final_client_event_seq"] = final_sequence
            result = ResultRecord(
                assignment.identity,
                result_id,
                final_sequence,
                payload,
                datetime.now(timezone.utc),
            )
            self._results[attempt_id] = result
            assignment.result_id = result_id
            assignment.final_client_event_seq = final_sequence
            if assignment.state == ASSIGNMENT_STARTED:
                assignment.state = ASSIGNMENT_FINISHED
            assignment.updated_at = datetime.now(timezone.utc)
            self._assignments[assignment.identity.assignment_message_id] = assignment
            return result

    def pending_result(self, attempt_id: str) -> ResultRecord | None:
        with self._lock:
            self._ready()
            return self._results.get(attempt_id)

    def ack_result(self, attempt_id: str, result_id: str) -> None:
        with self._lock:
            assignment = self.assignment_for_attempt(attempt_id)
            result = self._results.get(attempt_id)
            if result is None or result.result_id != result_id:
                raise RuntimeStoreCorrupt("Result ACK identity mismatch")
            self._results.pop(attempt_id, None)
            assignment.state = ASSIGNMENT_RESULT_ACKED
            assignment.updated_at = datetime.now(timezone.utc)
            self._assignments[assignment.identity.assignment_message_id] = assignment

    def clear_terminal_events(self, attempt_id: str) -> None:
        with self._lock:
            assignment = self.assignment_for_attempt(attempt_id)
            if assignment.state not in {ASSIGNMENT_RESULT_ACKED, ASSIGNMENT_REVOKED}:
                raise RuntimeStoreError("cannot clear non-terminal Events")
            self._events.pop(attempt_id, None)

    def discard_terminal_spool(self, attempt_id: str) -> None:
        with self._lock:
            assignment = self.assignment_for_attempt(attempt_id)
            if assignment.state not in {
                ASSIGNMENT_REJECTED,
                ASSIGNMENT_RESULT_ACKED,
                ASSIGNMENT_REVOKED,
            }:
                raise RuntimeStoreError("cannot discard spool for a non-terminal assignment")
            self._events.pop(attempt_id, None)
            self._results.pop(attempt_id, None)

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def _ready(self) -> None:
        if self._closed:
            raise RuntimeStoreError("Runtime store is closed")


class FileRuntimeStore(MemoryRuntimeStore):
    unsafe_memory = False

    def __init__(
        self,
        data_dir: str | os.PathLike[str],
        *,
        max_bytes: int = _MAX_BYTES,
        max_records: int = _MAX_RECORDS,
        reserve_bytes: int = _CONTROL_RESERVE_BYTES,
    ) -> None:
        self._path = Path(data_dir).expanduser().absolute()
        self._max_bytes = max_bytes
        self._max_records = max_records
        self._reserve_bytes = reserve_bytes
        if (
            self._max_bytes <= 0
            or self._max_records <= 0
            or self._reserve_bytes < 0
            or self._reserve_bytes >= self._max_bytes
        ):
            raise ValueError("invalid Runtime store capacity limits")
        self._file_sizes: dict[Path, int] = {}
        self._poisoned: Exception | None = None
        self._lock_file = None
        self._lock_backend = ""
        self._path.mkdir(mode=0o700, parents=True, exist_ok=True)
        _require_private_mode(self._path, directory=True)
        self._acquire_process_lock()
        try:
            self._key = self._load_or_create_key()
            identity = self._load_or_create_identity()
            self._identity = RuntimeIdentity(
                identity["worker_id"], str(uuid.uuid4()), identity["session_epoch"] + 1
            )
            self._persist_identity(self._identity.worker_id, self._identity.session_epoch)
            self._assignments = {}
            self._attempts = {}
            self._events = {}
            self._results = {}
            self._closed = False
            self._lock = threading.RLock()
            for directory in ("assignments", "events", "results"):
                target = self._path / directory
                target.mkdir(mode=0o700, exist_ok=True)
                _require_private_mode(target, directory=True)
            self._cleanup_temps()
            self._load_records()
        except Exception:
            self._release_process_lock()
            raise

    def accepts_new_runs(self) -> bool:
        with self._lock:
            if self._closed or self._poisoned:
                return False
            used = sum(self._file_sizes.values())
            records = len(self._file_sizes)
            try:
                free = shutil.disk_usage(self._path).free
            except OSError:
                return False
            return (
                used < int(self._max_bytes * _ADMISSION_RATIO)
                and records < int(self._max_records * _ADMISSION_RATIO)
                and free > self._reserve_bytes
            )

    def create_assignment(self, record: AssignmentRecord) -> AssignmentRecord:
        with self._lock:
            result = super().create_assignment(record)
            try:
                self._persist_assignment(result)
            except Exception as exc:
                self._poison(exc)
                raise
            return result

    def advance_assignment(self, message_id: str, state: str) -> AssignmentRecord:
        with self._lock:
            result = super().advance_assignment(message_id, state)
            try:
                self._persist_assignment(result)
            except Exception as exc:
                self._poison(exc)
                raise
            return result

    def delete_assignment(self, message_id: str) -> None:
        with self._lock:
            record = self.assignment(message_id)
            attempt_id = record.identity.attempt.attempt_id
            if record.state not in {
                ASSIGNMENT_REJECTED,
                ASSIGNMENT_RESULT_ACKED,
                ASSIGNMENT_REVOKED,
            }:
                raise RuntimeStoreError("cannot delete a non-terminal assignment")
            if self._events.get(attempt_id) or self._results.get(attempt_id):
                raise RuntimeStoreError("cannot delete assignment with durable spool records")
            try:
                self._remove_record(self._assignment_path(message_id))
                self._assignments.pop(message_id, None)
                self._attempts.pop(attempt_id, None)
            except Exception as exc:
                self._poison(exc)
                raise

    def append_event(
        self, attempt_id: str, event_type: str, payload: dict[str, Any]
    ) -> EventRecord:
        with self._lock:
            assignment = self.assignment_for_attempt(attempt_id)
            if assignment.state != ASSIGNMENT_STARTED:
                raise RuntimeStoreError("events require a started assignment")
            sequence = assignment.last_client_event_seq + 1
            record = EventRecord(
                assignment.identity,
                str(uuid.uuid4()),
                sequence,
                event_type,
                _json_object(payload),
                datetime.now(timezone.utc),
            )
            try:
                self._write_record(
                    self._event_path(attempt_id, record.client_event_id),
                    "event",
                    record.client_event_id,
                    record.to_dict(),
                )
                self._events.setdefault(attempt_id, {})[sequence] = record
                assignment.last_client_event_seq = sequence
                assignment.updated_at = datetime.now(timezone.utc)
                self._assignments[assignment.identity.assignment_message_id] = assignment
                self._persist_assignment(assignment)
            except Exception as exc:
                self._poison(exc)
                raise
            return record

    def ack_event(self, attempt_id: str, event_id: str, event_seq: int) -> None:
        with self._lock:
            super().ack_event(attempt_id, event_id, event_seq)
            try:
                self._persist_assignment(self.assignment_for_attempt(attempt_id))
            except Exception as exc:
                self._poison(exc)
                raise

    def store_result(self, attempt_id: str, payload: dict[str, Any]) -> ResultRecord:
        with self._lock:
            existing = self._results.get(attempt_id)
            if existing is not None:
                if existing.payload != payload and existing.payload != {
                    **payload,
                    "result_id": existing.result_id,
                    "final_client_event_seq": existing.final_client_event_seq,
                }:
                    raise RuntimeStoreCorrupt("Result payload conflict")
                return existing
            assignment = self.assignment_for_attempt(attempt_id)
            result_id = str(payload.get("result_id", "")) or str(uuid.uuid4())
            final_sequence = int(
                payload.get("final_client_event_seq", assignment.last_client_event_seq)
            )
            full_payload = dict(payload)
            full_payload["result_id"] = result_id
            full_payload["final_client_event_seq"] = final_sequence
            result = ResultRecord(
                assignment.identity,
                result_id,
                final_sequence,
                full_payload,
                datetime.now(timezone.utc),
            )
            try:
                self._write_record(
                    self._result_path(attempt_id), "result", result_id, result.to_dict()
                )
                self._results[attempt_id] = result
                assignment.result_id = result_id
                assignment.final_client_event_seq = final_sequence
                if assignment.state == ASSIGNMENT_STARTED:
                    assignment.state = ASSIGNMENT_FINISHED
                assignment.updated_at = datetime.now(timezone.utc)
                self._assignments[assignment.identity.assignment_message_id] = assignment
                self._persist_assignment(assignment)
            except Exception as exc:
                self._poison(exc)
                raise
            return result

    def ack_result(self, attempt_id: str, result_id: str) -> None:
        with self._lock:
            result = self._results.get(attempt_id)
            if result is None or result.result_id != result_id:
                raise RuntimeStoreCorrupt("Result ACK identity mismatch")
            assignment = self.assignment_for_attempt(attempt_id)
            assignment.state = ASSIGNMENT_RESULT_ACKED
            assignment.updated_at = datetime.now(timezone.utc)
            self._assignments[assignment.identity.assignment_message_id] = assignment
            try:
                self._persist_assignment(assignment)
                self._remove_record(self._result_path(attempt_id))
                self._results.pop(attempt_id, None)
            except Exception as exc:
                self._poison(exc)
                raise

    def clear_terminal_events(self, attempt_id: str) -> None:
        with self._lock:
            assignment = self.assignment_for_attempt(attempt_id)
            if assignment.state not in {ASSIGNMENT_RESULT_ACKED, ASSIGNMENT_REVOKED}:
                raise RuntimeStoreError("cannot clear non-terminal Events")
            try:
                for record in self._events.get(attempt_id, {}).values():
                    self._remove_record(self._event_path(attempt_id, record.client_event_id))
                self._events.pop(attempt_id, None)
            except Exception as exc:
                self._poison(exc)
                raise

    def discard_terminal_spool(self, attempt_id: str) -> None:
        with self._lock:
            assignment = self.assignment_for_attempt(attempt_id)
            if assignment.state not in {
                ASSIGNMENT_REJECTED,
                ASSIGNMENT_RESULT_ACKED,
                ASSIGNMENT_REVOKED,
            }:
                raise RuntimeStoreError("cannot discard spool for a non-terminal assignment")
            try:
                for record in self._events.get(attempt_id, {}).values():
                    self._remove_record(self._event_path(attempt_id, record.client_event_id))
                if attempt_id in self._results:
                    self._remove_record(self._result_path(attempt_id))
                self._events.pop(attempt_id, None)
                self._results.pop(attempt_id, None)
            except Exception as exc:
                self._poison(exc)
                raise

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._key = b""
            self._release_process_lock()

    def _ready(self) -> None:
        super()._ready()
        if self._poisoned:
            raise RuntimeStoreError("Runtime store is poisoned") from self._poisoned

    def _persist_assignment(self, record: AssignmentRecord) -> None:
        self._write_record(
            self._assignment_path(record.identity.assignment_message_id),
            "assignment",
            record.identity.assignment_message_id,
            record.to_dict(),
        )

    def _write_record(
        self,
        path: Path,
        kind: str,
        record_id: str,
        payload: dict[str, Any],
    ) -> None:
        self._ready()
        plaintext = wire_json_bytes(payload)
        nonce = os.urandom(12)
        aad = self._aad(kind, record_id)
        ciphertext = AESGCM(self._key).encrypt(nonce, plaintext, aad)
        envelope = wire_json_bytes(
            {
                "version": _STORE_VERSION,
                "kind": kind,
                "record_id": record_id,
                "nonce": base64.b64encode(nonce).decode(),
                "ciphertext": base64.b64encode(ciphertext).decode(),
            }
        )
        previous = self._file_sizes.get(path, 0)
        additional = max(0, len(envelope) - previous)
        record_delta = 0 if path in self._file_sizes else 1
        self._ensure_capacity(additional, record_delta)
        _atomic_write(path, envelope, 0o600)
        self._file_sizes[path] = len(envelope)

    def _read_record(self, path: Path, expected_kind: str) -> dict[str, Any]:
        _require_private_mode(path, directory=False)
        try:
            raw = path.read_bytes()
            envelope = json.loads(raw)
            if (
                envelope.get("version") != _STORE_VERSION
                or envelope.get("kind") != expected_kind
                or not isinstance(envelope.get("record_id"), str)
            ):
                raise RuntimeStoreCorrupt("durable record header mismatch")
            nonce = base64.b64decode(envelope["nonce"], validate=True)
            ciphertext = base64.b64decode(envelope["ciphertext"], validate=True)
            plaintext = AESGCM(self._key).decrypt(
                nonce,
                ciphertext,
                self._aad(expected_kind, envelope["record_id"]),
            )
            value = json.loads(plaintext)
        except RuntimeStoreCorrupt:
            raise
        except Exception as exc:
            raise RuntimeStoreCorrupt(f"cannot authenticate durable record {path.name}") from exc
        if not isinstance(value, dict):
            raise RuntimeStoreCorrupt("durable record is not an object")
        self._file_sizes[path] = len(raw)
        return value

    def _load_records(self) -> None:
        for path in sorted((self._path / "assignments").glob("*.record")):
            record = AssignmentRecord.from_dict(self._read_record(path, "assignment"))
            message_id = record.identity.assignment_message_id
            if path.stem != message_id or message_id in self._assignments:
                raise RuntimeStoreCorrupt("assignment filename or identity conflict")
            attempt_id = record.identity.attempt.attempt_id
            if attempt_id in self._attempts:
                raise RuntimeStoreCorrupt("duplicate durable Attempt")
            self._assignments[message_id] = record
            self._attempts[attempt_id] = message_id
        for path in sorted((self._path / "events").glob("*.record")):
            record = EventRecord.from_dict(self._read_record(path, "event"))
            attempt_id = record.identity.attempt.attempt_id
            if path.name != f"{attempt_id}.{record.client_event_id}.record":
                raise RuntimeStoreCorrupt("Event filename or identity conflict")
            assignment = self.assignment_for_attempt(attempt_id)
            if assignment.identity != record.identity:
                raise RuntimeStoreCorrupt("Event Attempt identity mismatch")
            by_sequence = self._events.setdefault(attempt_id, {})
            if record.client_event_seq in by_sequence:
                raise RuntimeStoreCorrupt("duplicate durable Event sequence")
            by_sequence[record.client_event_seq] = record
        for path in sorted((self._path / "results").glob("*.record")):
            record = ResultRecord.from_dict(self._read_record(path, "result"))
            attempt_id = record.identity.attempt.attempt_id
            if path.name != f"{attempt_id}.record":
                raise RuntimeStoreCorrupt("Result filename or identity conflict")
            assignment = self.assignment_for_attempt(attempt_id)
            if assignment.identity != record.identity or attempt_id in self._results:
                raise RuntimeStoreCorrupt("Result Attempt identity mismatch")
            self._results[attempt_id] = record
        for assignment in list(self._assignments.values()):
            attempt_id = assignment.identity.attempt.attempt_id
            events = self._events.get(attempt_id, {})
            result = self._results.get(attempt_id)
            if assignment.state in {
                ASSIGNMENT_REJECTED,
                ASSIGNMENT_RESULT_ACKED,
                ASSIGNMENT_REVOKED,
            }:
                if result is not None:
                    self._remove_record(self._result_path(attempt_id))
                    self._results.pop(attempt_id, None)
                for event in events.values():
                    self._remove_record(self._event_path(attempt_id, event.client_event_id))
                self._events.pop(attempt_id, None)
                continue
            sequences = sorted(events)
            if sequences and sequences != list(range(1, sequences[-1] + 1)):
                raise RuntimeStoreCorrupt("durable Event sequence has a gap")
            durable_sequence = sequences[-1] if sequences else 0
            if durable_sequence < assignment.last_client_event_seq:
                raise RuntimeStoreCorrupt("Event sequence and journal disagree")
            if durable_sequence > assignment.last_client_event_seq:
                if durable_sequence != assignment.last_client_event_seq + 1:
                    raise RuntimeStoreCorrupt("Event sequence advanced unexpectedly")
                assignment.last_client_event_seq = durable_sequence
                assignment.updated_at = datetime.now(timezone.utc)
                self._assignments[assignment.identity.assignment_message_id] = assignment
                self._persist_assignment(assignment)
            if result is not None:
                if result.final_client_event_seq != assignment.last_client_event_seq:
                    raise RuntimeStoreCorrupt("Result and Event journal disagree")
                if assignment.state == ASSIGNMENT_STARTED and not assignment.result_id:
                    assignment.result_id = result.result_id
                    assignment.final_client_event_seq = result.final_client_event_seq
                    assignment.state = ASSIGNMENT_FINISHED
                    assignment.updated_at = datetime.now(timezone.utc)
                    self._assignments[assignment.identity.assignment_message_id] = assignment
                    self._persist_assignment(assignment)
                elif assignment.result_id != result.result_id:
                    raise RuntimeStoreCorrupt("Result and journal disagree")
            elif assignment.state == ASSIGNMENT_FINISHED:
                raise RuntimeStoreCorrupt("finished assignment has no durable Result")

    def _load_or_create_key(self) -> bytes:
        path = self._path / "runtime.key"
        encrypted_records = any(
            child.suffix == ".record"
            for directory in ("assignments", "events", "results")
            if (self._path / directory).exists()
            for child in (self._path / directory).iterdir()
        )
        if path.is_symlink():
            raise RuntimeStoreError("Runtime store path has an unsafe file type")
        if not path.exists():
            if encrypted_records:
                raise RuntimeStoreCorrupt("Runtime store key is missing")
            key = os.urandom(_KEY_BYTES)
            _atomic_write(path, key, 0o600)
            return key
        _require_private_mode(path, directory=False)
        key = path.read_bytes()
        if len(key) != _KEY_BYTES:
            raise RuntimeStoreCorrupt("Runtime store key has an invalid length")
        return key

    def _load_or_create_identity(self) -> dict[str, Any]:
        path = self._path / "identity.json"
        durable = any(
            child.suffix == ".record"
            for directory in ("assignments", "events", "results")
            if (self._path / directory).exists()
            for child in (self._path / directory).iterdir()
        )
        if path.is_symlink():
            raise RuntimeStoreError("Runtime store path has an unsafe file type")
        if not path.exists():
            if durable:
                raise RuntimeStoreCorrupt("Runtime identity is missing")
            worker_id = str(uuid.uuid4())
            self._persist_identity(worker_id, 0)
            return {"worker_id": worker_id, "session_epoch": 0}
        _require_private_mode(path, directory=False)
        try:
            value = json.loads(path.read_bytes())
            version = int(value["version"])
            worker_id = str(value["worker_id"])
            session_epoch = int(value["session_epoch"])
            checksum = str(value["checksum"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeStoreCorrupt("Runtime identity is corrupt") from exc
        _validate_uuid(worker_id)
        if (
            version != _STORE_VERSION
            or session_epoch < 0
            or checksum != _identity_checksum(worker_id, session_epoch)
        ):
            raise RuntimeStoreCorrupt("Runtime identity checksum mismatch")
        return {"worker_id": worker_id, "session_epoch": session_epoch}

    def _persist_identity(self, worker_id: str, session_epoch: int) -> None:
        payload = wire_json_bytes(
            {
                "version": _STORE_VERSION,
                "worker_id": worker_id,
                "session_epoch": session_epoch,
                "checksum": _identity_checksum(worker_id, session_epoch),
            }
        )
        _atomic_write(self._path / "identity.json", payload, 0o600)

    def _ensure_capacity(self, additional_bytes: int, additional_records: int) -> None:
        used = sum(self._file_sizes.values())
        if used + additional_bytes > self._max_bytes:
            raise RuntimeStoreCapacity("Runtime store byte limit is exhausted")
        if len(self._file_sizes) + additional_records > self._max_records:
            raise RuntimeStoreCapacity("Runtime store record limit is exhausted")
        try:
            free = shutil.disk_usage(self._path).free
        except OSError as exc:
            raise RuntimeStoreCapacity("cannot determine Runtime store free space") from exc
        if free - additional_bytes < self._reserve_bytes:
            raise RuntimeStoreCapacity("Runtime store control-space reserve would be consumed")

    def _remove_record(self, path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        self._file_sizes.pop(path, None)
        _fsync_directory(path.parent)

    def _cleanup_temps(self) -> None:
        for directory in (
            self._path,
            self._path / "assignments",
            self._path / "events",
            self._path / "results",
        ):
            _require_private_mode(directory, directory=True)
            for path in directory.glob("*.tmp"):
                path.unlink()

    def _assignment_path(self, message_id: str) -> Path:
        return self._path / "assignments" / f"{message_id}.record"

    def _event_path(self, attempt_id: str, event_id: str) -> Path:
        return self._path / "events" / f"{attempt_id}.{event_id}.record"

    def _result_path(self, attempt_id: str) -> Path:
        return self._path / "results" / f"{attempt_id}.record"

    @staticmethod
    def _aad(kind: str, record_id: str) -> bytes:
        return f"openlinker-runtime\x00{kind}\x00{record_id}".encode()

    def _poison(self, exc: Exception) -> None:
        self._poisoned = exc

    def _acquire_process_lock(self) -> None:
        path = self._path / ".runtime.lock"
        if path.is_symlink():
            raise RuntimeStoreError("Runtime store path has an unsafe file type")
        flags = os.O_RDWR | os.O_CREAT | os.O_APPEND
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags, 0o600)
        except OSError as exc:
            raise RuntimeStoreError("cannot open the Runtime store lock safely") from exc
        handle = os.fdopen(descriptor, "a+b")
        try:
            os.chmod(path, 0o600)
            _require_private_mode(path, directory=False)
        except Exception:
            handle.close()
            raise
        try:
            if os.name == "nt":
                import msvcrt

                if path.stat().st_size == 0:
                    handle.write(b"\0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                self._lock_backend = "windows"
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._lock_backend = "unix"
        except (OSError, BlockingIOError) as exc:
            handle.close()
            raise RuntimeStoreLocked("Runtime data directory is already locked") from exc
        self._lock_file = handle

    def _release_process_lock(self) -> None:
        handle = self._lock_file
        if handle is None:
            return
        try:
            if self._lock_backend == "windows":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            elif self._lock_backend == "unix":
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._lock_file = None


def _copy_assignment(record: AssignmentRecord) -> AssignmentRecord:
    return AssignmentRecord.from_dict(record.to_dict())


def _event_acked(record: AssignmentRecord, sequence: int) -> bool:
    return (
        sequence <= record.acked_client_event_seq
        or sequence in record.acked_out_of_order_event_seqs
    )


def _apply_event_ack(record: AssignmentRecord, sequence: int) -> None:
    if _event_acked(record, sequence):
        return
    if sequence == record.acked_client_event_seq + 1:
        record.acked_client_event_seq = sequence
        pending = set(record.acked_out_of_order_event_seqs)
        while record.acked_client_event_seq + 1 in pending:
            record.acked_client_event_seq += 1
            pending.remove(record.acked_client_event_seq)
        record.acked_out_of_order_event_seqs = sorted(pending)
        return
    record.acked_out_of_order_event_seqs = sorted({*record.acked_out_of_order_event_seqs, sequence})


def _identity_checksum(worker_id: str, session_epoch: int) -> str:
    return hashlib.sha256(
        f"{_STORE_VERSION}\x00{worker_id}\x00{session_epoch}".encode()
    ).hexdigest()


def _json_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeStoreCorrupt("durable value is not a JSON object")
    return dict(value)


def _validate_uuid(value: str) -> None:
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise RuntimeStoreCorrupt("durable identifier is not a UUID") from exc
    if str(parsed) != value or parsed.int == 0:
        raise RuntimeStoreCorrupt("durable identifier is not a lowercase non-zero UUID")


def _require_private_mode(path: Path, *, directory: bool) -> None:
    info = path.lstat()
    expected = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if path.is_symlink() or not expected:
        raise RuntimeStoreError("Runtime store path has an unsafe file type")
    if os.name == "nt":
        return
    mode = stat.S_IMODE(info.st_mode)
    forbidden = mode & 0o077
    if forbidden:
        kind = "directory" if directory else "file"
        raise RuntimeStoreError(
            f"Runtime store {kind} permissions must not grant group/other access"
        )


def _atomic_write(path: Path, raw: bytes, mode: int) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        descriptor = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        os.chmod(path, mode)
        _fsync_directory(path.parent)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        if os.name != "nt":
            raise
    finally:
        os.close(descriptor)


__all__ = [
    "ASSIGNMENT_ACK_SENT",
    "ASSIGNMENT_CONFIRMED",
    "ASSIGNMENT_FINISHED",
    "ASSIGNMENT_RECEIVED",
    "ASSIGNMENT_REJECTED",
    "ASSIGNMENT_REJECT_SENT",
    "ASSIGNMENT_RESULT_ACKED",
    "ASSIGNMENT_REVOKED",
    "ASSIGNMENT_STARTED",
    "AssignmentRecord",
    "EventRecord",
    "FileRuntimeStore",
    "LocalAttemptIdentity",
    "MemoryRuntimeStore",
    "ResultRecord",
    "RuntimeIdentity",
    "RuntimeStore",
]
