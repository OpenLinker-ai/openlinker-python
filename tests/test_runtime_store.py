from __future__ import annotations

import base64
import json
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from openlinker import runtime
from openlinker.runtime.store import (
    ASSIGNMENT_ACK_SENT,
    ASSIGNMENT_CONFIRMED,
    ASSIGNMENT_REVOKED,
    ASSIGNMENT_STARTED,
    AssignmentRecord,
    LocalAttemptIdentity,
)


def make_started(store: runtime.FileRuntimeStore) -> AssignmentRecord:
    now = datetime.now(timezone.utc)
    attempt = runtime.RuntimeAttemptIdentity(
        run_id="33333333-3333-4333-8333-333333333333",
        attempt_id="44444444-4444-4444-8444-444444444444",
        lease_id="55555555-5555-4555-8555-555555555555",
        fencing_token=1,
        node_id="11111111-1111-4111-8111-111111111111",
        agent_id="22222222-2222-4222-8222-222222222222",
        worker_id=store.identity.worker_id,
        runtime_session_id=store.identity.runtime_session_id,
    )
    record = AssignmentRecord(
        identity=LocalAttemptIdentity.from_attempt(attempt, store.identity.session_epoch),
        input={"task": "echo"},
        metadata={},
        node_envelope="ol_ctx_v2.current.payload.signature",
        agent_invocation_token="ol_inv_v2.current.payload.signature",
        offer_expires_at=now + timedelta(minutes=1),
        attempt_deadline_at=now + timedelta(minutes=2),
        run_deadline_at=now + timedelta(minutes=3),
    )
    store.create_assignment(record)
    for state in (ASSIGNMENT_ACK_SENT, ASSIGNMENT_CONFIRMED, ASSIGNMENT_STARTED):
        record = store.advance_assignment(record.identity.assignment_message_id, state)
    return record


def test_identity_is_stable_and_session_rotates(tmp_path: Path):
    data_dir = tmp_path / "runtime"
    first_store = runtime.FileRuntimeStore(data_dir)
    first = first_store.identity
    first_store.close()

    second_store = runtime.FileRuntimeStore(data_dir)
    second = second_store.identity
    try:
        assert second.worker_id == first.worker_id
        assert second.runtime_session_id != first.runtime_session_id
        assert second.session_epoch == first.session_epoch + 1
        if os.name != "nt":
            assert stat.S_IMODE(data_dir.stat().st_mode) == 0o700
            assert stat.S_IMODE((data_dir / "runtime.key").stat().st_mode) == 0o600
            assert stat.S_IMODE((data_dir / "identity.json").stat().st_mode) == 0o600
    finally:
        second_store.close()


def test_store_rejects_a_second_process_lock(tmp_path: Path):
    data_dir = tmp_path / "runtime"
    first = runtime.FileRuntimeStore(data_dir)
    try:
        with pytest.raises(runtime.RuntimeStoreLocked):
            runtime.FileRuntimeStore(data_dir)
    finally:
        first.close()


def test_event_and_result_keep_stable_ids_across_restart(tmp_path: Path):
    data_dir = tmp_path / "runtime"
    store = runtime.FileRuntimeStore(data_dir)
    record = make_started(store)
    attempt_id = record.identity.attempt.attempt_id
    first = store.append_event(attempt_id, "run.progress", {"step": 1})
    second = store.append_event(attempt_id, "run.progress", {"step": 2})
    store.ack_event(attempt_id, second.client_event_id, second.client_event_seq)
    result = store.store_result(
        attempt_id,
        {
            "attempt_identity": record.identity.attempt.to_dict(),
            "status": "success",
            "output": {"answer": 42},
            "duration_ms": 1,
        },
    )
    store.close()

    reopened = runtime.FileRuntimeStore(data_dir)
    try:
        pending = reopened.pending_events(attempt_id)
        replayed = reopened.pending_result(attempt_id)
        assert [item.client_event_id for item in pending] == [first.client_event_id]
        assert replayed is not None
        assert replayed.result_id == result.result_id
        assert replayed.final_client_event_seq == 2
    finally:
        reopened.close()


def test_concurrent_event_appends_are_serialized_and_durable(tmp_path: Path):
    store = runtime.FileRuntimeStore(tmp_path / "runtime")
    record = make_started(store)
    attempt_id = record.identity.attempt.attempt_id
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            records = list(
                executor.map(
                    lambda step: store.append_event(
                        attempt_id,
                        "run.progress",
                        {"step": step},
                    ),
                    range(32),
                )
            )

        assert sorted(item.client_event_seq for item in records) == list(range(1, 33))
        assert len({item.client_event_id for item in records}) == 32
        assert [item.client_event_seq for item in store.pending_events(attempt_id)] == list(
            range(1, 33)
        )
    finally:
        store.close()


def test_revoked_attempt_discards_durable_spool_before_deletion(tmp_path: Path):
    data_dir = tmp_path / "runtime"
    store = runtime.FileRuntimeStore(data_dir)
    record = make_started(store)
    attempt_id = record.identity.attempt.attempt_id
    store.append_event(attempt_id, "run.progress", {"step": 1})
    store.store_result(
        attempt_id,
        {
            "attempt_identity": record.identity.attempt.to_dict(),
            "status": "success",
            "output": {"answer": 1},
            "duration_ms": 1,
        },
    )
    revoked = store.advance_assignment(record.identity.assignment_message_id, ASSIGNMENT_REVOKED)
    store.discard_terminal_spool(attempt_id)
    store.delete_assignment(revoked.identity.assignment_message_id)
    store.close()

    reopened = runtime.FileRuntimeStore(data_dir)
    try:
        assert reopened.assignments() == []
    finally:
        reopened.close()


def test_missing_key_and_modified_ciphertext_fail_closed(tmp_path: Path):
    missing_dir = tmp_path / "missing-key"
    store = runtime.FileRuntimeStore(missing_dir)
    make_started(store)
    store.close()
    (missing_dir / "runtime.key").unlink()
    with pytest.raises(runtime.RuntimeStoreCorrupt, match="key is missing"):
        runtime.FileRuntimeStore(missing_dir)

    corrupt_dir = tmp_path / "corrupt"
    store = runtime.FileRuntimeStore(corrupt_dir)
    record = make_started(store)
    store.close()
    path = corrupt_dir / "assignments" / f"{record.identity.assignment_message_id}.record"
    envelope = json.loads(path.read_bytes())
    ciphertext = bytearray(base64.b64decode(envelope["ciphertext"]))
    ciphertext[-1] ^= 1
    envelope["ciphertext"] = base64.b64encode(ciphertext).decode()
    path.write_text(json.dumps(envelope))
    os.chmod(path, 0o600)
    with pytest.raises(runtime.RuntimeStoreCorrupt, match="authenticate"):
        runtime.FileRuntimeStore(corrupt_dir)


def test_permissions_and_capacity_fail_closed(tmp_path: Path):
    if os.name != "nt":
        exposed = tmp_path / "exposed"
        exposed.mkdir(mode=0o755)
        with pytest.raises(runtime.RuntimeStoreError, match="permissions"):
            runtime.FileRuntimeStore(exposed)

    data_dir = tmp_path / "capacity"
    store = runtime.FileRuntimeStore(
        data_dir, max_bytes=10 * 1024 * 1024, max_records=3, reserve_bytes=1
    )
    try:
        record = make_started(store)
        attempt_id = record.identity.attempt.attempt_id
        store.append_event(attempt_id, "run.progress", {"step": 1})
        assert not store.accepts_new_runs()
        store.append_event(attempt_id, "run.progress", {"step": 2})
        with pytest.raises(runtime.RuntimeStoreCapacity):
            store.append_event(attempt_id, "run.progress", {"step": 3})
        assert not store.accepts_new_runs()
    finally:
        store.close()


def test_store_rejects_unsafe_symlinked_key(tmp_path: Path):
    if os.name == "nt" or not hasattr(os, "symlink"):
        pytest.skip("symlink security test requires POSIX")
    data_dir = tmp_path / "symlink"
    store = runtime.FileRuntimeStore(data_dir)
    store.close()
    key = data_dir / "runtime.key"
    target = tmp_path / "external.key"
    target.write_bytes(key.read_bytes())
    os.chmod(target, 0o600)
    key.unlink()
    key.symlink_to(target)
    with pytest.raises(runtime.RuntimeStoreError, match="unsafe file type"):
        runtime.FileRuntimeStore(data_dir)


def test_store_rejects_symlinked_root_and_record_directory_before_cleanup(tmp_path: Path):
    if os.name == "nt" or not hasattr(os, "symlink"):
        pytest.skip("symlink security test requires POSIX")

    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    root_link = tmp_path / "root-link"
    root_link.symlink_to(target, target_is_directory=True)
    with pytest.raises(runtime.RuntimeStoreError, match="unsafe file type"):
        runtime.FileRuntimeStore(root_link)

    data_dir = tmp_path / "runtime"
    store = runtime.FileRuntimeStore(data_dir)
    store.close()
    events = data_dir / "events"
    events.rmdir()
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    outside_temp = outside / "must-not-delete.tmp"
    outside_temp.write_text("keep")
    events.symlink_to(outside, target_is_directory=True)
    with pytest.raises(runtime.RuntimeStoreError, match="unsafe file type"):
        runtime.FileRuntimeStore(data_dir)
    assert outside_temp.read_text() == "keep"
