import json
import stat

import pytest

from openlinker.runtime.credentials import RuntimeCredentialManager


@pytest.mark.asyncio
async def test_runtime_credential_manager_generates_one_private_node_key(tmp_path):
    options = {
        "data_dir": tmp_path,
        "credential_endpoint": "http://127.0.0.1:8080/api/v1/runtime-credentials",
        "agent_token": "ol_agent_test",
        "node_id": "",
        "agent_id": "",
        "node_version": "openlinker-python/runtime-worker",
        "capacity": 1,
    }
    first = RuntimeCredentialManager(**options)
    await first.open()
    initial = json.loads((tmp_path / "runtime-credential.json").read_text())
    assert "BEGIN PRIVATE KEY" in initial["private_key_pem"]
    assert stat.S_IMODE((tmp_path / "runtime-credential.json").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "runtime-client-key.pem").stat().st_mode) == 0o600

    second = RuntimeCredentialManager(**options)
    await second.open()
    reopened = json.loads((tmp_path / "runtime-credential.json").read_text())
    assert reopened["node_id"] == initial["node_id"]
    assert reopened["private_key_pem"] == initial["private_key_pem"]
    assert reopened["public_key_thumbprint"] == initial["public_key_thumbprint"]
