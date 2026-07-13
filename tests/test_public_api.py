from __future__ import annotations

import openlinker
from openlinker import client, runtime


def test_root_exports_only_client_and_runtime_namespaces():
    assert openlinker.client is client
    assert openlinker.runtime is runtime
    assert not hasattr(openlinker, "Client")
    assert not hasattr(openlinker, "Native")


def test_client_rejects_agent_token_and_legacy_runtime_api_is_absent():
    try:
        client.Client("https://api.example.test", user_token="ol_agent_wrong")
    except ValueError as error:
        assert "does not accept Agent Token" in str(error)
    else:
        raise AssertionError("Client accepted an Agent Token")

    for name in (
        "Native",
        "WithFunc",
        "NativeRun",
        "EnsureRuntimeAgent",
        "RuntimeV2Worker",
    ):
        assert not hasattr(runtime, name)
