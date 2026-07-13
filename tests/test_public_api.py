from __future__ import annotations

import openlinker
from openlinker import client, runtime


def test_root_exports_only_client_and_runtime_namespaces():
    assert openlinker.client is client
    assert openlinker.runtime is runtime
    assert not hasattr(openlinker, "Client")
    assert not hasattr(openlinker, "Native")
