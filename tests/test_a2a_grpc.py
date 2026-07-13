from __future__ import annotations

import pytest

pytest.importorskip("a2a.client.transports.grpc")

from openlinker.a2a import A2AGRPCClient, A2AMessage, A2AMessageSendParams


def test_a2a_grpc_client_builds_proto_request_and_metadata():
    grpc = A2AGRPCClient(
        "grpc://localhost:50051",
        "tenant-1",
        token="runtime-token",
        headers={"x-test": "1"},
    )

    req = grpc._send_message_request(
        A2AMessageSendParams(
            message=A2AMessage(
                message_id="msg-1",
                context_id="ctx-1",
                role="user",
                parts=[{"text": "hello"}],
            )
        )
    )
    ctx = grpc._context()

    assert req.tenant == "tenant-1"
    assert req.message.message_id == "msg-1"
    assert req.message.context_id == "ctx-1"
    assert req.message.role == 1
    assert ctx.service_parameters["authorization"] == "Bearer runtime-token"
    assert ctx.service_parameters["x-test"] == "1"
