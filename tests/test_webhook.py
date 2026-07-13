from __future__ import annotations

from openlinker import client


def test_task_callback_signature_helpers_verify_payloads():
    payload = b'{"event":"run.completed"}'
    sig = client.sign_task_callback_payload(payload, "secret")
    assert client.verify_task_callback_signature(payload, "secret", sig)
    assert client.verify_task_callback_signature(payload, "secret", "sha256=" + sig)
    assert not client.verify_task_callback_signature(payload, "wrong", sig)


def test_new_webhook_run_callback_generates_secret():
    cfg = client.new_webhook_run_callback("https://example.test/callback")
    assert cfg.url == "https://example.test/callback"
    assert cfg.secret
