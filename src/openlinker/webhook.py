from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from typing import Any, Mapping

from .types import TaskCallbackAuthentication, TaskCallbackConfig


DEFAULT_TASK_CALLBACK_SECRET_BYTES = 32


@dataclass
class WebhookRunCallbackOptions:
    secret: str = ""
    token: str = ""
    authentication: TaskCallbackAuthentication | None = None
    metadata: Any = None
    event_types: list[str] | None = None


def new_webhook_run_callback(url: str, opts: WebhookRunCallbackOptions | None = None) -> TaskCallbackConfig:
    trimmed = url.strip()
    if not trimmed:
        raise ValueError("openlinker: task callback URL is required")
    opts = opts or WebhookRunCallbackOptions()
    secret = opts.secret.strip() or generate_task_callback_secret()
    return TaskCallbackConfig(
        url=trimmed,
        token=opts.token.strip(),
        secret=secret,
        authentication=opts.authentication,
        metadata=opts.metadata,
        event_types=list(opts.event_types or []),
    )


def generate_task_callback_secret() -> str:
    return secrets.token_hex(DEFAULT_TASK_CALLBACK_SECRET_BYTES)


def sign_task_callback_payload(payload: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


def verify_task_callback_signature(payload: bytes, secret: str, signature: str) -> bool:
    expected = _normalize_signature(sign_task_callback_payload(payload, secret))
    actual = _normalize_signature(signature)
    if not expected or not actual:
        return False
    try:
        return hmac.compare_digest(bytes.fromhex(expected), bytes.fromhex(actual))
    except ValueError:
        return False


def task_callback_signature_from_header(headers: Mapping[str, str]) -> str:
    for key, value in headers.items():
        if key.lower() == "x-openlinker-signature":
            return value
    return ""


def verify_task_callback_request_body(
    payload: bytes, headers: Mapping[str, str], secret: str
) -> tuple[bytes, bool]:
    signature = task_callback_signature_from_header(headers)
    return payload, bool(signature and verify_task_callback_signature(payload, secret, signature))


def _normalize_signature(signature: str) -> str:
    return signature.strip().lower().removeprefix("sha256=")


NewWebhookRunCallback = new_webhook_run_callback
GenerateTaskCallbackSecret = generate_task_callback_secret
SignTaskCallbackPayload = sign_task_callback_payload
VerifyTaskCallbackSignature = verify_task_callback_signature
TaskCallbackSignatureFromHeader = task_callback_signature_from_header

