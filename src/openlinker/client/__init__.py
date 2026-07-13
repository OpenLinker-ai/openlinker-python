from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Awaitable
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, TypeVar
from urllib.parse import quote, urlencode, urlparse, urlunparse

import httpx

from ..error import OpenLinkerError
from ..model import Model, maybe_model, to_json_value
from ..types import (
    AgentCardResponse,
    AgentDetailResponse,
    AgentListResponse,
    AgentResponse,
    AgentTokenListResponse,
    AgentTokenResponse,
    CreateAgentRequest,
    CreateAgentTokenRequest,
    ListAgentTokensParams,
    ListAgentsParams,
    ListMyAgentsParams,
    ListRunChildrenResponse,
    ListRunEventsParams,
    ListRunEventsResponse,
    MarketListResponse,
    PlatformCallbackOptions,
    RunAgentRequest,
    RunArtifactResponse,
    RunMessageResponse,
    RunResponse,
    StreamRunEvent,
    StreamRunEventsOptions,
    TaskCallbackAuthentication as TaskCallbackAuthentication,
    TaskCallbackConfig as TaskCallbackConfig,
    TaskCallbackSubscription as TaskCallbackSubscription,
    UpdateAgentRequest,
)
from ..webhook import (
    GenerateTaskCallbackSecret as GenerateTaskCallbackSecret,
    NewWebhookRunCallback as NewWebhookRunCallback,
    SignTaskCallbackPayload as SignTaskCallbackPayload,
    TaskCallbackSignatureFromHeader as TaskCallbackSignatureFromHeader,
    VerifyTaskCallbackSignature as VerifyTaskCallbackSignature,
    WebhookRunCallbackOptions as WebhookRunCallbackOptions,
    generate_task_callback_secret as generate_task_callback_secret,
    new_webhook_run_callback as new_webhook_run_callback,
    sign_task_callback_payload as sign_task_callback_payload,
    task_callback_signature_from_header as task_callback_signature_from_header,
    verify_task_callback_request_body as verify_task_callback_request_body,
    verify_task_callback_signature as verify_task_callback_signature,
)


DEFAULT_SDK_AGENT = "openlinker-python/0.2.0"
MAX_RESPONSE_BODY_BYTES = 4 << 20
T = TypeVar("T", bound=Model)


def _clean(value: str | None) -> str:
    return (value or "").strip()


def normalize_base_url(raw: str) -> str:
    normalized = _clean(raw).rstrip("/")
    if normalized.endswith("/api/v1"):
        normalized = normalized[: -len("/api/v1")]
    return normalized


def _set_query(query: dict[str, Any], key: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, bool):
        if value:
            query[key] = "true"
        return
    if isinstance(value, int):
        if value > 0:
            query[key] = str(value)
        return
    if isinstance(value, str) and value.strip():
        query[key] = value.strip()


def _quote(value: str) -> str:
    return quote(value, safe="")


def _maybe_await(value: Any) -> Awaitable[Any]:
    if inspect.isawaitable(value):
        return value

    async def _done() -> Any:
        return value

    return _done()


def _retry_after(headers: httpx.Headers) -> timedelta | None:
    value = headers.get("Retry-After")
    if not value:
        return None
    try:
        seconds = int(value)
        if seconds > 0:
            return timedelta(seconds=seconds)
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        delay = retry_at - datetime.now(timezone.utc)
        if delay.total_seconds() > 0:
            return delay
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    return None


class Client:
    def __init__(
        self,
        base_url: str,
        *,
        http_client: httpx.AsyncClient | None = None,
        user_token: str = "",
        sdk_agent: str = DEFAULT_SDK_AGENT,
        headers: dict[str, str] | None = None,
    ) -> None:
        normalized = normalize_base_url(base_url)
        if not normalized:
            raise ValueError("openlinker: base URL is required")
        parsed = urlparse(normalized)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("openlinker: base URL must include scheme and host")
        self.base_url = normalized
        self.user_token = _clean(user_token)
        if self.user_token.startswith("ol_agent_"):
            raise ValueError(
                "openlinker: Client does not accept Agent Token; use runtime.RuntimeWorker"
            )
        self.sdk_agent = _clean(sdk_agent) or DEFAULT_SDK_AGENT
        self.headers: dict[str, str] = dict(headers or {})
        self._client = http_client or httpx.AsyncClient(timeout=None)
        self._owns_client = http_client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    def clone(self, **kwargs: Any) -> Client:
        params = {
            "http_client": self._client,
            "user_token": self.user_token,
            "sdk_agent": self.sdk_agent,
            "headers": dict(self.headers),
        }
        params.update(kwargs)
        return Client(self.base_url, **params)

    def endpoint(self, path: str, query: dict[str, Any] | None = None) -> str:
        if path.startswith(("http://", "https://")):
            parsed = urlparse(path)
            return urlunparse(parsed._replace(query=urlencode(query or {}, doseq=True)))
        normalized_path = path.lstrip("/")
        if normalized_path.startswith("api/v1/"):
            normalized_path = normalized_path[len("api/v1/") :]
        base = self.base_url.rstrip("/")
        parsed = urlparse(f"{base}/api/v1/{normalized_path}")
        return urlunparse(parsed._replace(query=urlencode(query or {}, doseq=True)))

    def _request_headers(self, accept: str, token: str = "", body: bool = False) -> dict[str, str]:
        headers = {"Accept": accept, "X-OpenLinker-SDK": self.sdk_agent}
        if body:
            headers["Content-Type"] = "application/json"
        if token:
            headers["Authorization"] = f"Bearer {token}"
        headers.update(self.headers)
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        body: Any = None,
        accept: str = "application/json",
        token: str = "",
    ) -> httpx.Response:
        content = None
        if body is not None:
            content = json.dumps(to_json_value(body)).encode()
        return await self._client.request(
            method,
            self.endpoint(path, query),
            content=content,
            headers=self._request_headers(accept, token, body is not None),
        )

    async def _do(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        body: Any = None,
        out: type[T] | None = None,
    ) -> T | dict[str, Any] | None:
        response = await self._request(
            method,
            path,
            query=query,
            body=body,
            token=self.user_token,
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise self._parse_error(response)
        if response.status_code == 204:
            return None
        raw = response.content
        if len(raw) > MAX_RESPONSE_BODY_BYTES:
            raise ValueError(f"openlinker: response body exceeds {MAX_RESPONSE_BODY_BYTES} bytes")
        if not raw:
            return None
        data = response.json()
        if out is None:
            return data
        return out.from_dict(data)

    def _parse_error(self, response: httpx.Response) -> OpenLinkerError:
        raw = response.content[:MAX_RESPONSE_BODY_BYTES]
        parsed: dict[str, Any] = {}
        try:
            parsed = response.json()
        except ValueError:
            pass
        err = parsed.get("error") if isinstance(parsed, dict) else None
        err = err if isinstance(err, dict) else {}
        return OpenLinkerError(
            status_code=response.status_code,
            code=err.get("code") or f"HTTP_{response.status_code}",
            message=err.get("message") or response.reason_phrase,
            details=err.get("details"),
            request_id=response.headers.get("X-Request-Id")
            or response.headers.get("X-Correlation-Id", ""),
            retry_after=_retry_after(response.headers),
            response_body=raw,
        )

    async def list_agents(self, params: ListAgentsParams | dict[str, Any] | None = None):
        params = maybe_model(params or {}, ListAgentsParams)
        query: dict[str, Any] = {}
        _set_query(query, "q", params.query)
        _set_query(query, "page", params.page)
        _set_query(query, "size", params.size)
        _set_query(query, "callable_only", params.callable_only)
        if params.tags:
            query["tags"] = ",".join(params.tags)
        return await self._do("GET", "/agents", query=query, out=MarketListResponse)

    async def get_agent(self, slug: str):
        return await self._do("GET", f"/agents/{_quote(slug)}", out=AgentDetailResponse)

    async def get_agent_card(self, slug: str, extended: bool = False):
        suffix = "agent-card.extended.json" if extended else "agent-card.json"
        return await self._do("GET", f"/agents/{_quote(slug)}/{suffix}", out=AgentCardResponse)

    async def run_agent(self, req: RunAgentRequest | dict[str, Any]):
        return await self._do(
            "POST", "/run", body=maybe_model(req, RunAgentRequest), out=RunResponse
        )

    async def start_agent_run(self, req: RunAgentRequest | dict[str, Any]):
        return await self._do(
            "POST", "/runs", body=maybe_model(req, RunAgentRequest), out=RunResponse
        )

    async def get_run(self, run_id: str):
        return await self._do("GET", f"/runs/{_quote(run_id)}", out=RunResponse)

    async def list_run_events(
        self, run_id: str, params: ListRunEventsParams | dict[str, Any] | None = None
    ):
        params = maybe_model(params or {}, ListRunEventsParams)
        query: dict[str, Any] = {}
        _set_query(query, "after_sequence", params.after_sequence)
        _set_query(query, "limit", params.limit)
        return await self._do(
            "GET", f"/runs/{_quote(run_id)}/events", query=query, out=ListRunEventsResponse
        )

    async def list_run_children(self, run_id: str):
        return await self._do(
            "GET", f"/runs/{_quote(run_id)}/children", out=ListRunChildrenResponse
        )

    async def list_run_artifacts(self, run_id: str) -> list[RunArtifactResponse]:
        data = await self._do("GET", f"/runs/{_quote(run_id)}/artifacts")
        return [RunArtifactResponse.from_dict(item) for item in (data or {}).get("items", [])]

    async def list_run_messages(self, run_id: str) -> list[RunMessageResponse]:
        data = await self._do("GET", f"/runs/{_quote(run_id)}/messages")
        return [RunMessageResponse.from_dict(item) for item in (data or {}).get("items", [])]

    async def stream_run_events(
        self,
        run_id: str,
        opts: StreamRunEventsOptions | dict[str, Any] | None = None,
    ) -> AsyncIterator[StreamRunEvent]:
        opts = maybe_model(opts or {}, StreamRunEventsOptions)
        query: dict[str, Any] = {}
        _set_query(query, "after_sequence", opts.after_sequence)
        async with self._client.stream(
            "GET",
            self.endpoint(f"/runs/{_quote(run_id)}/stream", query),
            headers=self._request_headers("text/event-stream", self.user_token),
        ) as response:
            if response.status_code < 200 or response.status_code >= 300:
                body = await response.aread()
                raise self._parse_error(
                    httpx.Response(response.status_code, headers=response.headers, content=body)
                )
            async for event in read_sse(response.aiter_lines()):
                yield event

    async def run_agent_with_callbacks(
        self, req: RunAgentRequest | dict[str, Any], opts: PlatformCallbackOptions
    ):
        started = await self.start_agent_run(req)
        await self._stream_platform_callbacks(started.run_id, opts, until_terminal=True)
        return await self.get_run(started.run_id)

    async def start_agent_run_with_callbacks(
        self, req: RunAgentRequest | dict[str, Any], opts: PlatformCallbackOptions
    ):
        started = await self.start_agent_run(req)
        asyncio.create_task(
            self._stream_platform_callbacks(started.run_id, opts, until_terminal=False)
        )
        return started

    async def _stream_platform_callbacks(
        self, run_id: str, opts: PlatformCallbackOptions, *, until_terminal: bool
    ) -> StreamRunEvent | None:
        terminal = None
        try:
            async for event in self.stream_run_events(
                run_id, {"after_sequence": opts.after_sequence}
            ):
                if (
                    _matches_platform_callback_event(opts.event_types, event.event)
                    and opts.on_event
                ):
                    await _maybe_await(opts.on_event(event))
                if _is_terminal_run_event(event.event):
                    terminal = event
                    if opts.on_terminal:
                        await _maybe_await(opts.on_terminal(event))
                    if until_terminal:
                        break
            if opts.on_close:
                await _maybe_await(opts.on_close())
        except Exception as exc:
            if opts.on_error:
                await _maybe_await(opts.on_error(exc))
            raise
        return terminal

    async def create_agent(self, req: CreateAgentRequest | dict[str, Any]):
        return await self._do(
            "POST", "/creator/agents", body=maybe_model(req, CreateAgentRequest), out=AgentResponse
        )

    async def list_my_agents(self, params: ListMyAgentsParams | dict[str, Any] | None = None):
        params = maybe_model(params or {}, ListMyAgentsParams)
        query: dict[str, Any] = {}
        for key in ("query", "status", "visibility", "certification_status", "sort_by"):
            query_key = "q" if key == "query" else key
            _set_query(query, query_key, getattr(params, key))
        if params.skill_ids:
            query["skill_ids"] = ",".join(params.skill_ids)
        _set_query(query, "limit", params.limit)
        _set_query(query, "offset", params.offset)
        return await self._do("GET", "/creator/agents", query=query, out=AgentListResponse)

    async def get_my_agent(self, agent_id: str):
        return await self._do("GET", f"/creator/agents/{_quote(agent_id)}", out=AgentResponse)

    async def get_my_agent_by_slug(self, slug: str):
        return await self._do("GET", f"/creator/agents/by-slug/{_quote(slug)}", out=AgentResponse)

    async def update_agent(self, agent_id: str, req: UpdateAgentRequest | dict[str, Any]):
        return await self._do(
            "PATCH",
            f"/creator/agents/{_quote(agent_id)}",
            body=maybe_model(req, UpdateAgentRequest),
            out=AgentResponse,
        )

    async def create_agent_token(self, req: CreateAgentTokenRequest | dict[str, Any]):
        return await self._do(
            "POST",
            "/creator/agent-tokens",
            body=maybe_model(req, CreateAgentTokenRequest),
            out=AgentTokenResponse,
        )

    async def list_agent_tokens(self, params: ListAgentTokensParams | dict[str, Any] | None = None):
        params = maybe_model(params or {}, ListAgentTokensParams)
        query: dict[str, Any] = {}
        for key in ("agent_id", "sort_by", "sort_dir", "limit", "offset"):
            _set_query(query, key, getattr(params, key))
        return await self._do(
            "GET", "/creator/agent-tokens", query=query, out=AgentTokenListResponse
        )

    async def revoke_agent_token(self, token_id: str) -> None:
        await self._do("DELETE", f"/creator/agent-tokens/{_quote(token_id)}")

    def a2a_agent(self, slug: str):
        from ..a2a import A2AClient

        return A2AClient(
            self.endpoint(f"/a2a/agents/{_quote(slug)}"),
            http_client=self._client,
            token=self.user_token,
            headers=dict(self.headers),
            sdk_agent=self.sdk_agent,
        )

    # Go-style aliases for easier migration.
    ListAgents = list_agents
    GetAgent = get_agent
    GetAgentCard = get_agent_card
    RunAgent = run_agent
    StartAgentRun = start_agent_run
    GetRun = get_run
    ListRunEvents = list_run_events
    ListRunChildren = list_run_children
    ListRunArtifacts = list_run_artifacts
    ListRunMessages = list_run_messages
    StreamRunEvents = stream_run_events
    RunAgentWithCallbacks = run_agent_with_callbacks
    StartAgentRunWithCallbacks = start_agent_run_with_callbacks
    CreateAgent = create_agent
    ListMyAgents = list_my_agents
    GetMyAgent = get_my_agent
    GetMyAgentBySlug = get_my_agent_by_slug
    UpdateAgent = update_agent
    CreateAgentToken = create_agent_token
    ListAgentTokens = list_agent_tokens
    RevokeAgentToken = revoke_agent_token
    A2AAgent = a2a_agent


async def read_sse(lines: AsyncIterator[str]) -> AsyncIterator[StreamRunEvent]:
    event = StreamRunEvent(event="message")
    data: list[str] = []

    async def dispatch() -> StreamRunEvent | None:
        nonlocal event, data
        if not data:
            event = StreamRunEvent(event="message")
            return None
        event.data = "\n".join(data).encode()
        out = event
        event = StreamRunEvent(event="message")
        data = []
        return out

    async for raw_line in lines:
        line = raw_line.rstrip("\r")
        if line == "":
            out = await dispatch()
            if out is not None:
                yield out
            continue
        if line.startswith(":"):
            continue
        field, sep, value = line.partition(":")
        if sep:
            value = value.removeprefix(" ")
        if field == "event":
            event.event = value or "message"
        elif field == "id":
            event.id = value
        elif field == "data":
            data.append(value)
    out = await dispatch()
    if out is not None:
        yield out


def _matches_platform_callback_event(event_types: list[str] | None, event_type: str) -> bool:
    return not event_types or event_type in event_types


def _is_terminal_run_event(event_type: str) -> bool:
    return event_type in {"run.completed", "run.failed", "run.canceled"}
