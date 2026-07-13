from __future__ import annotations

import asyncio
import contextvars
import inspect
import os
import time
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import websockets

from ..client import Client, ClaimRuntimeRunResult
from ..model import maybe_model, to_json_value
from ..registration import (
    DEFAULT_NATIVE_API_BASE,
    DEFAULT_NATIVE_SDK_AGENT,
    DEFAULT_REGISTRATION_ENV_PATH,
    EnvRegistrationStore,
    RUNTIME_CONNECTOR_PULL,
    RUNTIME_CONNECTOR_WEBSOCKET,
    client_ensure_runtime_agent,
    ensure_runtime_agent,
    first_non_empty,
)
from ..types import (
    AgentError,
    AgentEvent,
    EnsureRuntimeAgentRequest,
    RuntimeAssignment,
    RuntimePullResultRequest,
    RuntimePullRunResponse,
    RuntimeWSClientMessage,
    RuntimeWSServerMessage,
)


AGENT_EVENT_TYPE_RUN_MESSAGE_DELTA = "run.message.delta"
_native_run_var: contextvars.ContextVar["NativeRun | None"] = contextvars.ContextVar(
    "openlinker_native_run", default=None
)


def _duration_seconds(value: int | float | timedelta | None, fallback: float) -> float:
    if isinstance(value, timedelta):
        seconds = value.total_seconds()
        return seconds if seconds > 0 else fallback
    if value and value > 0:
        return float(value)
    return fallback


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@dataclass
class RuntimeHandlers:
    on_ready: Callable[[RuntimeWSServerMessage], Any] | None = None
    on_assigned: Callable[[RuntimeAssignment], Any] | None = None
    on_message: Callable[[RuntimeWSServerMessage], Any] | None = None
    on_error: Callable[[Exception], Any] | None = None


class RuntimeConnector:
    def supports_live_events(self) -> bool:
        raise NotImplementedError

    async def start(self, handlers: RuntimeHandlers) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError

    async def send_run_event(self, run_id: str, event: AgentEvent) -> None:
        raise NotImplementedError

    async def complete_run(self, run_id: str, result: RuntimePullResultRequest) -> None:
        raise NotImplementedError


class RuntimePullConnector(RuntimeConnector):
    def __init__(
        self,
        client: Client,
        *,
        wait: float | timedelta = 25,
        heartbeat: float | timedelta = 60,
        empty_retry: float | timedelta = 5,
        max_runs: int = 0,
        stop_on_empty: bool = False,
    ) -> None:
        self.client = client
        self.wait = _duration_seconds(wait, 25)
        self.heartbeat = _duration_seconds(heartbeat, 60)
        self.empty_retry = _duration_seconds(empty_retry, 5)
        self.max_runs = max_runs
        self.stop_on_empty = stop_on_empty
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._processed = 0

    def supports_live_events(self) -> bool:
        return False

    async def start(self, handlers: RuntimeHandlers) -> None:
        if not self.client.agent_token:
            raise RuntimeError("openlinker: agent token is required")
        self._stop.clear()
        self._task = asyncio.create_task(self._loop(handlers))

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)

    async def send_run_event(self, run_id: str, event: AgentEvent) -> None:
        return None

    async def complete_run(self, run_id: str, result: RuntimePullResultRequest) -> None:
        await self.client.complete_runtime_run(run_id, result)

    async def _loop(self, handlers: RuntimeHandlers) -> None:
        last_heartbeat = 0.0
        while not self._stop.is_set() and (self.max_runs == 0 or self._processed < self.max_runs):
            if time.monotonic() - last_heartbeat >= self.heartbeat:
                try:
                    await self.client.heartbeat_agent()
                except Exception as exc:
                    if handlers.on_error:
                        await _maybe_await(handlers.on_error(exc))
                last_heartbeat = time.monotonic()
            try:
                claim = await self.client.claim_runtime_run_detailed({"wait": int(self.wait)})
                if claim.run is not None:
                    if handlers.on_assigned:
                        await _maybe_await(handlers.on_assigned(runtime_assignment_from_pull_run(claim.run)))
                    self._processed += 1
                    continue
                if self.stop_on_empty:
                    return
                await self._sleep(_retry_after_from_claim(claim, self.empty_retry))
            except Exception as exc:
                if handlers.on_error:
                    await _maybe_await(handlers.on_error(exc))
                await self._sleep(self.empty_retry)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass


class RuntimeWSConnector(RuntimeConnector):
    def __init__(
        self,
        client: Client,
        *,
        endpoint: str = "",
        reconnect: bool = True,
        reconnect_min: float | timedelta = 0.5,
        reconnect_max: float | timedelta = 10,
        heartbeat: float | timedelta = 60,
    ) -> None:
        self.client = client
        self.endpoint = endpoint or "/agent-runtime/ws"
        self.reconnect = reconnect
        self.reconnect_min = _duration_seconds(reconnect_min, 0.5)
        self.reconnect_max = _duration_seconds(reconnect_max, 10)
        self.heartbeat = _duration_seconds(heartbeat, 60)
        self._ws = None
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._send_lock = asyncio.Lock()

    def supports_live_events(self) -> bool:
        return True

    async def start(self, handlers: RuntimeHandlers) -> None:
        if not self.client.agent_token:
            raise RuntimeError("openlinker: agent token is required")
        self._stop.clear()
        await self._connect()
        self._tasks = [
            asyncio.create_task(self._read_loop(handlers)),
            asyncio.create_task(self._heartbeat_loop(handlers)),
        ]

    async def stop(self) -> None:
        self._stop.set()
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def send_run_event(self, run_id: str, event: AgentEvent) -> None:
        await self._send(
            RuntimeWSClientMessage(
                type="run.event",
                id=f"event-{run_id}-{int(time.time() * 1000)}",
                run_id=run_id,
                event_type=event.event_type,
                payload=event.payload,
            )
        )

    async def complete_run(self, run_id: str, result: RuntimePullResultRequest) -> None:
        await self._send(
            RuntimeWSClientMessage(
                type="run.result",
                id=f"result-{run_id}-{int(time.time() * 1000)}",
                run_id=run_id,
                status=result.status,
                output=result.output,
                events=result.events,
                error=result.error,
                duration_ms=result.duration_ms,
            )
        )

    async def _connect(self) -> None:
        header_arg = (
            "additional_headers"
            if "additional_headers" in inspect.signature(websockets.connect).parameters
            else "extra_headers"
        )
        self._ws = await websockets.connect(
            self.client.websocket_endpoint(self.endpoint),
            **{header_arg: self.client.runtime_websocket_headers()},
        )

    async def _read_loop(self, handlers: RuntimeHandlers) -> None:
        delay = self.reconnect_min
        while not self._stop.is_set():
            try:
                async for raw in self._ws:
                    await self._handle_message(raw, handlers)
                if not self.reconnect:
                    return
            except Exception as exc:
                if self._stop.is_set():
                    return
                if handlers.on_error:
                    await _maybe_await(handlers.on_error(exc))
            if not self.reconnect:
                return
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.reconnect_max)
            try:
                await self._connect()
                delay = self.reconnect_min
            except Exception as exc:
                if handlers.on_error:
                    await _maybe_await(handlers.on_error(exc))

    async def _heartbeat_loop(self, handlers: RuntimeHandlers) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.heartbeat)
            if self._stop.is_set():
                return
            try:
                await self._send(RuntimeWSClientMessage(type="heartbeat", id=f"heartbeat-{int(time.time() * 1000)}"))
            except Exception as exc:
                if handlers.on_error:
                    await _maybe_await(handlers.on_error(exc))

    async def _handle_message(self, raw: str | bytes, handlers: RuntimeHandlers) -> None:
        message = RuntimeWSServerMessage.from_dict(__import__("json").loads(raw))
        if handlers.on_message:
            await _maybe_await(handlers.on_message(message))
        if message.type == "runtime.ready":
            if handlers.on_ready:
                await _maybe_await(handlers.on_ready(message))
        elif message.type == "run.assigned":
            await self._send(
                RuntimeWSClientMessage(
                    type="run.assignment.accepted",
                    id=f"assignment-ack-{message.run_id}-{int(time.time() * 1000)}",
                    run_id=message.run_id,
                )
            )
            if handlers.on_assigned:
                await _maybe_await(handlers.on_assigned(runtime_assignment_from_ws_message(message)))
        elif message.type == "error":
            if handlers.on_error:
                await _maybe_await(handlers.on_error(_runtime_message_error(message)))

    async def _send(self, message: RuntimeWSClientMessage) -> None:
        async with self._send_lock:
            if self._ws is None:
                raise RuntimeError("openlinker: runtime websocket is not open")
            await self._ws.send(__import__("json").dumps(to_json_value(message)))


@dataclass
class NativeRun:
    assignment: RuntimeAssignment
    reporter: "NativeReporter"

    def text(self, *keys: str) -> str:
        return native_input_text(self.assignment.input, list(keys) or ["text", "query", "task", "prompt"])

    async def send_event(self, event: AgentEvent | dict[str, Any]) -> None:
        await self.reporter.send_event(maybe_model(event, AgentEvent))

    async def message_delta(self, text: str) -> None:
        await self.send_event(AgentEvent(event_type=AGENT_EVENT_TYPE_RUN_MESSAGE_DELTA, payload={"text": text}))

    def supports_live_events(self) -> bool:
        return self.reporter.supports_live_events()

    Text = text
    SendEvent = send_event
    MessageDelta = message_delta
    SupportsLiveEvents = supports_live_events


@dataclass
class NativeReporter:
    connector: RuntimeConnector | None
    run_id: str

    def supports_live_events(self) -> bool:
        return self.connector is not None and self.connector.supports_live_events()

    async def send_event(self, event: AgentEvent) -> None:
        if self.connector is not None and self.connector.supports_live_events():
            await self.connector.send_run_event(self.run_id, event)


@dataclass
class NativeResult:
    status: str = ""
    output: Any = None
    events: list[AgentEvent] | None = None
    error: AgentError | None = None

    @classmethod
    def success(cls, output: Any = None, events: list[AgentEvent] | None = None) -> "NativeResult":
        return cls(status="success", output=output, events=events or [])

    @classmethod
    def failed(cls, code: str, message: str, events: list[AgentEvent] | None = None) -> "NativeResult":
        return cls(status="failed", events=events or [], error=AgentError(code=code, message=message))


NativeHandler = Callable[[NativeRun], Awaitable[Any] | Any]


class NativeRunner:
    def __init__(self, handler: NativeHandler) -> None:
        self.handler = handler
        self.client: Client | None = None
        self.api_base = ""
        self.runtime_token = ""
        self.connector = ""
        self.pull_wait: float | timedelta | None = None
        self.max_runs = 0
        self.sdk_agent = ""
        self.on_ready: Callable[[RuntimeWSServerMessage], Any] | None = None
        self.on_error: Callable[[Exception], Any] | None = None

    def with_client(self, client: Client) -> "NativeRunner":
        self.client = client
        return self

    def with_api_base(self, api_base: str) -> "NativeRunner":
        self.api_base = api_base.strip()
        return self

    def with_runtime_token(self, token: str) -> "NativeRunner":
        self.runtime_token = token.strip()
        return self

    def with_connector(self, connector: str) -> "NativeRunner":
        self.connector = connector.strip()
        return self

    def with_pull_wait(self, wait: float | timedelta) -> "NativeRunner":
        self.pull_wait = wait
        return self

    def with_max_runs(self, max_runs: int) -> "NativeRunner":
        self.max_runs = max_runs
        return self

    def with_sdk_agent(self, agent: str) -> "NativeRunner":
        self.sdk_agent = agent.strip()
        return self

    def with_ready_handler(self, fn: Callable[[RuntimeWSServerMessage], Any]) -> "NativeRunner":
        self.on_ready = fn
        return self

    def with_error_handler(self, fn: Callable[[Exception], Any]) -> "NativeRunner":
        self.on_error = fn
        return self

    async def register(self, req: EnsureRuntimeAgentRequest) -> Any:
        req.api_base = first_non_empty(req.api_base, self.api_base, os.getenv("OPENLINKER_API_BASE"), DEFAULT_NATIVE_API_BASE)
        req.runtime_token = first_non_empty(
            req.runtime_token, self.runtime_token, os.getenv("OPENLINKER_RUNTIME_TOKEN"), os.getenv("OPENLINKER_AGENT_TOKEN")
        )
        req.connector = first_non_empty(req.connector, self.connector, os.getenv("OPENLINKER_WORKER_CONNECTOR"), RUNTIME_CONNECTOR_PULL)
        req.user_token = first_non_empty(req.user_token, os.getenv("OPENLINKER_USER_TOKEN"))
        client = self.client or Client(
            req.api_base,
            user_token=req.user_token,
            sdk_agent=first_non_empty(self.sdk_agent, DEFAULT_NATIVE_SDK_AGENT),
        )
        reg = await client_ensure_runtime_agent(client, req)
        self.api_base = first_non_empty(self.api_base, reg.api_base)
        self.runtime_token = reg.runtime_token or ""
        self.connector = first_non_empty(self.connector, reg.connector)
        return reg

    async def run_or_register(self, req: EnsureRuntimeAgentRequest) -> None:
        await self.register(req)
        await self.run()

    async def run(self) -> None:
        if self.handler is None:
            raise RuntimeError("openlinker: native handler is required")
        client = self._runtime_client()
        connector = self._runtime_connector(client)
        max_runs = _first_int(self.max_runs, "OPENLINKER_WORKER_MAX_RUNS", 0)
        if isinstance(connector, RuntimePullConnector):
            connector.max_runs = max_runs
        stop = asyncio.Event()
        completed = 0
        errors: asyncio.Queue[Exception] = asyncio.Queue(maxsize=1)

        async def on_assigned(assignment: RuntimeAssignment) -> None:
            nonlocal completed
            asyncio.create_task(handle_assignment(assignment))

        async def handle_assignment(assignment: RuntimeAssignment) -> None:
            nonlocal completed
            started = time.monotonic()
            result = await self._invoke_handler(connector, assignment)
            result.duration_ms = native_duration_ms(started)
            try:
                await connector.complete_run(assignment.run_id, result)
            except Exception as exc:
                if errors.empty():
                    await errors.put(exc)
                stop.set()
                return
            completed += 1
            if max_runs > 0 and completed >= max_runs:
                stop.set()

        async def on_error(exc: Exception) -> None:
            if self.on_error:
                await _maybe_await(self.on_error(exc))

        await connector.start(
            RuntimeHandlers(on_ready=self.on_ready, on_assigned=on_assigned, on_error=on_error)
        )
        waiter = asyncio.create_task(stop.wait())
        error_waiter = asyncio.create_task(errors.get())
        done, pending = await asyncio.wait({waiter, error_waiter}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await connector.stop()
        if error_waiter in done:
            raise error_waiter.result()

    def _runtime_client(self) -> Client:
        if self.client is not None:
            return self.client
        api_base = first_non_empty(self.api_base, os.getenv("OPENLINKER_API_BASE"), DEFAULT_NATIVE_API_BASE)
        token = first_non_empty(self.runtime_token, os.getenv("OPENLINKER_RUNTIME_TOKEN"), os.getenv("OPENLINKER_AGENT_TOKEN"))
        if not token:
            raise RuntimeError("openlinker: OPENLINKER_RUNTIME_TOKEN is required")
        return Client(api_base, runtime_token=token, sdk_agent=first_non_empty(self.sdk_agent, DEFAULT_NATIVE_SDK_AGENT))

    def _runtime_connector(self, client: Client) -> RuntimeConnector:
        mode = first_non_empty(self.connector, os.getenv("OPENLINKER_WORKER_CONNECTOR"), RUNTIME_CONNECTOR_PULL)
        if mode == RUNTIME_CONNECTOR_PULL:
            return RuntimePullConnector(
                client,
                wait=_first_duration(self.pull_wait, "OPENLINKER_WORKER_PULL_WAIT", 25),
                max_runs=_first_int(self.max_runs, "OPENLINKER_WORKER_MAX_RUNS", 0),
            )
        if mode == RUNTIME_CONNECTOR_WEBSOCKET:
            return RuntimeWSConnector(client)
        raise RuntimeError(
            f'openlinker: runtime connector must be "{RUNTIME_CONNECTOR_PULL}" or "{RUNTIME_CONNECTOR_WEBSOCKET}"'
        )

    async def _invoke_handler(self, connector: RuntimeConnector, assignment: RuntimeAssignment) -> RuntimePullResultRequest:
        run = NativeRun(assignment=assignment, reporter=NativeReporter(connector, assignment.run_id))
        token = _native_run_var.set(run)
        try:
            output = await _maybe_await(self.handler(run))
            return native_runtime_result(output, None)
        except Exception as exc:
            return native_runtime_result(None, exc)
        finally:
            _native_run_var.reset(token)

    WithClient = with_client
    WithAPIBase = with_api_base
    WithRuntimeToken = with_runtime_token
    WithConnector = with_connector
    WithPullWait = with_pull_wait
    WithMaxRuns = with_max_runs
    WithSDKAgent = with_sdk_agent
    WithReadyHandler = with_ready_handler
    WithErrorHandler = with_error_handler
    Register = register
    RunOrRegister = run_or_register
    Run = run


def Native(handler: NativeHandler) -> NativeRunner:
    return NativeRunner(handler)


def native_run_from_context() -> NativeRun | None:
    return _native_run_var.get()


NativeRunFromContext = native_run_from_context


class NativeTextAgent:
    async def run(self, input: str) -> str:
        raise NotImplementedError


class NativeAgentRunner:
    def __init__(self, agent: NativeTextAgent | Callable[[str], Any]) -> None:
        self.agent = agent
        self.model = ""
        self.sdk_agent = DEFAULT_NATIVE_SDK_AGENT
        self.fallback_input = "hello"
        self.native = Native(self._handle_run).with_sdk_agent(self.sdk_agent)

    def with_model(self, model: str) -> "NativeAgentRunner":
        self.model = model.strip()
        return self

    def with_sdk_agent(self, agent: str) -> "NativeAgentRunner":
        if agent.strip():
            self.sdk_agent = agent.strip()
            self.native.with_sdk_agent(self.sdk_agent)
        return self

    def with_fallback_input(self, input: str) -> "NativeAgentRunner":
        self.fallback_input = input.strip()
        return self

    def with_client(self, client: Client) -> "NativeAgentRunner":
        self.native.with_client(client)
        return self

    def with_api_base(self, api_base: str) -> "NativeAgentRunner":
        self.native.with_api_base(api_base)
        return self

    def with_runtime_token(self, token: str) -> "NativeAgentRunner":
        self.native.with_runtime_token(token)
        return self

    def with_connector(self, connector: str) -> "NativeAgentRunner":
        self.native.with_connector(connector)
        return self

    def with_pull_wait(self, wait: float | timedelta) -> "NativeAgentRunner":
        self.native.with_pull_wait(wait)
        return self

    def with_max_runs(self, max_runs: int) -> "NativeAgentRunner":
        self.native.with_max_runs(max_runs)
        return self

    def with_ready_handler(self, fn: Callable[[RuntimeWSServerMessage], Any]) -> "NativeAgentRunner":
        self.native.with_ready_handler(fn)
        return self

    def with_error_handler(self, fn: Callable[[Exception], Any]) -> "NativeAgentRunner":
        self.native.with_error_handler(fn)
        return self

    async def register(self, req: EnsureRuntimeAgentRequest) -> Any:
        return await self.native.register(req)

    async def run_or_register(self, req: EnsureRuntimeAgentRequest) -> None:
        await self.native.run_or_register(req)

    async def run(self) -> None:
        await self.native.run()

    async def _handle_run(self, run: NativeRun) -> NativeResult:
        text = run.text() or self.fallback_input
        progress = AgentEvent(event_type=AGENT_EVENT_TYPE_RUN_MESSAGE_DELTA, payload={"text": f"native worker received: {text}"})
        await run.send_event(progress)
        try:
            if hasattr(self.agent, "run"):
                answer = await _maybe_await(self.agent.run(text))
            else:
                answer = await _maybe_await(self.agent(text))
        except Exception as exc:
            return NativeResult.failed("AGENT_WORKER_ERROR", str(exc), [progress])
        answer = str(answer).strip()
        if not answer:
            return NativeResult.failed("AGENT_WORKER_ERROR", "agent returned empty response", [progress])
        events = [AgentEvent(event_type=AGENT_EVENT_TYPE_RUN_MESSAGE_DELTA, payload={"text": answer})]
        if not run.supports_live_events():
            events.insert(0, progress)
        output = {"text": answer, "input": {"text": text, "raw": run.assignment.input}}
        if self.model:
            output["llm"] = {"text": answer, "run_id": run.assignment.run_id, "agent_id": run.assignment.agent_id, "model": self.model}
        return NativeResult(status="success", output=output, events=events)


def WithAgent(agent: NativeTextAgent | Callable[[str], Any]) -> NativeAgentRunner:
    return NativeAgentRunner(agent)


def WithFunc(fn: Callable[[str], Any]) -> NativeAgentRunner:
    return WithAgent(fn)


NativeAgentRunner.WithModel = NativeAgentRunner.with_model
NativeAgentRunner.WithSDKAgent = NativeAgentRunner.with_sdk_agent
NativeAgentRunner.WithFallbackInput = NativeAgentRunner.with_fallback_input
NativeAgentRunner.WithClient = NativeAgentRunner.with_client
NativeAgentRunner.WithAPIBase = NativeAgentRunner.with_api_base
NativeAgentRunner.WithRuntimeToken = NativeAgentRunner.with_runtime_token
NativeAgentRunner.WithConnector = NativeAgentRunner.with_connector
NativeAgentRunner.WithPullWait = NativeAgentRunner.with_pull_wait
NativeAgentRunner.WithMaxRuns = NativeAgentRunner.with_max_runs
NativeAgentRunner.WithReadyHandler = NativeAgentRunner.with_ready_handler
NativeAgentRunner.WithErrorHandler = NativeAgentRunner.with_error_handler
NativeAgentRunner.Register = NativeAgentRunner.register
NativeAgentRunner.RunOrRegister = NativeAgentRunner.run_or_register
NativeAgentRunner.Run = NativeAgentRunner.run


def runtime_assignment_from_pull_run(run: RuntimePullRunResponse | None) -> RuntimeAssignment:
    if run is None:
        return RuntimeAssignment()
    return RuntimeAssignment(
        type="run.assigned",
        run_id=run.run_id,
        agent_id=run.agent_id,
        input=run.input,
        metadata=run.metadata,
        source=run.source,
        result_endpoint=run.result_endpoint,
        result_method=run.result_method,
        result_required=run.result_required,
        a2a=run.a2a,
        conversation=run.conversation,
    )


def runtime_assignment_from_ws_message(message: RuntimeWSServerMessage) -> RuntimeAssignment:
    return RuntimeAssignment(
        type=message.type,
        run_id=message.run_id or "",
        agent_id=message.agent_id,
        input=message.input,
        metadata=message.metadata,
        source=message.source,
        result_endpoint=message.result_endpoint,
        result_method=message.result_method,
        result_required=message.result_required,
        a2a=message.a2a,
        conversation=message.conversation,
    )


def native_runtime_result(output: Any, err: Exception | None) -> RuntimePullResultRequest:
    if err is not None:
        return RuntimePullResultRequest(
            status="failed",
            error=AgentError(code="AGENT_RUNTIME_ERROR", message=str(err)),
        )
    if isinstance(output, NativeResult):
        result = output
    elif isinstance(output, RuntimePullResultRequest):
        return output
    else:
        result = NativeResult(output=output)
    status = result.status or "success"
    if result.error is not None and status == "success":
        status = "failed"
    return RuntimePullResultRequest(status=status, output=result.output, events=result.events or [], error=result.error)


def native_input_text(input_value: Any, keys: list[str]) -> str:
    if input_value is None:
        return ""
    if isinstance(input_value, str):
        return input_value.strip()
    if isinstance(input_value, dict):
        for key in keys:
            value = input_value.get(key)
            text = "" if value is None else str(value).strip()
            if text and text != "<nil>":
                return text
    return str(input_value).strip()


def native_duration_ms(started: float) -> int:
    ms = int((time.monotonic() - started) * 1000)
    return min(max(ms, 1), 2_147_483_647)


def _first_duration(value: float | timedelta | None, env_key: str, fallback: float) -> float:
    if isinstance(value, timedelta):
        return value.total_seconds() if value.total_seconds() > 0 else fallback
    if value and value > 0:
        return float(value)
    raw = os.getenv(env_key, "").strip()
    if not raw:
        return fallback
    try:
        return float(raw.removesuffix("s"))
    except ValueError:
        return fallback


def _first_int(value: int, env_key: str, fallback: int) -> int:
    if value > 0:
        return value
    try:
        parsed = int(os.getenv(env_key, "").strip())
        return parsed if parsed >= 0 else fallback
    except ValueError:
        return fallback


def _retry_after_from_claim(claim: ClaimRuntimeRunResult, fallback: float) -> float:
    if claim.retry_after:
        return claim.retry_after.total_seconds()
    return fallback


def _runtime_message_error(message: RuntimeWSServerMessage) -> Exception:
    if message.error:
        if message.error.code:
            return RuntimeError(f"openlinker: runtime websocket error: {message.error.code}: {message.error.message}")
        return RuntimeError(f"openlinker: runtime websocket error: {message.error.message}")
    return RuntimeError("openlinker: runtime websocket error")
