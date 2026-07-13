from __future__ import annotations

import inspect
import json
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode, urlparse

import httpx

from .client import DEFAULT_SDK_AGENT, MAX_RESPONSE_BODY_BYTES, read_sse
from .error import OpenLinkerError
from .model import Model, jfield, maybe_model, to_json_value
from .types import AgentCardResponse, StreamRunEvent


A2A_DIALECT_CURRENT = "current"
A2A_DIALECT_LEGACY = "legacy"

A2A_METHOD_MESSAGE_SEND = "SendMessage"
A2A_METHOD_MESSAGE_STREAM = "SendStreamingMessage"
A2A_METHOD_TASKS_GET = "GetTask"
A2A_METHOD_TASKS_LIST = "ListTasks"
A2A_METHOD_TASKS_CANCEL = "CancelTask"
A2A_METHOD_TASKS_RESUBSCRIBE = "SubscribeToTask"
A2A_METHOD_TASK_PUSH_NOTIFICATION_SET = "CreateTaskPushNotificationConfig"
A2A_METHOD_TASK_PUSH_NOTIFICATION_GET = "GetTaskPushNotificationConfig"
A2A_METHOD_TASK_PUSH_NOTIFICATION_LIST = "ListTaskPushNotificationConfigs"
A2A_METHOD_TASK_PUSH_NOTIFICATION_DELETE = "DeleteTaskPushNotificationConfig"
A2A_METHOD_AGENT_GET_EXTENDED_CARD = "GetExtendedAgentCard"

A2A_LEGACY_METHODS = {
    A2A_METHOD_MESSAGE_SEND: "message/send",
    A2A_METHOD_MESSAGE_STREAM: "message/stream",
    A2A_METHOD_TASKS_GET: "tasks/get",
    A2A_METHOD_TASKS_LIST: "tasks/list",
    A2A_METHOD_TASKS_CANCEL: "tasks/cancel",
    A2A_METHOD_TASKS_RESUBSCRIBE: "tasks/resubscribe",
    A2A_METHOD_TASK_PUSH_NOTIFICATION_SET: "tasks/pushNotificationConfig/set",
    A2A_METHOD_TASK_PUSH_NOTIFICATION_GET: "tasks/pushNotificationConfig/get",
    A2A_METHOD_TASK_PUSH_NOTIFICATION_LIST: "tasks/pushNotificationConfig/list",
    A2A_METHOD_TASK_PUSH_NOTIFICATION_DELETE: "tasks/pushNotificationConfig/delete",
    A2A_METHOD_AGENT_GET_EXTENDED_CARD: "agent/getExtendedCard",
}


def normalize_a2a_dialect(dialect: str) -> str:
    dialect = (dialect or "").strip().lower()
    return A2A_DIALECT_LEGACY if dialect == A2A_DIALECT_LEGACY else A2A_DIALECT_CURRENT


@dataclass
class A2AJSONRPCError(Exception):
    code: Any = None
    message: str = ""
    data: Any = None

    def __str__(self) -> str:
        if not self.message:
            return f"openlinker: A2A JSON-RPC error: {self.code}"
        return f"openlinker: A2A JSON-RPC error {self.code}: {self.message}"


@dataclass
class A2AMessage(Model):
    kind: str | None = None
    message_id: str | None = jfield("messageId", None)
    context_id: str | None = jfield("contextId", None)
    task_id: str | None = jfield("taskId", None)
    reference_task_ids: list[str] = jfield("referenceTaskIds", default_factory=list)
    extensions: list[str] = jfield(default_factory=list)
    role: str | None = None
    parts: list[dict[str, Any]] = jfield(default_factory=list)
    metadata: dict[str, Any] | None = None


@dataclass
class A2APushAuthenticationInfo(Model):
    scheme: str | None = None
    credentials: str | None = None


@dataclass
class A2APushNotificationConfig(Model):
    id: str | None = None
    url: str | None = None
    token: str | None = None
    secret: str | None = None
    authentication: A2APushAuthenticationInfo | None = None
    metadata: dict[str, Any] | None = None
    event_types: list[str] = jfield("eventTypes", default_factory=list)
    event_types_alias: list[str] = jfield("event_types", default_factory=list)


@dataclass
class A2ATaskPushNotificationConfig(Model):
    tenant: str | None = None
    id: str | None = None
    task_id: str | None = jfield("taskId", None)
    url: str | None = None
    token: str | None = None
    secret: str | None = None
    authentication: A2APushAuthenticationInfo | None = None
    metadata: dict[str, Any] | None = None
    event_types: list[str] = jfield("eventTypes", default_factory=list)
    event_types_alias: list[str] = jfield("event_types", default_factory=list)
    push_notification_config: A2APushNotificationConfig | None = jfield(
        "pushNotificationConfig", None
    )


@dataclass
class A2ASendConfiguration(Model):
    accepted_output_modes: list[str] = jfield("acceptedOutputModes", default_factory=list)
    blocking: bool | None = None
    return_immediately: bool | None = jfield("returnImmediately", None)
    push_notification_config: A2APushNotificationConfig | None = jfield(
        "pushNotificationConfig", None
    )
    task_push_notification_config: A2ATaskPushNotificationConfig | None = jfield(
        "taskPushNotificationConfig", None
    )
    history_length: int | None = jfield("historyLength", None)


@dataclass
class A2AMessageSendParams(Model):
    message: A2AMessage | None = None
    configuration: A2ASendConfiguration | None = None
    metadata: dict[str, Any] | None = None


@dataclass
class A2ATaskPushConfigParams(Model):
    id: str | None = None
    task_id: str | None = jfield("taskId", None)
    push_notification_config_id: str | None = jfield("pushNotificationConfigId", None)
    push_notification_config: A2APushNotificationConfig | None = jfield(
        "pushNotificationConfig", None
    )
    url: str | None = None
    token: str | None = None
    secret: str | None = None
    authentication: A2APushAuthenticationInfo | None = None
    metadata: dict[str, Any] | None = None
    event_types: list[str] = jfield("eventTypes", default_factory=list)
    event_types_alias: list[str] = jfield("event_types", default_factory=list)
    page_size: int | None = jfield("pageSize", None)
    page_token: str | None = jfield("pageToken", None)


@dataclass
class A2ATaskQueryParams(Model):
    id: str = ""
    history_length: int | None = jfield("historyLength", None)


@dataclass
class A2ATaskListParams(Model):
    context_id: str | None = jfield("contextId", None)
    status: str | None = None
    page_size: int | None = jfield("pageSize", None)
    page_token: str | None = jfield("pageToken", None)
    history_length: int | None = jfield("historyLength", None)
    status_timestamp_after: str | None = jfield("statusTimestampAfter", None)
    include_artifacts: bool | None = jfield("includeArtifacts", None)


@dataclass
class A2ATaskStatus(Model):
    state: str = ""
    timestamp: str | None = None
    message: A2AMessage | None = None


@dataclass
class A2AArtifact(Model):
    artifact_id: str | None = jfield("artifactId", None)
    name: str | None = None
    extensions: list[str] = jfield(default_factory=list)
    parts: list[dict[str, Any]] = jfield(default_factory=list)
    metadata: dict[str, Any] | None = None


@dataclass
class A2ATask(Model):
    kind: str | None = None
    id: str = ""
    context_id: str | None = jfield("contextId", None)
    status: A2ATaskStatus | None = None
    artifacts: list[A2AArtifact] = jfield(default_factory=list)
    history: list[A2AMessage] = jfield(default_factory=list)
    metadata: dict[str, Any] | None = None


@dataclass
class A2ATaskListResponse(Model):
    tasks: list[A2ATask] = jfield(default_factory=list)
    next_page_token: str | None = jfield("nextPageToken", None)
    page_size: int = jfield("pageSize", 0)
    total_size: int = jfield("totalSize", 0)


@dataclass
class A2ATaskStatusUpdateEvent(Model):
    kind: str | None = None
    task_id: str | None = jfield("taskId", None)
    context_id: str | None = jfield("contextId", None)
    status: A2ATaskStatus | None = None
    final: bool = False
    metadata: dict[str, Any] | None = None


@dataclass
class A2ATaskArtifactUpdateEvent(Model):
    kind: str | None = None
    task_id: str | None = jfield("taskId", None)
    context_id: str | None = jfield("contextId", None)
    artifact: A2AArtifact | None = None
    append: bool = False
    last_chunk: bool = jfield("lastChunk", False)
    metadata: dict[str, Any] | None = None


@dataclass
class A2AStreamResponse(Model):
    task: A2ATask | None = None
    message: A2AMessage | None = None
    status_update: A2ATaskStatusUpdateEvent | None = jfield("statusUpdate", None)
    artifact_update: A2ATaskArtifactUpdateEvent | None = jfield("artifactUpdate", None)


@dataclass
class A2ASendMessageResponse(Model):
    task: A2ATask | None = None
    message: A2AMessage | None = None


@dataclass
class A2ATaskPushConfigList(Model):
    configs: list[A2ATaskPushNotificationConfig] = jfield(default_factory=list)
    next_page_token: str | None = jfield("nextPageToken", None)
    items: list[A2ATaskPushNotificationConfig] = jfield(default_factory=list)

    @classmethod
    def from_dict(cls, data):
        obj = super().from_dict(data)
        if not obj.configs and obj.items:
            obj.configs = obj.items
        if not obj.items and obj.configs:
            obj.items = obj.configs
        return obj


@dataclass
class A2AStreamEvent:
    id: str = ""
    event: str = ""
    raw: bytes = b""
    result: A2AStreamResponse | None = None


class A2AClient:
    def __init__(
        self,
        endpoint: str,
        *,
        token: str = "",
        headers: dict[str, str] | None = None,
        http_client: httpx.AsyncClient | None = None,
        protocol_version: str = "1.0",
        dialect: str = A2A_DIALECT_CURRENT,
        sdk_agent: str = DEFAULT_SDK_AGENT,
    ) -> None:
        endpoint = endpoint.strip()
        if not endpoint:
            raise ValueError("openlinker: A2A endpoint is required")
        parsed = urlparse(endpoint)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("openlinker: A2A endpoint must include scheme and host")
        self.endpoint = endpoint
        self.token = token.strip()
        self.headers = dict(headers or {})
        self.http_client = http_client or httpx.AsyncClient(timeout=None)
        self._owns_client = http_client is None
        self.protocol_version = protocol_version.strip() or "1.0"
        self.dialect = normalize_a2a_dialect(dialect)
        self.sdk_agent = sdk_agent.strip() or DEFAULT_SDK_AGENT
        self._rpc_counter = 0

    async def aclose(self) -> None:
        if self._owns_client:
            await self.http_client.aclose()

    def _headers(
        self, accept: str, content_type: str = "application/json", has_body: bool = True
    ) -> dict[str, str]:
        headers = {"Accept": accept, "X-OpenLinker-SDK": self.sdk_agent}
        if has_body:
            headers["Content-Type"] = content_type
        if self.protocol_version:
            headers["A2A-Version"] = self.protocol_version
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        headers.update(self.headers)
        return headers

    def _method(self, method: str) -> str:
        return (
            A2A_LEGACY_METHODS.get(method, method) if self.dialect == A2A_DIALECT_LEGACY else method
        )

    async def call(self, method: str, params: Any = None) -> Any:
        response = await self._do_jsonrpc(method, params, "application/json")
        if response.get("error"):
            err = response["error"]
            raise A2AJSONRPCError(err.get("code"), err.get("message", ""), err.get("data"))
        return response.get("result")

    async def send_message_response(
        self, params: A2AMessageSendParams | dict[str, Any]
    ) -> A2ASendMessageResponse:
        result = await self.call(A2A_METHOD_MESSAGE_SEND, maybe_model(params, A2AMessageSendParams))
        return decode_a2a_send_message_response(result)

    async def send_message(self, params: A2AMessageSendParams | dict[str, Any]) -> A2ATask:
        resp = await self.send_message_response(params)
        if resp.task is not None:
            return resp.task
        if resp.message is not None:
            raise RuntimeError(
                "openlinker: A2A SendMessage returned a message; use send_message_response"
            )
        raise RuntimeError("openlinker: A2A SendMessage returned an empty response")

    async def stream_message(
        self, params: A2AMessageSendParams | dict[str, Any]
    ) -> AsyncIterator[A2AStreamEvent]:
        async for event in self._stream_jsonrpc(
            A2A_METHOD_MESSAGE_STREAM, maybe_model(params, A2AMessageSendParams)
        ):
            yield event

    async def get_task(self, params: A2ATaskQueryParams | dict[str, Any]) -> A2ATask:
        return A2ATask.from_dict(
            await self.call(A2A_METHOD_TASKS_GET, maybe_model(params, A2ATaskQueryParams))
        )

    async def list_tasks(self, params: A2ATaskListParams | dict[str, Any]) -> A2ATaskListResponse:
        return A2ATaskListResponse.from_dict(
            await self.call(A2A_METHOD_TASKS_LIST, maybe_model(params, A2ATaskListParams))
        )

    async def cancel_task(self, params: A2ATaskQueryParams | dict[str, Any]) -> A2ATask:
        return A2ATask.from_dict(
            await self.call(A2A_METHOD_TASKS_CANCEL, maybe_model(params, A2ATaskQueryParams))
        )

    async def resubscribe_task(
        self, params: A2ATaskQueryParams | dict[str, Any]
    ) -> AsyncIterator[A2AStreamEvent]:
        async for event in self._stream_jsonrpc(
            A2A_METHOD_TASKS_RESUBSCRIBE, maybe_model(params, A2ATaskQueryParams)
        ):
            yield event

    async def set_task_push_notification_config(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ):
        return A2ATaskPushNotificationConfig.from_dict(
            await self.call(
                A2A_METHOD_TASK_PUSH_NOTIFICATION_SET, maybe_model(params, A2ATaskPushConfigParams)
            )
        )

    async def get_task_push_notification_config(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ):
        return A2ATaskPushNotificationConfig.from_dict(
            await self.call(
                A2A_METHOD_TASK_PUSH_NOTIFICATION_GET, maybe_model(params, A2ATaskPushConfigParams)
            )
        )

    async def list_task_push_notification_configs(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ):
        return A2ATaskPushConfigList.from_dict(
            await self.call(
                A2A_METHOD_TASK_PUSH_NOTIFICATION_LIST, maybe_model(params, A2ATaskPushConfigParams)
            )
        )

    async def delete_task_push_notification_config(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ) -> None:
        await self.call(
            A2A_METHOD_TASK_PUSH_NOTIFICATION_DELETE, maybe_model(params, A2ATaskPushConfigParams)
        )

    async def get_extended_agent_card(self) -> AgentCardResponse:
        return AgentCardResponse.from_dict(await self.call(A2A_METHOD_AGENT_GET_EXTENDED_CARD, {}))

    async def send_message_rest(
        self, params: A2AMessageSendParams | dict[str, Any]
    ) -> A2ASendMessageResponse:
        raw = await self._do_rest(
            "POST", "/message:send", body=maybe_model(params, A2AMessageSendParams)
        )
        return decode_a2a_send_message_response(raw)

    async def get_task_rest(self, params: A2ATaskQueryParams | dict[str, Any]) -> A2ATask:
        params = maybe_model(params, A2ATaskQueryParams)
        query = {}
        if params.history_length is not None:
            query["historyLength"] = str(params.history_length)
        return A2ATask.from_dict(
            await self._do_rest("GET", f"/tasks/{quote(params.id, safe='')}", query=query)
        )

    async def list_tasks_rest(
        self, params: A2ATaskListParams | dict[str, Any]
    ) -> A2ATaskListResponse:
        params = maybe_model(params, A2ATaskListParams)
        query = _a2a_task_list_query(params)
        return A2ATaskListResponse.from_dict(await self._do_rest("GET", "/tasks", query=query))

    async def cancel_task_rest(self, params: A2ATaskQueryParams | dict[str, Any]) -> A2ATask:
        params = maybe_model(params, A2ATaskQueryParams)
        return A2ATask.from_dict(
            await self._do_rest("POST", f"/tasks/{quote(params.id, safe='')}:cancel")
        )

    async def stream_message_rest(
        self, params: A2AMessageSendParams | dict[str, Any]
    ) -> AsyncIterator[A2AStreamEvent]:
        async for event in self._stream_rest(
            "POST",
            "/message:stream",
            body=maybe_model(params, A2AMessageSendParams),
        ):
            yield event

    async def resubscribe_task_rest(
        self, params: A2ATaskQueryParams | dict[str, Any]
    ) -> AsyncIterator[A2AStreamEvent]:
        params = maybe_model(params, A2ATaskQueryParams)
        query = {}
        if params.history_length is not None:
            query["historyLength"] = str(params.history_length)
        async for event in self._stream_rest(
            "GET",
            f"/tasks/{quote(params.id, safe='')}/subscribe",
            query=query,
        ):
            yield event

    async def set_task_push_notification_config_rest(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ):
        params = maybe_model(params, A2ATaskPushConfigParams)
        task_id = _a2a_task_id_from_push_params(params)
        return A2ATaskPushNotificationConfig.from_dict(
            await self._do_rest(
                "POST",
                f"/tasks/{quote(task_id, safe='')}/pushNotificationConfigs",
                body=params,
            )
        )

    async def get_task_push_notification_config_rest(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ):
        params = maybe_model(params, A2ATaskPushConfigParams)
        task_id = _a2a_task_id_from_push_params(params)
        config_id = _a2a_push_config_id(params)
        return A2ATaskPushNotificationConfig.from_dict(
            await self._do_rest(
                "GET",
                f"/tasks/{quote(task_id, safe='')}/pushNotificationConfigs/{quote(config_id, safe='')}",
            )
        )

    async def list_task_push_notification_configs_rest(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ):
        params = maybe_model(params, A2ATaskPushConfigParams)
        task_id = _a2a_task_id_from_push_params(params)
        query = {}
        if params.page_size is not None:
            query["pageSize"] = str(params.page_size)
        if params.page_token:
            query["pageToken"] = params.page_token
        return A2ATaskPushConfigList.from_dict(
            await self._do_rest(
                "GET",
                f"/tasks/{quote(task_id, safe='')}/pushNotificationConfigs",
                query=query,
            )
        )

    async def delete_task_push_notification_config_rest(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ) -> None:
        params = maybe_model(params, A2ATaskPushConfigParams)
        task_id = _a2a_task_id_from_push_params(params)
        config_id = _a2a_push_config_id(params)
        await self._do_rest(
            "DELETE",
            f"/tasks/{quote(task_id, safe='')}/pushNotificationConfigs/{quote(config_id, safe='')}",
        )

    async def get_extended_agent_card_rest(self) -> AgentCardResponse:
        return AgentCardResponse.from_dict(await self._do_rest("GET", "/extendedAgentCard"))

    async def _do_jsonrpc(self, method: str, params: Any, accept: str) -> dict[str, Any]:
        self._rpc_counter += 1
        payload = {
            "jsonrpc": "2.0",
            "id": f"openlinker-a2a-{int(time.time() * 1000)}-{self._rpc_counter}",
            "method": self._method(method),
        }
        if params is not None:
            payload["params"] = to_json_value(params)
        response = await self.http_client.post(
            self.endpoint,
            content=json.dumps(payload).encode(),
            headers=self._headers(accept),
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise _parse_a2a_http_error(response)
        return response.json()

    async def _stream_jsonrpc(self, method: str, params: Any) -> AsyncIterator[A2AStreamEvent]:
        async with self.http_client.stream(
            "POST",
            self.endpoint,
            content=json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": f"openlinker-a2a-{int(time.time() * 1000)}",
                    "method": self._method(method),
                    "params": to_json_value(params),
                }
            ).encode(),
            headers=self._headers("text/event-stream"),
        ) as response:
            if response.status_code < 200 or response.status_code >= 300:
                body = await response.aread()
                raise _parse_a2a_http_error(
                    httpx.Response(response.status_code, headers=response.headers, content=body)
                )
            async for event in read_sse(response.aiter_lines()):
                yield a2a_stream_event_from_sse(event)

    async def _do_rest(
        self, method: str, path: str, query: dict[str, str] | None = None, body: Any = None
    ) -> Any:
        response = await self.http_client.request(
            method,
            self._rest_url(path, query),
            content=json.dumps(to_json_value(body)).encode() if body is not None else None,
            headers=self._headers("application/a2a+json", "application/a2a+json", body is not None),
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise _parse_a2a_http_error(response)
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    async def _stream_rest(
        self,
        method: str,
        path: str,
        query: dict[str, str] | None = None,
        body: Any = None,
    ) -> AsyncIterator[A2AStreamEvent]:
        async with self.http_client.stream(
            method,
            self._rest_url(path, query),
            content=json.dumps(to_json_value(body)).encode() if body is not None else None,
            headers=self._headers("text/event-stream", "application/a2a+json", body is not None),
        ) as response:
            if response.status_code < 200 or response.status_code >= 300:
                body_bytes = await response.aread()
                raise _parse_a2a_http_error(
                    httpx.Response(
                        response.status_code, headers=response.headers, content=body_bytes
                    )
                )
            async for event in read_sse(response.aiter_lines()):
                yield a2a_stream_event_from_sse(event)

    def _rest_url(self, path: str, query: dict[str, str] | None = None) -> str:
        raw = self.endpoint.rstrip("/") + "/" + path.lstrip("/")
        if query:
            raw += "?" + urlencode(query)
        return raw

    Call = call
    SendMessage = send_message
    SendMessageResponse = send_message_response
    StreamMessage = stream_message
    GetTask = get_task
    ListTasks = list_tasks
    CancelTask = cancel_task
    ResubscribeTask = resubscribe_task
    SetTaskPushNotificationConfig = set_task_push_notification_config
    GetTaskPushNotificationConfig = get_task_push_notification_config
    ListTaskPushNotificationConfigs = list_task_push_notification_configs
    DeleteTaskPushNotificationConfig = delete_task_push_notification_config
    GetExtendedAgentCard = get_extended_agent_card
    SendMessageREST = send_message_rest
    StreamMessageREST = stream_message_rest
    GetTaskREST = get_task_rest
    ListTasksREST = list_tasks_rest
    CancelTaskREST = cancel_task_rest
    ResubscribeTaskREST = resubscribe_task_rest
    SetTaskPushNotificationConfigREST = set_task_push_notification_config_rest
    GetTaskPushNotificationConfigREST = get_task_push_notification_config_rest
    ListTaskPushNotificationConfigsREST = list_task_push_notification_configs_rest
    DeleteTaskPushNotificationConfigREST = delete_task_push_notification_config_rest
    GetExtendedAgentCardREST = get_extended_agent_card_rest


def decode_a2a_send_message_response(raw: Any) -> A2ASendMessageResponse:
    if raw is None:
        return A2ASendMessageResponse()
    if isinstance(raw, dict) and ("task" in raw or "message" in raw):
        return A2ASendMessageResponse.from_dict(raw)
    if isinstance(raw, dict) and "status" in raw and "id" in raw:
        return A2ASendMessageResponse(task=A2ATask.from_dict(raw))
    if isinstance(raw, dict) and ("role" in raw or "parts" in raw):
        return A2ASendMessageResponse(message=A2AMessage.from_dict(raw))
    return A2ASendMessageResponse.from_dict(raw if isinstance(raw, dict) else {})


def a2a_stream_event_from_sse(event: StreamRunEvent) -> A2AStreamEvent:
    payload = json.loads(event.data.decode() or "{}")
    if isinstance(payload, dict) and "result" in payload:
        payload = payload["result"]
    return A2AStreamEvent(
        id=event.id,
        event=event.event,
        raw=event.data,
        result=A2AStreamResponse.from_dict(_normalize_stream_payload(payload)),
    )


def _normalize_stream_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    if "status" in payload and "id" in payload:
        return {"task": payload}
    if "role" in payload or "parts" in payload:
        return {"message": payload}
    if (
        "statusUpdate" in payload
        or "artifactUpdate" in payload
        or "task" in payload
        or "message" in payload
    ):
        return payload
    if "status" in payload:
        return {"statusUpdate": payload}
    if "artifact" in payload:
        return {"artifactUpdate": payload}
    return payload


def _a2a_task_list_query(params: A2ATaskListParams) -> dict[str, str]:
    query: dict[str, str] = {}
    mapping = {
        "contextId": params.context_id,
        "status": params.status,
        "pageSize": params.page_size,
        "pageToken": params.page_token,
        "historyLength": params.history_length,
        "statusTimestampAfter": params.status_timestamp_after,
        "includeArtifacts": None
        if params.include_artifacts is None
        else str(params.include_artifacts).lower(),
    }
    for key, value in mapping.items():
        if value is not None and str(value).strip():
            query[key] = str(value)
    return query


def _a2a_task_id_from_push_params(params: A2ATaskPushConfigParams) -> str:
    value = (params.task_id or "").strip()
    if not value and params.push_notification_config:
        value = (params.push_notification_config.extra.get("taskId") or "").strip()
    if not value:
        raise ValueError("openlinker: A2A task id is required")
    return value


def _a2a_push_config_id(params: A2ATaskPushConfigParams) -> str:
    value = (params.push_notification_config_id or params.id or "").strip()
    if not value and params.push_notification_config:
        value = (params.push_notification_config.id or "").strip()
    if not value:
        raise ValueError("openlinker: A2A push notification config id is required")
    return value


def _parse_a2a_http_error(response: httpx.Response) -> OpenLinkerError:
    raw = response.content[:MAX_RESPONSE_BODY_BYTES]
    message = response.reason_phrase
    code = f"HTTP_{response.status_code}"
    try:
        parsed = response.json()
        if isinstance(parsed, dict):
            err = parsed.get("error")
            if isinstance(err, dict):
                code = str(err.get("code") or code)
                message = str(err.get("message") or message)
    except ValueError:
        pass
    return OpenLinkerError(
        status_code=response.status_code, code=code, message=message, response_body=raw
    )


class A2AGRPCClient:
    def __init__(
        self,
        endpoint: str,
        tenant: str,
        *,
        token: str = "",
        headers: dict[str, str] | None = None,
        sdk_agent: str = DEFAULT_SDK_AGENT,
        channel_factory: Callable[[str], Any] | None = None,
        timeout: float | None = None,
    ) -> None:
        endpoint = endpoint.strip()
        tenant = tenant.strip()
        if not endpoint:
            raise ValueError("openlinker: A2A gRPC endpoint is required")
        if not tenant:
            raise ValueError("openlinker: A2A gRPC tenant is required")
        self.endpoint = endpoint
        self.tenant = tenant
        self.token = token.strip()
        self.headers = dict(headers or {})
        self.sdk_agent = sdk_agent.strip() or DEFAULT_SDK_AGENT
        self.timeout = timeout

        modules = _load_a2a_grpc_modules()
        self._types = modules["types"]
        self._parse_dict = modules["parse_dict"]
        self._message_to_dict = modules["message_to_dict"]
        self._client_call_context = modules["client_call_context"]

        config = modules["client_config"](
            grpc_channel_factory=channel_factory or _default_grpc_channel_factory
        )
        agent_card = self._types.AgentCard(name="OpenLinker A2A gRPC Client", version="1.0")
        self._transport = modules["grpc_transport"].create(agent_card, endpoint, config)

    async def aclose(self) -> None:
        close = getattr(self._transport, "close", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result

    async def send_message_response(
        self, params: A2AMessageSendParams | dict[str, Any]
    ) -> A2ASendMessageResponse:
        resp = await self._transport.send_message(
            self._send_message_request(params), context=self._context()
        )
        return decode_a2a_send_message_response(self._proto_to_dict(resp))

    async def send_message(self, params: A2AMessageSendParams | dict[str, Any]) -> A2ATask:
        resp = await self.send_message_response(params)
        if resp.task is not None:
            return resp.task
        if resp.message is not None:
            raise RuntimeError(
                "openlinker: A2A gRPC SendMessage returned a message; use send_message_response"
            )
        raise RuntimeError("openlinker: A2A gRPC SendMessage returned an empty response")

    async def stream_message(
        self, params: A2AMessageSendParams | dict[str, Any]
    ) -> AsyncIterator[A2AStreamEvent]:
        stream = self._transport.send_message_streaming(
            self._send_message_request(params), context=self._context()
        )
        async for resp in stream:
            yield self._stream_event_from_proto(resp)

    async def get_task(self, params: A2ATaskQueryParams | dict[str, Any]) -> A2ATask:
        params = maybe_model(params, A2ATaskQueryParams)
        request = self._parse(
            {
                "tenant": self.tenant,
                "id": params.id,
                "historyLength": params.history_length,
            },
            self._types.GetTaskRequest(),
        )
        return A2ATask.from_dict(
            self._proto_to_dict(await self._transport.get_task(request, context=self._context()))
        )

    async def list_tasks(self, params: A2ATaskListParams | dict[str, Any]) -> A2ATaskListResponse:
        params = maybe_model(params, A2ATaskListParams)
        request = self._parse(
            {
                "tenant": self.tenant,
                "contextId": params.context_id,
                "status": _task_state_to_proto(params.status),
                "pageSize": params.page_size,
                "pageToken": params.page_token,
                "historyLength": params.history_length,
                "statusTimestampAfter": params.status_timestamp_after,
                "includeArtifacts": params.include_artifacts,
            },
            self._types.ListTasksRequest(),
        )
        return A2ATaskListResponse.from_dict(
            self._proto_to_dict(await self._transport.list_tasks(request, context=self._context()))
        )

    async def cancel_task(self, params: A2ATaskQueryParams | dict[str, Any]) -> A2ATask:
        params = maybe_model(params, A2ATaskQueryParams)
        request = self._parse(
            {"tenant": self.tenant, "id": params.id}, self._types.CancelTaskRequest()
        )
        return A2ATask.from_dict(
            self._proto_to_dict(await self._transport.cancel_task(request, context=self._context()))
        )

    async def resubscribe_task(
        self, params: A2ATaskQueryParams | dict[str, Any]
    ) -> AsyncIterator[A2AStreamEvent]:
        params = maybe_model(params, A2ATaskQueryParams)
        request = self._parse(
            {"tenant": self.tenant, "id": params.id}, self._types.SubscribeToTaskRequest()
        )
        async for resp in self._transport.subscribe(request, context=self._context()):
            yield self._stream_event_from_proto(resp)

    async def set_task_push_notification_config(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ):
        request = self._task_push_config_request(params)
        return A2ATaskPushNotificationConfig.from_dict(
            self._proto_to_dict(
                await self._transport.create_task_push_notification_config(
                    request, context=self._context()
                )
            )
        )

    async def get_task_push_notification_config(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ):
        params = maybe_model(params, A2ATaskPushConfigParams)
        request = self._parse(
            {
                "tenant": self.tenant,
                "taskId": _a2a_task_id_from_push_params(params),
                "id": _a2a_push_config_id(params),
            },
            self._types.GetTaskPushNotificationConfigRequest(),
        )
        return A2ATaskPushNotificationConfig.from_dict(
            self._proto_to_dict(
                await self._transport.get_task_push_notification_config(
                    request, context=self._context()
                )
            )
        )

    async def list_task_push_notification_configs(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ):
        params = maybe_model(params, A2ATaskPushConfigParams)
        request = self._parse(
            {
                "tenant": self.tenant,
                "taskId": _a2a_task_id_from_push_params(params),
                "pageSize": params.page_size,
                "pageToken": params.page_token,
            },
            self._types.ListTaskPushNotificationConfigsRequest(),
        )
        return A2ATaskPushConfigList.from_dict(
            self._proto_to_dict(
                await self._transport.list_task_push_notification_configs(
                    request, context=self._context()
                )
            )
        )

    async def delete_task_push_notification_config(
        self, params: A2ATaskPushConfigParams | dict[str, Any]
    ) -> None:
        params = maybe_model(params, A2ATaskPushConfigParams)
        request = self._parse(
            {
                "tenant": self.tenant,
                "taskId": _a2a_task_id_from_push_params(params),
                "id": _a2a_push_config_id(params),
            },
            self._types.DeleteTaskPushNotificationConfigRequest(),
        )
        await self._transport.delete_task_push_notification_config(request, context=self._context())

    async def get_extended_agent_card(self) -> AgentCardResponse:
        request = self._parse({"tenant": self.tenant}, self._types.GetExtendedAgentCardRequest())
        return AgentCardResponse.from_dict(
            self._proto_to_dict(
                await self._transport.get_extended_agent_card(request, context=self._context())
            )
        )

    def _send_message_request(self, params: A2AMessageSendParams | dict[str, Any]) -> Any:
        params = maybe_model(params, A2AMessageSendParams)
        return self._parse(
            {
                "tenant": self.tenant,
                "message": _normalize_a2a_proto_request(to_json_value(params.message)),
                "configuration": to_json_value(params.configuration),
                "metadata": params.metadata,
            },
            self._types.SendMessageRequest(),
        )

    def _task_push_config_request(self, params: A2ATaskPushConfigParams | dict[str, Any]) -> Any:
        params = maybe_model(params, A2ATaskPushConfigParams)
        push = params.push_notification_config
        body = {
            "tenant": self.tenant,
            "id": params.push_notification_config_id or params.id,
            "taskId": _a2a_task_id_from_push_params(params),
            "url": params.url or (push.url if push else None),
            "token": params.token or (push.token if push else None),
            "authentication": to_json_value(
                params.authentication or (push.authentication if push else None)
            ),
        }
        return self._parse(body, self._types.TaskPushNotificationConfig())

    def _context(self) -> Any:
        metadata = dict(self.headers)
        if self.token:
            metadata["authorization"] = f"Bearer {self.token}"
        if self.sdk_agent:
            metadata["x-openlinker-sdk-agent"] = self.sdk_agent
        return self._client_call_context(timeout=self.timeout, service_parameters=metadata)

    def _parse(self, data: dict[str, Any], message: Any) -> Any:
        return self._parse_dict(
            _drop_none(_normalize_a2a_proto_request(data)),
            message,
            ignore_unknown_fields=True,
        )

    def _proto_to_dict(self, message: Any) -> dict[str, Any]:
        raw = self._message_to_dict(
            message,
            preserving_proto_field_name=False,
            always_print_fields_with_no_presence=False,
        )
        return _normalize_a2a_proto_response(raw)

    def _stream_event_from_proto(self, message: Any) -> A2AStreamEvent:
        return A2AStreamEvent(result=A2AStreamResponse.from_dict(self._proto_to_dict(message)))

    Close = aclose
    SendMessage = send_message
    SendMessageResponse = send_message_response
    StreamMessage = stream_message
    GetTask = get_task
    ListTasks = list_tasks
    CancelTask = cancel_task
    ResubscribeTask = resubscribe_task
    SetTaskPushNotificationConfig = set_task_push_notification_config
    GetTaskPushNotificationConfig = get_task_push_notification_config
    ListTaskPushNotificationConfigs = list_task_push_notification_configs
    DeleteTaskPushNotificationConfig = delete_task_push_notification_config
    GetExtendedAgentCard = get_extended_agent_card


NewA2AClient = A2AClient
NewA2AGRPCClient = A2AGRPCClient
NormalizeA2ADialect = normalize_a2a_dialect


def _load_a2a_grpc_modules() -> dict[str, Any]:
    try:
        from a2a.client import ClientCallContext, ClientConfig
        from a2a.client.transports.grpc import GrpcTransport
        import a2a.types as a2a_types
        from google.protobuf.json_format import MessageToDict, ParseDict
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "openlinker: A2A gRPC support requires optional dependencies. "
            "Install openlinker[grpc] or a2a-sdk[grpc]."
        ) from exc
    return {
        "client_call_context": ClientCallContext,
        "client_config": ClientConfig,
        "grpc_transport": GrpcTransport,
        "types": a2a_types,
        "parse_dict": ParseDict,
        "message_to_dict": MessageToDict,
    }


def _default_grpc_channel_factory(endpoint: str) -> Any:
    import grpc

    parsed = urlparse(endpoint)
    if parsed.scheme and parsed.netloc:
        target = parsed.netloc
        if parsed.scheme.lower() in {"http", "grpc"}:
            return grpc.aio.insecure_channel(target)
        if parsed.scheme.lower() in {"https", "grpcs"}:
            return grpc.aio.secure_channel(target, grpc.ssl_channel_credentials())
    if endpoint.startswith(("localhost:", "127.0.0.1:", "[::1]:")):
        return grpc.aio.insecure_channel(endpoint)
    return grpc.aio.secure_channel(endpoint, grpc.ssl_channel_credentials())


def _drop_none(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _drop_none(v) for k, v in value.items() if v is not None and v != []}
    if isinstance(value, list):
        return [_drop_none(v) for v in value if v is not None]
    return value


def _normalize_a2a_proto_request(value: Any) -> Any:
    if isinstance(value, list):
        return [_normalize_a2a_proto_request(item) for item in value]
    if not isinstance(value, dict):
        return value
    out = {key: _normalize_a2a_proto_request(item) for key, item in value.items()}
    role = out.get("role")
    if isinstance(role, str):
        normalized = role.strip().lower()
        if normalized == "user":
            out["role"] = "ROLE_USER"
        elif normalized in {"agent", "assistant"}:
            out["role"] = "ROLE_AGENT"
    state = out.get("status")
    if isinstance(state, str):
        out["status"] = _task_state_to_proto(state)
    status_obj = out.get("status")
    if isinstance(status_obj, dict) and isinstance(status_obj.get("state"), str):
        status_obj["state"] = _task_state_to_proto(status_obj["state"])
    return out


def _normalize_a2a_proto_response(value: Any) -> Any:
    if isinstance(value, list):
        return [_normalize_a2a_proto_response(item) for item in value]
    if not isinstance(value, dict):
        return value
    out = {key: _normalize_a2a_proto_response(item) for key, item in value.items()}
    role = out.get("role")
    if isinstance(role, str):
        if role == "ROLE_USER":
            out["role"] = "user"
        elif role == "ROLE_AGENT":
            out["role"] = "agent"
    status = out.get("status")
    if isinstance(status, dict) and isinstance(status.get("state"), str):
        status["state"] = _task_state_from_proto(status["state"])
    if isinstance(out.get("state"), str):
        out["state"] = _task_state_from_proto(out["state"])
    return out


def _task_state_to_proto(state: str | None) -> str | None:
    if state is None:
        return None
    normalized = state.strip()
    if not normalized:
        return None
    upper = normalized.upper()
    if upper.startswith("TASK_STATE_"):
        return upper
    return "TASK_STATE_" + upper.replace("-", "_")


def _task_state_from_proto(state: str) -> str:
    normalized = state.strip()
    if normalized.startswith("TASK_STATE_"):
        return normalized[len("TASK_STATE_") :].lower()
    return normalized.lower()
