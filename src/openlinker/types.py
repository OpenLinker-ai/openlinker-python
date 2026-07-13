from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from .model import Model, jfield

JSON = dict[str, Any]


@dataclass
class ListAgentsParams(Model):
    query: str = jfield("q", "")
    tags: list[str] = jfield(default_factory=list)
    page: int = 0
    size: int = 0
    callable_only: bool = False


@dataclass
class CreatorMini(Model):
    display_name: str = ""


@dataclass
class Availability(Model):
    status: str = ""
    label: str = ""
    hint: str = ""
    last_successful_run_at: str | None = None
    last_failed_run_at: str | None = None
    last_checked_at: str | None = None
    consecutive_failures: int = 0


@dataclass
class Readiness(Model):
    listed: bool = False
    discoverable: bool = False
    callable: bool = False
    verified: bool = False
    certified: bool = False
    paid_enabled: bool = False
    agent_card_url: str = ""
    a2a_endpoint: str = ""
    last_successful_run_at: str | None = None
    availability_status: str = ""
    verified_skill_count: int = 0
    latest_benchmark_batch_id: str | None = None
    explanation: dict[str, str] = jfield(default_factory=dict)


@dataclass
class MarketListItem(Model):
    id: str = ""
    slug: str = ""
    name: str = ""
    description: str = ""
    price_per_call_cents: int = 0
    tags: list[str] = jfield(default_factory=list)
    total_calls: int = 0
    creator: CreatorMini | None = None
    connection_mode: str = ""
    mcp_tool_name: str | None = None
    availability: Availability | None = None
    readiness: Readiness | None = None


@dataclass
class MarketListResponse(Model):
    items: list[MarketListItem] = jfield(default_factory=list)
    total: int = 0
    page: int = 0
    size: int = 0


@dataclass
class SkillMini(Model):
    id: str = ""
    category: str = ""
    name: str = ""
    description: str = ""


@dataclass
class AgentDetailResponse(MarketListItem):
    endpoint_url: str = ""
    created_at: str = ""
    certified_at: str | None = None
    lifecycle_status: str = ""
    visibility: str = ""
    certification_status: str = ""
    verified_skill_count: int = 0
    latest_benchmark_id: str | None = jfield("latest_benchmark_batch_id", None)
    skills: list[SkillMini] = jfield(default_factory=list)
    capability: JSON | None = None
    examples: list[JSON] = jfield(default_factory=list)


@dataclass
class AgentCardResponse(Model):
    name: str = ""
    description: str = ""
    url: str = ""
    version: str = ""
    protocol_version: str | None = jfield("protocolVersion", None)
    protocol_versions: list[str] = jfield("protocolVersions", default_factory=list)
    preferred_transport: str | None = jfield("preferredTransport", None)
    additional_interfaces: list[JSON] = jfield("additionalInterfaces", default_factory=list)
    supported_interfaces: list[JSON] = jfield("supportedInterfaces", default_factory=list)
    supports_authenticated_extended_card: bool = jfield("supportsAuthenticatedExtendedCard", False)
    provider: JSON | None = None
    capabilities: JSON | None = None
    default_input_modes: list[str] = jfield("default_input_modes", default_factory=list)
    default_output_modes: list[str] = jfield("default_output_modes", default_factory=list)
    default_input_modes_current: list[str] = jfield("defaultInputModes", default_factory=list)
    default_output_modes_current: list[str] = jfield("defaultOutputModes", default_factory=list)
    skills: list[JSON] = jfield(default_factory=list)
    security_schemes: JSON | None = jfield("securitySchemes", None)
    security: list[dict[str, list[str]]] = jfield(default_factory=list)
    security_requirements: list[dict[str, list[str]]] = jfield("securityRequirements", default_factory=list)
    authentication: JSON | None = None
    openlinker: JSON | None = None
    capability: JSON | None = None
    examples: list[JSON] = jfield(default_factory=list)
    signature: JSON | None = None


@dataclass
class RunA2AContext(Model):
    protocol_context_id: str | None = None
    protocol_task_id: str | None = None
    root_context_id: str | None = None
    parent_context_id: str | None = None
    parent_task_id: str | None = None
    parent_run_id: str | None = None
    caller_agent_id: str | None = None
    target_agent_id: str | None = None
    trace_id: str | None = None
    reference_task_ids: list[str] = jfield(default_factory=list)
    source: str | None = None


@dataclass
class TaskCallbackAuthentication(Model):
    scheme: str | None = None
    credentials: str | None = None


@dataclass
class TaskCallbackConfig(Model):
    url: str | None = None
    token: str | None = None
    secret: str | None = None
    authentication: TaskCallbackAuthentication | None = None
    metadata: Any = None
    event_types: list[str] = jfield(default_factory=list)


@dataclass
class RunAgentRequest(Model):
    agent_id: str = ""
    input: Any = None
    metadata: Any = None
    a2a_context: RunA2AContext | None = None
    task_callback: TaskCallbackConfig | None = None
    push_notification: TaskCallbackConfig | None = None
    push_notification_config: TaskCallbackConfig | None = jfield("pushNotificationConfig", None)


@dataclass
class TaskCallbackSubscription(Model):
    id: str = ""
    run_id: str = ""
    target_url: str = ""
    event_types: list[str] = jfield(default_factory=list)
    auth_scheme: str | None = None
    status: str = ""
    consecutive_failures: int = 0
    secret: str | None = None
    created_at: str = ""
    updated_at: str = ""


@dataclass
class RunResponse(Model):
    run_id: str = ""
    status: str = ""
    output: Any = None
    error_code: str | None = None
    error_message: str | None = None
    cost_cents: int = 0
    duration_ms: int = 0
    source: str | None = None
    parent_run_id: str | None = None
    caller_agent_id: str | None = None
    billing_mode: str | None = None
    a2a_context: RunA2AContext | None = None
    task_callback: TaskCallbackSubscription | None = None
    requirement_evidence: Any = None
    evidence_summary: Any = None
    next_action: Any = None


@dataclass
class ListRunEventsParams(Model):
    after_sequence: int = 0
    limit: int = 0


@dataclass
class RunEventResponse(Model):
    event_id: str = ""
    run_id: str = ""
    parent_run_id: str | None = None
    sequence: int = 0
    event_type: str = ""
    payload: Any = None
    created_at: str = ""


@dataclass
class ListRunEventsResponse(Model):
    events: list[RunEventResponse] = jfield(default_factory=list)


@dataclass
class RunChildResponse(Model):
    child_run_id: str = ""
    status: str = ""


@dataclass
class ListRunChildrenResponse(Model):
    items: list[RunChildResponse] = jfield(default_factory=list)


@dataclass
class RunArtifactResponse(Model):
    id: str = ""
    run_id: str = ""
    artifact_type: str = ""
    title: str = ""
    content: Any = None
    visibility: str = ""
    source_artifact_id: str | None = None
    mime_type: str | None = None
    file_uri: str | None = None
    file_name: str | None = None
    file_sha256: str | None = None
    file_size_bytes: int | None = None
    created_at: str = ""


@dataclass
class RunMessageResponse(Model):
    id: str = ""
    run_id: str = ""
    event_sequence: int | None = None
    role: str = ""
    content: str = ""
    payload: Any = None
    created_at: str = ""


@dataclass
class AgentA2AContext(Model):
    current_run_id: str = ""
    parent_run_id: str | None = None
    caller_agent_id: str | None = None
    protocol_context_id: str | None = None
    protocol_task_id: str | None = None
    root_context_id: str | None = None
    parent_context_id: str | None = None
    parent_task_id: str | None = None
    trace_id: str | None = None
    reference_task_ids: list[str] = jfield(default_factory=list)
    call_agent_endpoint: str = ""
    call_agent_method: str = ""
    agent_token_type: str = ""
    agent_scopes: list[str] = jfield(default_factory=list)


@dataclass
class ConversationMessage(Model):
    run_id: str = ""
    event_sequence: int | None = None
    role: str = ""
    content: str = ""
    payload: dict[str, Any] | None = None
    created_at: str | None = None


@dataclass
class ConversationContext(Model):
    id: str = ""
    session_key: str = ""
    protocol_context_id: str | None = None
    root_context_id: str | None = None
    current_run_id: str = ""
    current_protocol_task: str | None = jfield("current_protocol_task_id", None)
    history_before_current: list[ConversationMessage] = jfield(default_factory=list)
    truncated: bool = False
    source: str = ""


@dataclass
class AgentHeartbeatResponse(Model):
    agent_id: str = ""
    availability_status: str = ""
    last_checked_at: str | None = None
    consecutive_failures: int = 0
    pending_run_count: int = 0
    claim_now: bool = False
    next_claim_after_seconds: int = 0
    recommended_heartbeat_after_seconds: int = 0
    max_claim_wait_seconds: int = 0


@dataclass
class ClaimRuntimeRunParams(Model):
    wait_seconds: int = jfield("wait", 0)


@dataclass
class RuntimePullRunResponse(Model):
    run_id: str = ""
    agent_id: str = ""
    input: Any = None
    metadata: Any = None
    source: str = ""
    result_endpoint: str = ""
    result_method: str = ""
    result_required: bool = False
    a2a: AgentA2AContext | None = None
    conversation: ConversationContext | None = None


@dataclass
class AgentEvent(Model):
    event_type: str = ""
    payload: Any = None


@dataclass
class AgentError(Model):
    code: str = ""
    message: str = ""


@dataclass
class RuntimePullResultRequest(Model):
    status: str = ""
    output: Any = None
    events: list[AgentEvent] = jfield(default_factory=list)
    error: AgentError | None = None
    duration_ms: int = 0


@dataclass
class CallAgentRequest(Model):
    parent_run_id: str | None = None
    current_run_id: str | None = None
    target_agent_id: str = ""
    reason: str | None = None
    input: Any = None
    metadata: Any = None
    context_id: str | None = None
    trace_id: str | None = None
    reference_task_ids: list[str] = jfield(default_factory=list)
    task_callback: TaskCallbackConfig | None = None
    push_notification: TaskCallbackConfig | None = None
    push_notification_alias: TaskCallbackConfig | None = jfield("pushNotification", None)
    push_notification_config: TaskCallbackConfig | None = jfield("pushNotificationConfig", None)


@dataclass
class RuntimeAssignment(Model):
    type: str | None = None
    run_id: str = ""
    agent_id: str | None = None
    input: Any = None
    metadata: Any = None
    source: str | None = None
    result_endpoint: str | None = None
    result_method: str | None = None
    result_required: bool = False
    a2a: AgentA2AContext | None = None
    conversation: ConversationContext | None = None


@dataclass
class RuntimeWSClientMessage(Model):
    type: str = ""
    id: str | None = None
    run_id: str | None = None
    event_type: str | None = None
    payload: Any = None
    status: str | None = None
    output: Any = None
    events: list[AgentEvent] = jfield(default_factory=list)
    error: AgentError | None = None
    duration_ms: int = 0


@dataclass
class RuntimeWSServerMessage(Model):
    type: str = ""
    id: str | None = None
    run_id: str | None = None
    agent_id: str | None = None
    input: Any = None
    metadata: Any = None
    source: str | None = None
    result_endpoint: str | None = None
    result_method: str | None = None
    result_required: bool = False
    a2a: AgentA2AContext | None = None
    conversation: ConversationContext | None = None
    status: str | None = None
    result: RunResponse | None = None
    event: RunEventResponse | None = None
    heartbeat: AgentHeartbeatResponse | None = None
    error: AgentError | None = None
    retry_after_seconds: int = 0


@dataclass
class StreamRunEventsOptions(Model):
    after_sequence: int = 0


@dataclass
class StreamRunEvent:
    id: str = ""
    event: str = "message"
    data: bytes = b""


@dataclass
class PlatformCallbackOptions:
    event_types: list[str] | None = None
    after_sequence: int = 0
    on_event: Callable[[StreamRunEvent], Any] | None = None
    on_terminal: Callable[[StreamRunEvent], Any] | None = None
    on_close: Callable[[], Any] | None = None
    on_error: Callable[[Exception], Any] | None = None


@dataclass
class CreateAgentRequest(Model):
    slug: str = ""
    name: str = ""
    description: str | None = None
    endpoint_url: str | None = None
    endpoint_auth_header: str | None = None
    price_per_call_cents: int = 0
    tags: list[str] = jfield(default_factory=list)
    skill_ids: list[str] = jfield(default_factory=list)
    visibility: str | None = None
    connection_mode: str | None = None
    mcp_tool_name: str | None = None


@dataclass
class UpdateAgentRequest(CreateAgentRequest):
    clear_endpoint_auth: bool = jfield("clear_endpoint_auth_header", False)


@dataclass
class ListMyAgentsParams(Model):
    query: str = jfield("q", "")
    status: str = ""
    visibility: str = ""
    certification_status: str = ""
    skill_ids: list[str] = jfield(default_factory=list)
    sort_by: str = ""
    limit: int = 0
    offset: int = 0


@dataclass
class Creator(Model):
    id: str = ""
    email: str = ""
    display_name: str = ""


@dataclass
class AgentCounts(Model):
    total: int = 0
    online: int = 0
    public: int = 0
    unlisted: int = 0
    private: int = 0
    pending: int = 0


@dataclass
class AgentResponse(Model):
    id: str = ""
    slug: str = ""
    name: str = ""
    description: str = ""
    endpoint_url: str = ""
    price_per_call_cents: int = 0
    tags: list[str] = jfield(default_factory=list)
    skill_ids: list[str] = jfield(default_factory=list)
    status: str = ""
    lifecycle_status: str = ""
    visibility: str = ""
    certification_status: str = ""
    rejection_reason: str | None = None
    total_calls: int = 0
    total_revenue_cents: int = 0
    calls_this_month: int = 0
    revenue_this_month: int = jfield("revenue_this_month_cents", 0)
    connection_mode: str = ""
    mcp_tool_name: str | None = None
    availability: Availability | None = None
    readiness: Readiness | None = None
    created_at: str = ""
    certified_at: str | None = None
    creator: Creator | None = None


@dataclass
class AgentListResponse(Model):
    items: list[AgentResponse] = jfield(default_factory=list)
    total: int = 0
    limit: int = 0
    offset: int = 0
    counts: AgentCounts | None = None


@dataclass
class CreateAgentTokenRequest(Model):
    name: str = ""
    agent_id: str | None = None
    scopes: list[str] = jfield(default_factory=list)
    expires_in_minutes: int = 0


@dataclass
class ListAgentTokensParams(Model):
    agent_id: str = ""
    limit: int = 0
    offset: int = 0
    sort_by: str = ""
    sort_dir: str = ""


@dataclass
class AgentTokenResponse(Model):
    id: str = ""
    agent_id: str | None = None
    name: str = ""
    prefix: str = ""
    status: str = ""
    scopes: list[str] = jfield(default_factory=list)
    expires_at: str | None = None
    redeemed_at: str | None = None
    revoked_at: str | None = None
    last_used_at: str | None = None
    created_at: str = ""
    plaintext_token: str | None = None


@dataclass
class AgentTokenListResponse(Model):
    items: list[AgentTokenResponse] = jfield(default_factory=list)
    total: int = 0
    limit: int = 0
    offset: int = 0
    sort_by: str = ""
    sort_dir: str = ""
    has_more: bool = False


@dataclass
class RegisterAgentViaTokenRequest(Model):
    agent_token: str | None = None
    slug: str | None = None
    name: str = ""
    description: str | None = None
    endpoint_url: str | None = None
    endpoint_auth_header: str | None = None
    price_per_call_cents: int = 0
    tags: list[str] = jfield(default_factory=list)
    ability_tags: list[str] = jfield(default_factory=list)
    skill_ids: list[str] = jfield(default_factory=list)
    visibility: str | None = None
    connection_mode: str | None = None
    mcp_tool_name: str | None = None


@dataclass
class RegisterAgentViaTokenResponse(Model):
    agent: AgentResponse | None = None
    agent_token: AgentTokenResponse | None = None


REGISTER_POLICY_REUSE_EXISTING = "reuse_existing"
REGISTER_POLICY_ROTATE_TOKEN = "rotate_token"
REGISTER_POLICY_FORCE_NEW = "force_new"
REGISTER_POLICY_VALIDATE_ONLY = "validate_only"


@dataclass
class RuntimeAgentRegistration(Model):
    agent_id: str | None = None
    agent_slug: str | None = None
    agent_name: str | None = None
    runtime_token: str | None = None
    runtime_token_id: str | None = None
    runtime_prefix: str | None = None
    api_base: str | None = None
    connector: str | None = None
    registered_at: datetime | str | None = None
    updated_at: datetime | str | None = None

    def __str__(self) -> str:
        if self.agent_slug:
            return f"{self.agent_slug} ({self.agent_id or ''})".strip()
        return (self.agent_id or "").strip()


@dataclass
class EnsureRuntimeAgentRequest:
    slug: str = ""
    name: str = ""
    description: str = ""
    endpoint_url: str = ""
    endpoint_auth_header: str = ""
    price_per_call_cents: int = 0
    tags: list[str] | None = None
    skill_ids: list[str] | None = None
    visibility: str = ""
    connection_mode: str = ""
    mcp_tool_name: str = ""
    token_name: str = ""
    token_scopes: list[str] | None = None
    token_expires_in_minutes: int = 0
    policy: str = ""
    user_token: str = ""
    runtime_token: str = ""
    api_base: str = ""
    connector: str = ""
    store: Any = None

