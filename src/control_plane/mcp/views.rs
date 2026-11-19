//! Customer-facing views of the HTTP API's replies. Each view names the fields it shows, so
//! runtime internals the API also returns (hosts, sessions, epochs, reroutes) drop out on parse.

use std::cmp::Reverse;

use chrono::{DateTime, SecondsFormat};
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};

use super::pages::{self, Cursor, Page};

const MAX_SCHEMA_DEPTH: usize = 16;
const MAX_LOG_MESSAGE_CHARS: usize = 2_000;
const MAX_LISTED_CONNECTIONS: usize = 10;
pub(super) const UNKNOWN_OUTCOME_HINT: &str =
    "The call may have run. Check list_requests or get_actor_state before retrying.";
const UNAVAILABLE: &str = "The actor is temporarily unavailable. Try again shortly.";
const NOT_RUN: &str =
    "The actor could not be reached, so the call did not run. It is safe to retry.";
const INTERNAL: &str = "Something went wrong on our side. Try again shortly.";

/// Platform failures read as the service being unavailable, never as its hosts or storage.
pub(super) fn customer_error(mut body: Value) -> Value {
    let message = match body["error"]["code"].as_str() {
        Some("unavailable") => UNAVAILABLE,
        Some("internal") => INTERNAL,
        Some("outcome_unknown") => UNKNOWN_OUTCOME_HINT,
        _ => return body,
    };
    body["error"]["message"] = message.into();
    body
}

#[derive(Deserialize)]
pub(super) struct ContractReply {
    contract: Contract,
}

#[derive(Deserialize)]
struct Contract {
    actors: Vec<ContractActor>,
    #[serde(default)]
    typescript: Option<TypeScript>,
}

#[derive(Deserialize)]
struct TypeScript {
    declarations: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContractActor {
    actor_name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    sandbox: Option<Value>,
    rpc: Rpc,
    socket: Value,
}

#[derive(Deserialize)]
struct Rpc {
    methods: Vec<RpcMethod>,
    schema: Value,
}

#[derive(Deserialize)]
struct RpcMethod {
    name: String,
    #[serde(default)]
    description: Option<String>,
    parameters: Vec<RpcParameter>,
    result: RpcResult,
}

#[derive(Deserialize)]
struct RpcParameter {
    name: String,
    optional: bool,
    rest: bool,
}

#[derive(Deserialize)]
struct RpcResult {
    kind: String,
}

#[derive(Serialize, JsonSchema)]
pub(super) struct Deployment {
    actors: Vec<ActorContract>,
    /// TypeScript declarations for every method, state and socket message type.
    #[serde(skip_serializing_if = "Option::is_none")]
    types: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct ActorContract {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compute: Option<Value>,
    methods: Vec<MethodSignature>,
    /// JSON Schemas, only for contracts without TypeScript declarations (Python projects).
    #[serde(skip_serializing_if = "Option::is_none")]
    schemas: Option<Value>,
}

#[derive(Serialize, JsonSchema)]
struct MethodSignature {
    name: String,
    parameters: Vec<String>,
    returns: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

impl From<ContractReply> for Deployment {
    fn from(reply: ContractReply) -> Self {
        let typed = reply.contract.typescript.is_some();
        Self {
            actors: reply
                .contract
                .actors
                .into_iter()
                .map(|actor| actor.view(typed))
                .collect(),
            types: reply.contract.typescript.map(|types| types.declarations),
        }
    }
}

impl ContractActor {
    fn view(self, typed: bool) -> ActorContract {
        let schemas = (!typed).then(|| json!({ "rpc": self.rpc.schema, "socket": self.socket }));
        ActorContract {
            name: self.actor_name,
            description: self.description,
            compute: self.sandbox,
            methods: self.rpc.methods.into_iter().map(RpcMethod::view).collect(),
            schemas,
        }
    }
}

impl RpcMethod {
    fn view(self) -> MethodSignature {
        MethodSignature {
            name: self.name,
            parameters: self.parameters.iter().map(RpcParameter::label).collect(),
            returns: self.result.kind,
            description: self.description,
        }
    }
}

impl RpcParameter {
    fn label(&self) -> String {
        match (self.rest, self.optional) {
            (true, _) => format!("...{}", self.name),
            (false, true) => format!("{}?", self.name),
            (false, false) => self.name.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Inventory {
    actors: Vec<ActorInventory>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActorInventory {
    actor_name: String,
    #[serde(default)]
    methods: Vec<String>,
    live: u64,
    dormant: u64,
    unknown: u64,
    instances: Vec<Instance>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Instance {
    actor_id: String,
    status: String,
    #[serde(default)]
    last_active_at_ms: Option<u64>,
    connections: Vec<Connection>,
    #[serde(default)]
    waiting: Option<Vec<Value>>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
struct Connection {
    id: String,
    metadata: Value,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct Actors {
    actors: Vec<ActorSummary>,
    instances: Vec<InstanceView>,
    total_instances: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ActorSummary {
    actor_name: String,
    methods: Vec<String>,
    live: u64,
    dormant: u64,
    unknown: u64,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct InstanceView {
    actor_name: String,
    actor_id: String,
    status: String,
    #[schemars(with = "Option<String>")]
    #[serde(
        serialize_with = "optional_timestamp",
        skip_serializing_if = "Option::is_none"
    )]
    last_active_at: Option<u64>,
    connection_count: usize,
    /// The first few open connections; `connectionCount` has them all.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    connections: Vec<Connection>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    waiting: Vec<Value>,
}

pub(super) struct InstanceFilter<'a> {
    pub actor_name: Option<&'a str>,
    pub status: Option<&'a str>,
}

impl Inventory {
    /// Every actor class, then one page of instances: live first, most recently active next.
    pub(super) fn view(self, filter: InstanceFilter, cursor: Cursor, limit: usize) -> Actors {
        let actors = self.actors.iter().map(ActorInventory::summary).collect();
        let mut instances: Vec<InstanceView> = self
            .actors
            .into_iter()
            .flat_map(ActorInventory::instance_views)
            .filter(|instance| instance.matches(&filter))
            .collect();
        instances
            .sort_by_key(|instance| (instance.status != "live", Reverse(instance.last_active_at)));
        let page = pages::slice(instances, cursor, limit);
        Actors {
            actors,
            total_instances: page.total.unwrap_or_default(),
            instances: page.records,
            next_cursor: page.next_cursor,
        }
    }
}

impl ActorInventory {
    fn summary(&self) -> ActorSummary {
        ActorSummary {
            actor_name: self.actor_name.clone(),
            methods: self.methods.clone(),
            live: self.live,
            dormant: self.dormant,
            unknown: self.unknown,
        }
    }

    fn instance_views(self) -> impl Iterator<Item = InstanceView> {
        let actor_name = self.actor_name;
        self.instances
            .into_iter()
            .map(move |instance| instance.view(actor_name.clone()))
    }
}

impl Instance {
    fn view(self, actor_name: String) -> InstanceView {
        let connection_count = self.connections.len();
        let mut connections = self.connections;
        connections.truncate(MAX_LISTED_CONNECTIONS);
        InstanceView {
            actor_name,
            actor_id: self.actor_id,
            status: self.status,
            last_active_at: self.last_active_at_ms,
            connection_count,
            connections,
            waiting: self.waiting.unwrap_or_default(),
        }
    }
}

impl InstanceView {
    fn matches(&self, filter: &InstanceFilter) -> bool {
        filter.actor_name.is_none_or(|name| name == self.actor_name)
            && filter.status.is_none_or(|status| status == self.status)
    }
}

#[derive(Deserialize)]
pub(super) struct StateReply {
    snapshot: Option<Snapshot>,
    #[serde(default)]
    schema: Option<Value>,
}

#[derive(Deserialize)]
struct Snapshot {
    #[serde(flatten)]
    record: SnapshotRecord,
    state: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotRecord {
    state_version: u64,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    attribution: Attribution,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Attribution {
    committed_at_ms: Option<u64>,
    connection_id: Option<String>,
    operation: Option<String>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct Version {
    version: u64,
    #[schemars(with = "Option<String>")]
    #[serde(
        serialize_with = "optional_timestamp",
        skip_serializing_if = "Option::is_none"
    )]
    saved_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connection_id: Option<String>,
}

// One struct, not an untagged enum: MCP clients reject output schemas whose root is not an object.
#[derive(Serialize, JsonSchema)]
pub(super) struct StateView {
    #[serde(flatten)]
    version: Option<Version>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    state: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

pub(super) struct StateQuery {
    pub path: Option<String>,
    pub include_schema: bool,
}

impl StateReply {
    pub(super) fn view(self, query: StateQuery) -> StateView {
        let Some(snapshot) = self.snapshot else {
            return StateView {
                version: None,
                path: None,
                state: Value::Null,
                schema: None,
                message: Some("This instance has no saved state yet.".into()),
            };
        };
        let state = match &query.path {
            Some(path) => snapshot.state.pointer(path).cloned().unwrap_or(Value::Null),
            None => snapshot.state,
        };
        let schema = query
            .include_schema
            .then(|| self.schema.as_ref().and_then(state_schema))
            .flatten();
        StateView {
            version: Some(snapshot.record.into()),
            path: query.path,
            state,
            schema,
            message: None,
        }
    }
}

impl From<SnapshotRecord> for Version {
    fn from(record: SnapshotRecord) -> Self {
        Self {
            version: record.state_version,
            saved_at: record.attribution.committed_at_ms,
            operation: record.attribution.operation,
            request_id: record.request_id,
            connection_id: record.attribution.connection_id,
        }
    }
}

fn state_schema(schema: &Value) -> Option<Value> {
    let definitions = schema.get("definitions")?;
    Some(resolve_refs(definitions.get("State")?, definitions, 0))
}

fn resolve_refs(schema: &Value, definitions: &Value, depth: usize) -> Value {
    match schema {
        Value::Object(fields) => {
            let target = fields
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix("#/definitions/"))
                .and_then(|name| definitions.get(name));
            match target {
                Some(target) if depth < MAX_SCHEMA_DEPTH => {
                    resolve_refs(target, definitions, depth + 1)
                }
                Some(_) => schema.clone(),
                None => fields
                    .iter()
                    .map(|(key, value)| (key.clone(), resolve_refs(value, definitions, depth)))
                    .collect(),
            }
        }
        Value::Array(items) => items
            .iter()
            .map(|item| resolve_refs(item, definitions, depth))
            .collect(),
        other => other.clone(),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StateHistoryReply {
    records: Vec<SnapshotRecord>,
    next_before: Option<u64>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct StateHistory {
    versions: Vec<Version>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_before: Option<u64>,
}

impl From<StateHistoryReply> for StateHistory {
    fn from(reply: StateHistoryReply) -> Self {
        Self {
            versions: reply.records.into_iter().map(Version::from).collect(),
            next_before: reply.next_before,
        }
    }
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct Request {
    request_id: String,
    actor_name: String,
    actor_id: String,
    kind: String,
    operation: String,
    outcome: String,
    #[schemars(with = "String")]
    #[serde(rename(deserialize = "startedAtMs"), serialize_with = "timestamp")]
    started_at: u64,
    #[serde(serialize_with = "millis")]
    duration_ms: f64,
    #[serde(
        default,
        serialize_with = "optional_millis",
        skip_serializing_if = "Option::is_none"
    )]
    queue_wait_ms: Option<f64>,
    /// The actor was asleep and started for this request.
    #[serde(rename(deserialize = "hostState"), default, deserialize_with = "cold")]
    cold_start: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    connection_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    state_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    metadata: Value,
}

/// A rerouted attempt is the platform moving a request before it ran; the attempt that ran follows.
pub(super) fn requests(mut page: Page<Request>) -> Page<Request> {
    page.records.retain(|request| request.outcome != "rerouted");
    page
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct Log {
    #[schemars(with = "String")]
    #[serde(rename(deserialize = "timestampMs"), serialize_with = "timestamp")]
    at: u64,
    level: String,
    actor_name: String,
    actor_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    #[serde(deserialize_with = "capped_message")]
    message: String,
}

/// An actor instance's resource usage. Each sample is the highest in its period.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct Resources {
    /// The requests and limits in force over the window, from when each sandbox was claimed. An
    /// absent limit leaves that resource uncapped.
    sandboxes: Vec<SandboxResources>,
    /// The latest SQLite database size, however long ago it was reported.
    database_bytes: Option<u64>,
    usage: Vec<UsageSample>,
    database: Vec<DatabaseSample>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SandboxResources {
    #[schemars(with = "String")]
    #[serde(rename(deserialize = "claimedAtMs"), serialize_with = "timestamp")]
    since: u64,
    cpu_millis: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cpu_limit_millis: Option<u32>,
    memory_mib: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory_limit_mib: Option<u32>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct UsageSample {
    #[schemars(with = "String")]
    #[serde(rename(deserialize = "atMs"), serialize_with = "timestamp")]
    at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cpu_cores: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory_bytes: Option<u64>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DatabaseSample {
    #[schemars(with = "String")]
    #[serde(rename(deserialize = "atMs"), serialize_with = "timestamp")]
    at: u64,
    bytes: u64,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct Session {
    connection_id: String,
    actor_name: String,
    actor_id: String,
    status: SessionStatus,
    #[schemars(with = "String")]
    #[serde(rename(deserialize = "openedAtMs"), serialize_with = "timestamp")]
    opened_at: u64,
    /// When it closed, or for a lost session when its gateway last reported.
    #[schemars(with = "Option<String>")]
    #[serde(
        rename(deserialize = "closedAtMs"),
        default,
        serialize_with = "optional_timestamp",
        skip_serializing_if = "Option::is_none"
    )]
    closed_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    close_code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    close_reason: Option<String>,
    /// Text messages each way, known once the connection closes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    messages_in: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    messages_out: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<Value>,
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum SessionStatus {
    Open,
    Closed,
    /// The gateway stopped reporting before the connection closed.
    Lost,
}

#[derive(Deserialize)]
pub(super) struct Metrics {
    classes: Vec<Stats>,
    total: Stats,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct Stats {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    actor_name: String,
    #[serde(rename(deserialize = "count"))]
    requests: u64,
    #[serde(rename(deserialize = "success"), serialize_with = "optional_millis")]
    success_percent: Option<f64>,
    #[serde(rename(deserialize = "p95"), serialize_with = "optional_millis")]
    p95_ms: Option<f64>,
    #[serde(rename(deserialize = "queueP95"), serialize_with = "optional_millis")]
    queue_p95_ms: Option<f64>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct QueueWait {
    actor_name: String,
    actor_id: String,
    #[serde(rename(deserialize = "admitted"))]
    requests: u64,
    #[serde(serialize_with = "millis")]
    average_ms: f64,
    #[serde(serialize_with = "millis")]
    max_ms: f64,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct Health {
    total: Stats,
    by_actor: Vec<Stats>,
    /// The instances with the longest queue waits, longest first.
    longest_queue_waits: Vec<QueueWait>,
}

impl Health {
    pub(super) fn new(metrics: Metrics, mut queue_waits: Vec<QueueWait>, limit: usize) -> Self {
        queue_waits.sort_by(|a, b| b.max_ms.total_cmp(&a.max_ms));
        queue_waits.truncate(limit);
        Self {
            total: metrics.total,
            by_actor: metrics.classes,
            longest_queue_waits: queue_waits,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum Outcome {
    Completed {
        result: Value,
        #[serde(default)]
        metadata: Timing,
    },
    Failed {
        code: String,
        message: String,
        #[serde(default)]
        metadata: Timing,
    },
    NotExecuted {},
    Unauthenticated,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Timing {
    duration_ms: Option<f64>,
    #[serde(default, deserialize_with = "cold")]
    host_state: bool,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct Invocation {
    status: InvocationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Value>,
    #[serde(
        serialize_with = "optional_millis",
        skip_serializing_if = "Option::is_none"
    )]
    duration_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cold_start: Option<bool>,
}

#[derive(Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum InvocationStatus {
    Completed,
    Failed,
    /// The call never ran, so it is safe to retry.
    NotRun,
}

impl Outcome {
    pub(super) fn completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }
}

impl From<Outcome> for Invocation {
    fn from(outcome: Outcome) -> Self {
        let (status, result, error, timing) = match outcome {
            Outcome::Completed { result, metadata } => {
                (InvocationStatus::Completed, Some(result), None, metadata)
            }
            Outcome::Failed {
                code,
                message,
                metadata,
            } => (
                InvocationStatus::Failed,
                None,
                Some(failure(code, message)),
                metadata,
            ),
            Outcome::NotExecuted { .. } | Outcome::Unauthenticated => (
                InvocationStatus::NotRun,
                None,
                Some(json!({ "code": "unavailable", "message": NOT_RUN })),
                Timing::default(),
            ),
        };
        Self {
            status,
            result,
            error,
            duration_ms: timing.duration_ms,
            cold_start: timing.duration_ms.map(|_| timing.host_state),
        }
    }
}

/// `actor_error` carries the actor's own message; platform failures get a customer-facing one.
fn failure(code: String, message: String) -> Value {
    customer_error(json!({ "error": { "code": code, "message": message } }))["error"].take()
}

fn capped_message<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let message = String::deserialize(deserializer)?;
    let chars = message.chars().count();
    if chars <= MAX_LOG_MESSAGE_CHARS {
        return Ok(message);
    }
    let kept: String = message.chars().take(MAX_LOG_MESSAGE_CHARS).collect();
    Ok(format!(
        "{kept}… [{} more characters]",
        chars - MAX_LOG_MESSAGE_CHARS
    ))
}

fn cold<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    Ok(Option::<String>::deserialize(deserializer)?.as_deref() == Some("cold"))
}

fn timestamp<S: Serializer>(ms: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    let at = i64::try_from(*ms)
        .ok()
        .and_then(DateTime::from_timestamp_millis)
        .map(|at| at.to_rfc3339_opts(SecondsFormat::Millis, true));
    match at {
        Some(at) => serializer.serialize_str(&at),
        None => serializer.serialize_u64(*ms),
    }
}

fn optional_timestamp<S: Serializer>(ms: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error> {
    match ms {
        Some(ms) => timestamp(ms, serializer),
        None => serializer.serialize_none(),
    }
}

fn millis<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_f64((value * 10.0).round() / 10.0)
}

fn optional_millis<S: Serializer>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => millis(value, serializer),
        None => serializer.serialize_none(),
    }
}
