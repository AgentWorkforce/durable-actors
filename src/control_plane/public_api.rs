use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::actor::ActorKey;

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminService, HostLaunchSpec},
    contracts::PublicActorContract,
    service::{ControlPlaneService, TargetResolutionTimings},
};

#[derive(Clone)]
pub(super) struct PublicApiState {
    pub invocations: ControlPlaneService,
    pub admin: AdminService,
    pub hosts: reqwest::Client,
}

pub(super) fn router(invocations: ControlPlaneService, admin: AdminService) -> Router {
    let contracts = super::contract_api::router(admin.clone());
    let gateway = invocations
        .gateway
        .clone()
        .map(|gateway| gateway.router(invocations.clone()))
        .unwrap_or_default();
    let hosts = invocations.host_client();
    Router::new()
        .route("/openapi.yaml", get(openapi))
        .route("/healthz", get(health))
        .route(
            "/v1/organizations/{organization_id}/access",
            put(set_organization_access),
        )
        .route("/v1/projects/{project_id}/sessions", post(issue_session))
        .route(
            "/v1/projects/{project_id}/deployment",
            put(register_deployment)
                .get(get_deployment)
                .delete(delete_deployment),
        )
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/invoke",
            post(super::invocation::invoke),
        )
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/socket-effects",
            post(super::invocation::publish),
        )
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/find-actor",
            post(find_actor),
        )
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/find-websocket",
            post(find_websocket),
        )
        .layer(DefaultBodyLimit::max(MAX_CONTROL_PLANE_MESSAGE_BYTES))
        .with_state(PublicApiState {
            invocations,
            admin,
            hosts,
        })
        .merge(contracts)
        .merge(gateway)
}

async fn health(State(state): State<PublicApiState>) -> (StatusCode, &'static str) {
    if state
        .invocations
        .gateway
        .as_ref()
        .is_some_and(|gateway| gateway.connections.stop.is_cancelled())
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "socket gateway stopping");
    }
    (StatusCode::OK, "ok")
}

async fn openapi() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/yaml")],
        include_str!("../../docs/reference/openapi.yaml"),
    )
}

async fn set_organization_access(
    State(state): State<PublicApiState>,
    Path(organization): Path<String>,
    headers: HeaderMap,
    request: Result<Json<AccessRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<AccessRequest>), ApiError> {
    authorized_admin(&state.admin, &headers)?;
    super::admin::validate_component("organization ID", &organization, 64)
        .map_err(ApiError::bad_request)?;
    let Json(request) = request.map_err(ApiError::json)?;
    state
        .admin
        .set_organization_access(&organization, request.source, request.enabled)
        .await
        .map_err(ApiError::internal)?;
    info!(
        event = "access_updated",
        organization_id = organization,
        source = request.source.as_str(),
        enabled = request.enabled,
        "access setting updated"
    );
    Ok((StatusCode::ACCEPTED, Json(request)))
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AccessRequest {
    enabled: bool,
    #[serde(default)]
    source: super::access::AccessSource,
}

async fn issue_session(
    State(state): State<PublicApiState>,
    path: Path<ProjectPath>,
    headers: HeaderMap,
    request: Result<Json<IssueSessionRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    state
        .admin
        .authorize_session_issuance(
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or(""),
        )
        .map_err(|_| ApiError::unauthorized("administrative credential is required"))?;
    let project = project_id(path)?;
    let Json(request) = request.map_err(ApiError::json)?;
    let issued = state
        .admin
        .issue_session(project.clone(), request.subject, request.expires_at_ms)
        .map_err(ApiError::bad_request)?;
    if state
        .admin
        .current_deployment(&project)
        .await
        .map_err(ApiError::internal)?
        .is_none()
    {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "project deployment not found",
        ));
    }
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({"token":issued.token, "expiresAtMs":issued.expires_at_ms})),
    )
        .into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IssueSessionRequest {
    subject: String,
    expires_at_ms: i64,
}

async fn find_websocket(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    request: Result<Json<FindWebSocketRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Json(request) = request.map_err(ApiError::json)?;
    state
        .invocations
        .validate_home_region(request.home_region.as_deref())
        .map_err(ApiError::assignment)?;
    let grant = super::socket_ticket::SocketGrant {
        actor: path.into_actor(),
        region: request
            .home_region
            .clone()
            .unwrap_or_else(|| state.invocations.default_region().into()),
        home_region: request.home_region.clone(),
        metadata: request.metadata,
    };
    grant.validate().map_err(ApiError::bad_request)?;
    let gateway =
        state.invocations.gateway.as_ref().ok_or_else(|| {
            ApiError::internal(anyhow::anyhow!("socket gateway is not configured"))
        })?;
    let issued = state
        .admin
        .issue_socket(grant, &gateway.origin)
        .map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(issued)).into_response())
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FindWebSocketRequest {
    #[serde(default)]
    home_region: Option<String>,
    metadata: Value,
}

async fn get_deployment(
    State(state): State<PublicApiState>,
    path: Path<ProjectPath>,
    headers: HeaderMap,
) -> Result<Json<CurrentDeployment>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let project = project_id(path)?;
    let spec = state
        .admin
        .current_deployment(&project)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "deployment not found"))?;
    let bundle = spec
        .code_snapshot
        .as_deref()
        .map(crate::bundle::ActorBundle::decode)
        .transpose()
        .map_err(ApiError::internal)?;
    let local_source = spec
        .source
        .map(|super::admin::DeploymentSource::Local(local)| local);
    Ok(Json(CurrentDeployment {
        organization_id: spec.organization_id,
        image_ref: (spec.image_policy == super::admin::ImagePolicy::Custom && bundle.is_some())
            .then_some(spec.image_ref),
        bundle,
        local_source,
        secret_refs: spec.secret_refs,
    }))
}

async fn delete_deployment(
    State(state): State<PublicApiState>,
    path: Path<ProjectPath>,
    headers: HeaderMap,
) -> Result<Json<DeploymentReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let project = project_id(path)?;
    let changed = state
        .invocations
        .delete_deployment(&state.admin, &project)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(DeploymentReply { changed }))
}

async fn register_deployment(
    State(state): State<PublicApiState>,
    path: Path<ProjectPath>,
    headers: HeaderMap,
    request: Result<Json<RegisterDeploymentRequest>, JsonRejection>,
) -> Result<Json<DeploymentReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Json(request) = request.map_err(ApiError::json)?;
    let image_policy = if request.image_ref.is_some() {
        super::admin::ImagePolicy::Custom
    } else {
        super::admin::ImagePolicy::Platform
    };
    let (source, code_snapshot, image_ref, working_directory, actor_entrypoint) =
        match (request.bundle, request.local_source) {
            (Some(bundle), None) => {
                let snapshot = bundle.encode().map_err(ApiError::bad_request)?;
                let entrypoint = bundle
                    .entrypoint()
                    .map_err(ApiError::bad_request)?
                    .to_owned();
                (
                    None,
                    Some(snapshot),
                    request.image_ref.unwrap_or_else(|| "bundle".into()),
                    "/customer".into(),
                    Some(entrypoint),
                )
            }
            (None, Some(local)) => {
                if request.image_ref.is_some() {
                    return Err(ApiError::bad_request("imageRef requires a hosted bundle"));
                }
                let directory = local.working_directory.clone();
                let entrypoint = local.actor_entrypoint.clone();
                (
                    Some(super::admin::DeploymentSource::Local(local)),
                    None,
                    "local".into(),
                    directory,
                    entrypoint,
                )
            }
            _ => {
                return Err(ApiError::bad_request(
                    "provide exactly one bundle or localSource",
                ));
            }
        };
    let contract = request
        .contract
        .map(PublicActorContract::new)
        .transpose()
        .map_err(ApiError::bad_request)?;
    let spec = HostLaunchSpec {
        image_policy,
        organization_id: request.organization_id,
        sandboxes: Default::default(),
        project_id: project_id(path)?,
        source,
        code_snapshot,
        image_ref,
        working_directory,
        actor_entrypoint,
        secret_refs: request.secret_refs,
    };
    let changed = state
        .invocations
        .deploy_source(&state.admin, &spec, contract.as_ref())
        .await
        .map_err(|error| {
            if error.is::<super::access::AccessDisabled>() {
                ApiError::access(error)
            } else {
                ApiError::bad_request(error)
            }
        })?;
    Ok(Json(DeploymentReply { changed }))
}

async fn find_actor(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    request: Result<Json<FindActorRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let target = resolve_actor_target(&state, &path.into_actor(), &headers, request, None).await?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({"homeRegion": target.home_region})),
    )
        .into_response())
}

pub(super) async fn resolve_actor_target(
    state: &PublicApiState,
    actor: &ActorKey,
    headers: &HeaderMap,
    request: Result<Json<FindActorRequest>, JsonRejection>,
    invocation: Option<&crate::actor::ActorInvocation>,
) -> Result<super::service::ActorTarget, ApiError> {
    let mut timings = TargetResolutionTimings::new();
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let grant = state
        .admin
        .authorize_discovery(
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or(""),
            &actor.project_id,
        )
        .map_err(|_| ApiError::unauthorized("actor discovery credential was rejected"))?;
    let Json(request) = request.map_err(ApiError::json)?;
    actor.validate().map_err(ApiError::bad_request)?;
    state
        .invocations
        .validate_home_region(request.home_region.as_deref())
        .map_err(ApiError::assignment)?;
    let result: Result<super::service::ActorTarget, ApiError> = async {
        actor.validate().map_err(ApiError::bad_request)?;
        timings.request_validated_at_ms = Some(timings.elapsed_ms());
        timings.client_authenticated_at_ms = Some(timings.elapsed_ms());
        let grant = match grant {
            Some(grant) => {
                let contract = state
                    .admin
                    .deployment_contract(&actor.project_id)
                    .await
                    .map_err(ApiError::internal)?
                    .ok_or_else(|| {
                        ApiError::new(
                            StatusCode::NOT_FOUND,
                            "not_found",
                            "actor contract not found",
                        )
                    })?;
                Some(
                    grant
                        .invocation(
                            actor,
                            contract.rpc_methods(&actor.actor_name).map_err(|_| {
                                ApiError::new(StatusCode::NOT_FOUND, "not_found", "actor not found")
                            })?,
                        )
                        .map_err(|_| {
                            ApiError::new(
                                StatusCode::FORBIDDEN,
                                "forbidden",
                                "actor has no published RPC methods",
                            )
                        })?,
                )
            }
            None => None,
        };
        let operation = invocation.map(|invocation| crate::sandbox::assignment::InitialOperation {
            request: crate::sandbox::assignment::InitialRequest::Invoke {
                invocation: invocation.clone(),
                grant: grant.clone(),
            },
            routing_ms: timings.elapsed_ms(),
        });
        let target = state
            .invocations
            .resolve_actor_target_timed(
                actor,
                request.home_region.as_deref(),
                &mut timings,
                grant,
                operation.as_ref(),
            )
            .await
            .map_err(ApiError::routing)?;
        Ok(target)
    }
    .await;
    let completed_at_ms = timings.elapsed_ms();
    match &result {
        Ok(_) => info!(
            event = "actor_target_resolution",
            request_id,
            actor_name = %actor.actor_name,
            actor_id = %actor.actor_id,
            started_at_ms = 0,
            request_validated_at_ms = timings.request_validated_at_ms,
            client_authenticated_at_ms = timings.client_authenticated_at_ms,
            deployment_loaded_at_ms = timings.deployment_loaded_at_ms,
            placement_loaded_at_ms = timings.placement_loaded_at_ms,
            lease_checked_at_ms = timings.lease_checked_at_ms,
            host_ensured_at_ms = timings.host_ensured_at_ms,
            placement_claimed_at_ms = timings.placement_claimed_at_ms,

            invocation_token_issued_at_ms = timings.invocation_token_issued_at_ms,
            route_selected_at_ms = timings.route_selected_at_ms,
            completed_at_ms,
            outcome = "resolved",
            "actor target resolution completed"
        ),
        Err(error) => warn!(
            event = "actor_target_resolution",
            request_id,
            actor_name = %actor.actor_name,
            actor_id = %actor.actor_id,
            started_at_ms = 0,
            request_validated_at_ms = timings.request_validated_at_ms,
            client_authenticated_at_ms = timings.client_authenticated_at_ms,
            deployment_loaded_at_ms = timings.deployment_loaded_at_ms,
            placement_loaded_at_ms = timings.placement_loaded_at_ms,
            lease_checked_at_ms = timings.lease_checked_at_ms,
            host_ensured_at_ms = timings.host_ensured_at_ms,
            placement_claimed_at_ms = timings.placement_claimed_at_ms,

            invocation_token_issued_at_ms = timings.invocation_token_issued_at_ms,
            route_selected_at_ms = timings.route_selected_at_ms,
            completed_at_ms,
            outcome = "failed",
            error_code = %error.code,
            error = %error.message,
            "actor target resolution failed"
        ),
    }
    result
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FindActorRequest {
    pub home_region: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct ActorPath {
    project_id: String,
    actor_name: String,
    actor_id: String,
}

impl ActorPath {
    pub(super) fn into_actor(self) -> ActorKey {
        ActorKey {
            project_id: self.project_id,
            actor_name: self.actor_name,
            actor_id: self.actor_id,
        }
    }
}

#[derive(Deserialize)]
pub(super) struct ProjectPath {
    pub(super) project_id: String,
}

pub(super) fn project_id(path: Path<ProjectPath>) -> Result<String, ApiError> {
    let Path(ProjectPath {
        project_id: project,
    }) = path;
    super::admin::validate_component("project ID", &project, 64).map_err(ApiError::bad_request)?;
    Ok(project)
}

pub(super) fn authorized_admin(admin: &AdminService, headers: &HeaderMap) -> Result<(), ApiError> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    admin
        .authenticate(authorization)
        .map_err(|_| ApiError::unauthorized("admin credential was rejected"))?;
    Ok(())
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RegisterDeploymentRequest {
    organization_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bundle: Option<crate::bundle::ActorBundle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    image_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    contract: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_source: Option<super::admin::LocalSource>,
    #[serde(default)]
    secret_refs: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CurrentDeployment {
    #[serde(skip_serializing_if = "Option::is_none")]
    image_ref: Option<String>,
    organization_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    bundle: Option<crate::bundle::ActorBundle>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_source: Option<super::admin::LocalSource>,
    secret_refs: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentReply {
    changed: bool,
}

pub(super) struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
    retry_after_seconds: Option<u64>,
}

impl ApiError {
    pub(super) fn json(error: JsonRejection) -> Self {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                error.body_text(),
            )
        } else {
            Self::bad_request(error.body_text())
        }
    }

    pub(super) fn assignment(error: anyhow::Error) -> Self {
        if error.is::<super::service::RegionConflict>() {
            Self::conflict(error.to_string())
        } else {
            Self::bad_request(error)
        }
    }

    pub(super) fn routing(error: anyhow::Error) -> Self {
        if error.is::<crate::sandbox::assignment::AssignmentOutcomeUnknown>() {
            Self::new(
                StatusCode::BAD_GATEWAY,
                "outcome_unknown",
                error.to_string(),
            )
        } else if error.is::<super::access::AccessDisabled>() {
            Self::access(error)
        } else if let Some(rejected) =
            error.downcast_ref::<super::organization_limits::AdmissionRejected>()
        {
            use super::organization_limits::AdmissionRejected;
            let (code, retry_after_seconds) = match rejected {
                AdmissionRejected::Quota { .. } => ("actor_quota_exceeded", None),
                AdmissionRejected::Rate {
                    retry_after_seconds,
                } => ("actor_start_rate_limited", Some(*retry_after_seconds)),
            };
            let mut response = Self::new(StatusCode::TOO_MANY_REQUESTS, code, rejected.to_string());
            response.retry_after_seconds = retry_after_seconds;
            response
        } else if error.is::<super::service::RegionConflict>() {
            Self::conflict(error.to_string())
        } else {
            Self::unavailable(format!("actor host is unavailable: {error:#}"))
        }
    }

    pub(super) fn access(error: anyhow::Error) -> Self {
        if error.is::<super::access::AccessDisabled>() {
            Self::new(StatusCode::FORBIDDEN, "access_disabled", error.to_string())
        } else {
            Self::internal(error)
        }
    }

    pub(super) fn bad_request(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            error.to_string(),
        )
    }

    pub(super) fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthenticated", message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    pub(super) fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
    }

    pub(super) fn internal(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            error.to_string(),
        )
    }

    pub(super) fn into_outcome(self) -> Value {
        serde_json::json!({"type": "failed", "code": self.code, "message": self.message})
    }

    pub(super) fn new(
        status: StatusCode,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
            retry_after_seconds: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(ErrorDocument {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response();
        if self.status == StatusCode::TOO_MANY_REQUESTS {
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
        }
        if let Some(seconds) = self.retry_after_seconds {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, seconds.into());
        }
        response
    }
}

#[derive(Serialize)]
struct ErrorDocument {
    error: ErrorBody,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: String,
    message: String,
}
