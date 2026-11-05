use crate::request_tracking::HostState;
use crate::sandbox::assignment::{AssignmentOutcomeUnknown, InitialOperation};
use serde_json::Value;
use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::{
    actor::{ActorKey, ActorSocketEvent},
    bucket::OwnershipHint,
    grpc::proto::{
        ControlPlaneReply, ControlPlaneRequest,
        actor_control_plane_service_server::{
            ActorControlPlaneService, ActorControlPlaneServiceServer,
        },
    },
    host::HostId,
    host_leases::HostLease,
    placement::{ObjectPlacement, ObjectPlacementStore},
    sandbox::{
        EnsureHostRequest, HostSandboxRuntimeConfig, HostTermination, SandboxProvider,
        TerminateHostsRequest,
    },
};

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminRegistry, AdminService, HostLaunchSpec},
    auth::{ActorJwtVerifier, ActorPrincipal},
    issuer::ActorJwtIssuer,
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_command, encode_reply},
    routes::{RouteEntry, RouteIdentity},
};

const FALLBACK_REGION: &str = "north-america-central";

#[derive(Clone)]
pub struct ControlPlaneService {
    pub(super) lifecycle: crate::actor_lifecycle::LifecycleStore,
    pub(super) changes: crate::postgres::notifications::ChangeFeed,
    pub(super) region: Option<String>,
    runtime_access: Option<Arc<crate::bucket::access::RuntimeAccess>>,
    pub(super) routes: Arc<dyn super::routes::RouteStore>,
    pub(super) gateway: Option<super::gateway::Gateway>,
    placements: Arc<dyn ObjectPlacementStore>,
    pub(super) auth: ActorJwtVerifier,
    host_token_issuer: ActorJwtIssuer,
    pub(super) registry: Arc<dyn AdminRegistry>,
    provisioner: Arc<dyn HostProvisioner>,
    alarms: Arc<dyn super::alarms::AlarmStore>,
    socket_events: Option<Arc<dyn super::event_sink::SocketMessageEventSink>>,
    local_builds: Option<Arc<super::local_build::LocalBuilds>>,
    hosts: reqwest::Client,
}

impl ControlPlaneService {
    pub(super) fn with_local_builds(
        mut self,
        builds: Arc<super::local_build::LocalBuilds>,
    ) -> Self {
        self.local_builds = Some(builds);
        self
    }

    pub(crate) fn with_runtime_access(
        mut self,
        access: Arc<crate::bucket::access::RuntimeAccess>,
    ) -> Self {
        self.runtime_access = Some(access);
        self
    }

    pub(crate) fn new(
        placements: Arc<dyn ObjectPlacementStore>,
        auth: ActorJwtVerifier,
        registry: Arc<dyn AdminRegistry>,
        issuer: ActorJwtIssuer,
        provisioner: Arc<dyn HostProvisioner>,
    ) -> Self {
        Self {
            lifecycle: Default::default(),
            changes: Default::default(),
            runtime_access: None,
            gateway: None,
            routes: Arc::new(super::routes::MemoryRoutes::default()),
            region: None,
            placements,
            auth,
            host_token_issuer: issuer,
            registry,
            provisioner,
            alarms: Arc::new(super::alarms::MemoryAlarmStore::default()),
            socket_events: None,
            local_builds: None,
            hosts: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("actor invocation HTTP client"),
        }
    }

    pub(super) fn host_client(&self) -> reqwest::Client {
        self.gateway.as_ref().map_or_else(
            || self.hosts.clone(),
            |gateway| gateway.connections.http.clone(),
        )
    }

    pub(super) fn with_alarm_store(mut self, alarms: Arc<dyn super::alarms::AlarmStore>) -> Self {
        self.alarms = alarms;
        self
    }

    pub(crate) fn start_alarm_scheduler(
        &self,
        poll_interval: std::time::Duration,
        stop: tokio_util::sync::CancellationToken,
    ) {
        super::alarms::start(self.clone(), self.alarms.clone(), poll_interval, stop);
    }

    pub(crate) fn with_socket_event_sink(
        mut self,
        sink: Option<Arc<dyn super::event_sink::SocketMessageEventSink>>,
    ) -> Self {
        self.socket_events = sink;
        self
    }

    pub fn into_internal_service(self) -> ActorControlPlaneServiceServer<Self> {
        ActorControlPlaneServiceServer::new(self)
            .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
    }

    pub(super) fn default_region(&self) -> &str {
        self.region.as_deref().unwrap_or(FALLBACK_REGION)
    }

    pub(super) async fn runtime_deployment(
        &self,
        project_id: &str,
    ) -> Result<Option<HostLaunchSpec>> {
        let spec = self.registry.launch_spec(project_id).await?;
        self.ensure_organization_access(spec.as_ref().map(|s| s.organization_id.as_str()))
            .await?;
        Ok(spec)
    }

    pub(super) async fn register_deployment(
        &self,
        admin: &AdminService,
        update: &mut dyn super::admin::DeploymentUpdate,
        spec: &HostLaunchSpec,
        contract: Option<&super::contracts::PublicActorContract>,
    ) -> Result<bool> {
        let previous = admin.current_deployment(&spec.project_id).await?;
        spec.validate()?;
        let replacing = previous.is_some();
        if let Some(previous) = previous {
            self.terminate_deployment_hosts(&previous).await?;
        }
        let changed = update.register(spec, contract).await?;
        self.routes.remove_project(&spec.project_id).await;
        self.changes.notify().await;
        Ok(changed || replacing)
    }

    pub(super) async fn deploy_source(
        &self,
        admin: &AdminService,
        source: &HostLaunchSpec,
        supplied_contract: Option<&super::contracts::PublicActorContract>,
    ) -> Result<bool> {
        source.validate()?;
        let mut update = admin.lock_deployment(&source.project_id).await?;
        self.ensure_project_access(&source.project_id).await?;
        self.ensure_organization_access(Some(&source.organization_id))
            .await?;
        let previous = admin.current_deployment(&source.project_id).await?;
        let local_build = match &self.local_builds {
            Some(builds) => Some(builds.prepare(source).await?),
            None => None,
        };
        let (mut prepared, mut compiled_contract) = match &local_build {
            Some(build) => (build.spec.clone(), Some(build.contract.clone())),
            None => {
                self.provisioner
                    .prepare_deployment(source, previous.as_ref(), self.default_region())
                    .await?
            }
        };
        if compiled_contract.is_none()
            && prepared.code_snapshot.is_some()
            && supplied_contract.is_none()
        {
            previous
                .as_ref()
                .filter(|old| old.code_snapshot == prepared.code_snapshot)
                .context("a new bundle requires its actor contract")?;
            let record = admin
                .deployment_contract(&source.project_id)
                .await?
                .context("compiled deployment contract is missing")?;
            compiled_contract = Some(super::contracts::PublicActorContract::new(record.contract)?);
        }
        if let (Some(compiled), Some(supplied)) = (&compiled_contract, supplied_contract) {
            ensure!(
                compiled.hash() == supplied.hash(),
                "supplied actor contract differs from compiled actor code"
            );
        }
        let contract = compiled_contract.as_ref().or(supplied_contract);
        prepared.sandboxes = contract
            .map(|contract| contract.sandboxes())
            .transpose()?
            .unwrap_or_default();
        self.provisioner.check_sandboxes(&prepared)?;
        let changed = self
            .register_deployment(admin, update.as_mut(), &prepared, contract)
            .await?;
        if let Some(build) = local_build {
            build.commit().await;
        }
        Ok(changed)
    }

    pub(super) async fn delete_deployment(
        &self,
        admin: &AdminService,
        project_id: &str,
    ) -> Result<bool> {
        let mut update = admin.lock_deployment(project_id).await?;
        let Some(previous) = admin.current_deployment(project_id).await? else {
            return Ok(false);
        };
        self.terminate_deployment_hosts(&previous).await?;
        update.remove().await?;
        self.routes.remove_project(project_id).await;
        self.changes.notify().await;
        Ok(true)
    }

    pub(super) async fn terminate_deployment_hosts(&self, spec: &HostLaunchSpec) -> Result<()> {
        self.routes.remove_project(&spec.project_id).await;
        let started_at = Instant::now();
        match self
            .provisioner
            .terminate_hosts(
                spec,
                &super::regions::ALL
                    .iter()
                    .map(|r| (*r).into())
                    .collect::<Vec<_>>(),
            )
            .await
        {
            Ok(termination) => info!(
                event = "actor_hosts_terminated",
                project_id = %spec.project_id,
                provider = %termination.provider,
                resource_count = termination.resource_ids.len(),
                total_ms = elapsed_ms(started_at),
                outcome = "terminated",
                "deployment hosts terminated"
            ),
            Err(error) => {
                return Err(
                    error.context("actor hosts could not be terminated; retry the operation")
                );
            }
        }
        Ok(())
    }

    pub(super) async fn resolve_actor_target_timed(
        &self,
        actor: &ActorKey,
        home_region: Option<&str>,
        timings: &mut TargetResolutionTimings,
        grant: Option<super::session::InvocationGrant>,
        operation: Option<&InitialOperation>,
    ) -> Result<ActorTarget> {
        self.resolve_actor_route(actor, home_region, Some(timings), grant, operation)
            .await
    }

    pub(super) fn validate_home_region(&self, assignment: Option<&str>) -> Result<()> {
        if let Some(assignment) = assignment {
            crate::placement::validate_region(assignment)?;
        }
        Ok(())
    }

    pub(super) fn deliver_socket_message_event(
        &self,
        actor: &ActorKey,
        trigger_id: Option<String>,
        event: &ActorSocketEvent,
    ) {
        let ActorSocketEvent::Message {
            connection_id,
            message,
        } = event
        else {
            return;
        };
        if let Some(sink) = self.socket_events.clone() {
            let event = super::event_sink::SocketMessageEvent::new(
                actor,
                trigger_id,
                connection_id,
                message,
            );
            tokio::spawn(async move {
                if let Err(error) = sink.deliver(event).await {
                    warn!(error = %format!("{error:#}"), "actor socket message trigger delivery failed");
                }
            });
        }
        info!(
            event = "actor_socket_message_committed",
            actor_name = %actor.actor_name,
            actor_id = %actor.actor_id,
            connection_id,
            message_kind = match message {
                crate::actor::ActorSocketMessage::Text { .. } => "text",
                crate::actor::ActorSocketMessage::Binary { .. } => "binary",
            },
            "actor socket message committed"
        );
    }

    pub(super) async fn resolve_actor_route(
        &self,
        actor: &ActorKey,
        home_region: Option<&str>,
        mut timings: Option<&mut TargetResolutionTimings>,
        grant: Option<super::session::InvocationGrant>,
        operation: Option<&InitialOperation>,
    ) -> Result<ActorTarget> {
        actor.validate()?;
        self.validate_home_region(home_region)?;
        if let Some(RouteEntry::Active(cached)) = self.routes.get(actor).await {
            if let Some(assigned) = home_region
                && (assigned != cached.placement.home_region
                    || (!cached.overrides_regions
                        && self
                            .region
                            .as_deref()
                            .is_some_and(|region| region != assigned)))
            {
                return Err(RegionConflict.into());
            }
            return self.issue_actor_target(
                actor,
                RoutedActor {
                    initial_reply: None,
                    host_state: HostState::Warm,
                    placement: cached.placement,
                    host_config_key: cached.host_config_key,
                },
                timings,
                grant,
            );
        }
        let target = self
            .route_actor_when_ready(
                actor,
                self.default_region(),
                home_region,
                timings.as_deref_mut(),
                operation,
            )
            .await?;
        let executed = target.initial_reply.is_some();
        self.issue_actor_target(actor, target, timings, grant)
            .map_err(|error| {
                if executed {
                    error.context(AssignmentOutcomeUnknown)
                } else {
                    error
                }
            })
    }

    fn issue_actor_target(
        &self,
        actor: &ActorKey,
        target: RoutedActor,
        mut timings: Option<&mut TargetResolutionTimings>,
        grant: Option<super::session::InvocationGrant>,
    ) -> Result<ActorTarget> {
        let issued = self.host_token_issuer.issue_invocation_target(
            actor,
            &target.placement.lease.id,
            &target.placement.lease.session_id,
            &target.host_config_key,
            &target.placement.home_region,
            target.placement.owner_epoch,
            grant,
            &target.placement.lease.route,
        )?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.invocation_token_issued_at_ms = Some(timings.elapsed_ms());
        }
        let identity = RouteIdentity::of(&target.placement);
        let route = target.placement.lease.route;
        if let Some(timings) = timings {
            timings.route_selected_at_ms = Some(timings.elapsed_ms());
        }
        Ok(ActorTarget {
            initial_reply: target.initial_reply,
            host_state: target.host_state,
            home_region: target.placement.home_region,
            route,
            token: issued.token,
            owner_epoch: target.placement.owner_epoch,
            identity,
        })
    }
}

#[derive(Clone)]
pub(super) struct ActorTarget {
    pub initial_reply: Option<Value>,
    pub host_state: HostState,
    pub home_region: String,
    pub route: String,
    pub token: String,
    pub owner_epoch: u64,
    pub identity: RouteIdentity,
}

pub(super) struct TargetResolutionTimings {
    started_at: Instant,
    pub request_validated_at_ms: Option<f64>,
    pub client_authenticated_at_ms: Option<f64>,
    pub deployment_loaded_at_ms: Option<f64>,
    pub placement_loaded_at_ms: Option<f64>,
    pub lease_checked_at_ms: Option<f64>,
    pub host_ensured_at_ms: Option<f64>,
    pub placement_claimed_at_ms: Option<f64>,
    pub invocation_token_issued_at_ms: Option<f64>,
    pub route_selected_at_ms: Option<f64>,
}

impl TargetResolutionTimings {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            request_validated_at_ms: None,
            client_authenticated_at_ms: None,
            deployment_loaded_at_ms: None,
            placement_loaded_at_ms: None,
            lease_checked_at_ms: None,
            host_ensured_at_ms: None,
            placement_claimed_at_ms: None,
            invocation_token_issued_at_ms: None,
            route_selected_at_ms: None,
        }
    }

    pub fn elapsed_ms(&self) -> f64 {
        elapsed_ms(self.started_at)
    }
}

#[tonic::async_trait]
impl ActorControlPlaneService for ControlPlaneService {
    async fn execute(
        &self,
        request: Request<ControlPlaneRequest>,
    ) -> std::result::Result<Response<ControlPlaneReply>, Status> {
        let principal = self.auth.authenticate(&request).await?;
        let command = decode_command(request.into_inner())
            .map_err(|error| Status::invalid_argument(format!("invalid command: {error:#}")))?;
        let reply = match command {
            ControlPlaneCommand::SocketOperation { actor, operation } => {
                self.authorize_socket_host(&principal, &actor)
                    .map_err(failed_precondition)?;
                let gateway = self
                    .gateway
                    .as_ref()
                    .ok_or_else(|| Status::unavailable("socket gateway is unavailable"))?;
                let reply = gateway
                    .connections
                    .operation(&actor, operation)
                    .await
                    .map_err(failed_precondition)?;
                ControlPlaneCommandReply::SocketOperation { reply }
            }
            command => self
                .execute_command(&principal, command)
                .await
                .map_err(failed_precondition)?,
        };
        Ok(Response::new(encode_reply(reply).map_err(internal)?))
    }
}

impl ControlPlaneService {
    async fn execute_command(
        &self,
        principal: &ActorPrincipal,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommandReply> {
        match command {
            ControlPlaneCommand::Lifecycle { report } => {
                use crate::clock::Clock;
                self.follow_host_route(principal, &report).await;
                self.lifecycle
                    .record(crate::actor_lifecycle::LifecycleEvent {
                        organization_id: principal.organization_id.clone(),
                        event_id: format!("{}:{}", principal.session_id, report.sequence),
                        actor: principal.actor.clone(),
                        host_id: principal.host_id.to_string(),
                        session_id: principal.session_id.clone(),
                        region: principal.region.clone(),
                        received_at_ms: crate::clock::SystemClock.now_ms()?,
                        report,
                    })
                    .await?;
                Ok(ControlPlaneCommandReply::Unit)
            }
            ControlPlaneCommand::SocketOperation { .. } => {
                anyhow::bail!("socket operation requires authenticated routing")
            }
            ControlPlaneCommand::GetAlarm => {
                let timestamp_ms = self.alarms.get(&principal.actor).await?;
                Ok(ControlPlaneCommandReply::Alarm { timestamp_ms })
            }
            ControlPlaneCommand::UpdateAlarm { timestamp_ms } => {
                self.alarms.update(&principal.actor, timestamp_ms).await?;
                Ok(ControlPlaneCommandReply::Unit)
            }
            ControlPlaneCommand::InvokeActor { call } => {
                let invocation = call.in_project(&principal.actor.project_id);
                let outcome = super::invocation::invoke_from_host(self, &invocation).await;
                Ok(ControlPlaneCommandReply::Invocation { outcome })
            }
            ControlPlaneCommand::RefreshStorageAccess => {
                self.require_active_host(principal).await?;
                let spec = self
                    .registry
                    .launch_spec(&principal.actor.project_id)
                    .await?
                    .context("deployment removed")?;
                ensure!(
                    principal.host_config_key.as_deref() == Some(spec.host_config_key().as_str()),
                    "host deployment changed"
                );
                let tokens = self
                    .runtime_access
                    .as_ref()
                    .context("direct storage is not configured")?
                    .issue(&principal.actor, spec.code_snapshot.as_deref())
                    .await?;
                let replacement_token = self
                    .host_token_issuer
                    .issue_host(
                        &spec.organization_id,
                        &principal.host_id,
                        &principal.session_id,
                        principal
                            .host_config_key
                            .as_deref()
                            .context("host configuration missing")?,
                        &principal.region,
                        &principal.actor,
                    )?
                    .token;
                Ok(ControlPlaneCommandReply::StorageAccess {
                    tokens,
                    replacement_token,
                })
            }
        }
    }

    async fn follow_host_route(
        &self,
        principal: &ActorPrincipal,
        report: &crate::actor_lifecycle::LifecycleReport,
    ) {
        use crate::actor_lifecycle::LifecycleKind;
        let identity = RouteIdentity {
            host: principal.host_id.clone(),
            session: principal.session_id.clone(),
            epoch: report.owner_epoch,
        };
        let actor = &principal.actor;
        match report.kind {
            LifecycleKind::Started | LifecycleKind::Heartbeat => {
                self.routes.renew(actor, &identity).await
            }
            LifecycleKind::Draining => self.routes.put(actor, RouteEntry::Draining(identity)).await,
            LifecycleKind::Stopped => self.routes.put(actor, RouteEntry::Stopped(identity)).await,
        }
    }

    pub(super) fn authorize_socket_host(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
    ) -> Result<()> {
        actor.validate()?;
        ensure!(&principal.actor == actor, "host actor scope mismatch");
        Ok(())
    }

    async fn route_actor_when_ready(
        &self,
        actor: &ActorKey,
        storage_region: &str,
        home_region: Option<&str>,
        mut timings: Option<&mut TargetResolutionTimings>,
        operation: Option<&InitialOperation>,
    ) -> Result<RoutedActor> {
        let deadline = tokio::time::Instant::now() + super::CONTROL_PLANE_REQUEST_TIMEOUT;
        loop {
            match self
                .route_actor(
                    actor,
                    storage_region,
                    home_region,
                    timings.as_deref_mut(),
                    operation,
                )
                .await
            {
                Err(error) if error.is::<ShuttingDownHost>() => {
                    tokio::time::timeout_at(
                        deadline,
                        self.wait_for_shutdown(actor, error.downcast_ref().unwrap()),
                    )
                    .await
                    .context("actor host is still shutting down; retry discovery")??;
                }
                result => return result,
            }
        }
    }

    async fn wait_for_shutdown(&self, actor: &ActorKey, host: &ShuttingDownHost) -> Result<()> {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            if host.lease.expires_at_ms <= crate::clock::Clock::now_ms(&crate::clock::SystemClock)?
                || self.routes.get(actor).await != Some(RouteEntry::Draining(host.identity.clone()))
            {
                return Ok(());
            }
        }
    }

    async fn route_actor(
        &self,
        actor: &ActorKey,
        storage_region: &str,
        home_region: Option<&str>,
        mut timings: Option<&mut TargetResolutionTimings>,
        operation: Option<&InitialOperation>,
    ) -> Result<RoutedActor> {
        self.validate_home_region(home_region)?;
        let generation = self.routes.generation(&actor.project_id).await;
        let spec = self
            .runtime_deployment(&actor.project_id)
            .await?
            .context("project has no registered actor code")?;
        let overrides_regions = spec
            .sandboxes
            .get(&actor.actor_name)
            .is_some_and(|options| options.regions.is_some());
        if !overrides_regions && let (Some(default), Some(assigned)) = (&self.region, home_region) {
            if default != assigned {
                return Err(RegionConflict.into());
            }
        }
        if let Some(timings) = timings.as_deref_mut() {
            timings.deployment_loaded_at_ms = Some(timings.elapsed_ms());
        }
        let (current, owner_hint) = self
            .placements
            .get_owner_with_hint(&actor.storage_key())
            .await?;
        if let (Some(assigned), Some(placement)) = (home_region, current.as_ref())
            && assigned != placement.home_region
        {
            return Err(RegionConflict.into());
        }
        let regions =
            placement_regions(&spec, actor, storage_region, home_region, current.as_ref())?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.placement_loaded_at_ms = Some(timings.elapsed_ms());
        }
        let lease_checked = current.as_ref().is_some_and(|placement| {
            host_matches_config(&placement.owner, &spec.host_config_key())
        });
        let active = self.active_target(actor, &current, &spec).await?;
        if lease_checked && let Some(timings) = timings.as_deref_mut() {
            timings.lease_checked_at_ms = Some(timings.elapsed_ms());
        }
        if let Some(target) = active {
            self.routes
                .remember(actor, target.cached(overrides_regions), generation)
                .await;
            return Ok(target);
        }
        let target = self
            .provision_in_regions(
                actor,
                &spec,
                current.as_ref(),
                owner_hint.as_ref(),
                &regions,
                operation,
            )
            .await?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.host_ensured_at_ms = Some(timings.elapsed_ms());
        }
        if let Some(timings) = timings {
            timings.placement_claimed_at_ms = Some(timings.elapsed_ms());
        }
        self.routes
            .remember(actor, target.cached(overrides_regions), generation)
            .await;
        Ok(target)
    }

    async fn provision_in_regions(
        &self,
        actor: &ActorKey,
        spec: &HostLaunchSpec,
        current: Option<&ObjectPlacement>,
        owner_hint: Option<&OwnershipHint>,
        regions: &[String],
        operation: Option<&InitialOperation>,
    ) -> Result<RoutedActor> {
        let mut last_error = None;
        for region in regions {
            match self
                .ensure_actor_host(actor, spec, current, region, owner_hint, operation)
                .await
            {
                Ok(target) => return Ok(target),
                Err(error)
                    if error.is::<AssignmentOutcomeUnknown>()
                        || error.is::<super::organization_limits::AdmissionFailure>() =>
                {
                    return Err(error);
                }
                Err(error) => {
                    let current = self.placements.get_owner(&actor.storage_key()).await?;
                    if let Some(placement) = &current {
                        if !regions.contains(&placement.home_region) {
                            return Err(RegionConflict.into());
                        }
                        if let Some(mut target) = self.active_target(actor, &current, spec).await? {
                            target.host_state = HostState::Cold;
                            return Ok(target);
                        }
                        return Err(error);
                    }
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.context("actor has no eligible sandbox region")?)
    }

    async fn ensure_actor_host(
        &self,
        actor: &ActorKey,
        spec: &HostLaunchSpec,
        current: Option<&ObjectPlacement>,
        requested_region: &str,
        owner_hint: Option<&OwnershipHint>,
        operation: Option<&InitialOperation>,
    ) -> Result<RoutedActor> {
        let region = current
            .map(|p| p.home_region.clone())
            .unwrap_or_else(|| requested_region.to_owned());
        let (lease, owner_epoch, initial_reply) = self
            .provisioner
            .ensure_actor_host(
                spec,
                &region,
                actor,
                current.is_none(),
                owner_hint,
                operation,
            )
            .await?;
        ensure!(
            owner_epoch > 0,
            "host readiness returned no ownership epoch"
        );
        Ok(RoutedActor {
            initial_reply,
            host_state: HostState::Cold,
            placement: ObjectPlacement {
                lease: lease.clone(),
                object: actor.storage_key(),
                owner: lease.id.clone(),
                owner_epoch,
                home_region: region,
                state_version: 0,
                state_object: None,
                last_request_id: None,
            },
            host_config_key: spec.host_config_key(),
        })
    }

    async fn require_active_host(&self, principal: &ActorPrincipal) -> Result<HostLease> {
        let placement = self.current_placement(&principal.actor).await?;
        ensure!(
            placement.owner == principal.host_id
                && placement.lease.id == principal.host_id
                && placement.home_region == principal.region,
            "host no longer owns actor"
        );
        let lease = placement.lease;
        ensure!(
            lease.expires_at_ms > crate::clock::Clock::now_ms(&crate::clock::SystemClock)?,
            "host lease is not active"
        );
        ensure!(
            lease.session_id == principal.session_id,
            "host lease belongs to another session"
        );
        Ok(lease)
    }

    async fn current_placement(&self, actor: &ActorKey) -> Result<ObjectPlacement> {
        self.placements
            .get_owner(&actor.storage_key())
            .await?
            .context("actor has no current placement")
    }

    async fn active_target(
        &self,
        actor: &ActorKey,
        current: &Option<ObjectPlacement>,
        spec: &HostLaunchSpec,
    ) -> Result<Option<RoutedActor>> {
        let Some(placement) = current else {
            return Ok(None);
        };
        if !host_matches_config(&placement.owner, &spec.host_config_key()) {
            return Ok(None);
        }
        let lease = &placement.lease;
        if lease.id != placement.owner
            || lease.expires_at_ms <= crate::clock::Clock::now_ms(&crate::clock::SystemClock)?
        {
            return Ok(None);
        }
        let identity = RouteIdentity::of(placement);
        if let Some(RouteEntry::Draining(shutting_down) | RouteEntry::Stopped(shutting_down)) =
            self.routes.get(actor).await
            && shutting_down == identity
        {
            return Err(ShuttingDownHost {
                lease: lease.clone(),
                identity,
            }
            .into());
        }
        self.provisioner.wait_ready(&placement.owner).await?;
        Ok(Some(RoutedActor {
            initial_reply: None,
            host_state: HostState::Warm,
            placement: placement.clone(),
            host_config_key: spec.host_config_key(),
        }))
    }
}

#[derive(Debug)]
struct ShuttingDownHost {
    lease: HostLease,
    identity: RouteIdentity,
}

impl std::fmt::Display for ShuttingDownHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("actor host is shutting down")
    }
}

impl std::error::Error for ShuttingDownHost {}

fn select_target_region(current: Option<&ObjectPlacement>, requested: &str) -> Result<String> {
    if let Some(placement) = current {
        return Ok(placement.home_region.clone());
    }
    let region = super::regions::storage_region(requested).unwrap_or(FALLBACK_REGION);
    crate::placement::validate_region(region)?;
    Ok(region.into())
}

fn placement_regions(
    spec: &HostLaunchSpec,
    actor: &ActorKey,
    requested: &str,
    assignment: Option<&str>,
    current: Option<&ObjectPlacement>,
) -> Result<Vec<String>> {
    let allowed = spec
        .sandboxes
        .get(&actor.actor_name)
        .and_then(|options| options.regions.as_ref());
    if let Some(region) =
        assignment.or_else(|| current.map(|placement| placement.home_region.as_str()))
    {
        if allowed.is_some_and(|regions| !regions.iter().any(|allowed| allowed == region)) {
            return Err(RegionConflict.into());
        }
        return Ok(vec![region.into()]);
    }
    let requested = select_target_region(None, requested)?;
    let Some(allowed) = allowed else {
        return Ok(vec![requested]);
    };
    let mut regions = allowed.clone();
    regions.sort_by_cached_key(|region| {
        let identity =
            serde_json::to_vec(&(actor, region)).expect("placement identity is serializable");
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &identity);
        (region != &requested, digest.as_ref().to_vec())
    });
    Ok(regions)
}

#[derive(Debug)]
pub(super) struct RegionConflict;

impl std::fmt::Display for RegionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "homeRegion conflicts with the control-plane region, allowed sandbox regions, or existing actor ownership",
        )
    }
}

impl std::error::Error for RegionConflict {}

#[async_trait]
pub(crate) trait HostProvisioner: Send + Sync {
    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        previous: Option<&HostLaunchSpec>,
        region: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<super::contracts::PublicActorContract>,
    )>;
    fn check_sandboxes(&self, _spec: &HostLaunchSpec) -> Result<()> {
        Ok(())
    }

    async fn wait_ready(&self, _host: &HostId) -> Result<()> {
        Ok(())
    }

    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
        new_actor: bool,
        owner_hint: Option<&OwnershipHint>,
        operation: Option<&InitialOperation>,
    ) -> Result<(HostLease, u64, Option<Value>)>;
    async fn terminate_hosts(
        &self,
        spec: &HostLaunchSpec,
        regions: &[String],
    ) -> Result<HostTermination>;
}

pub(crate) struct SandboxHostProvisioner {
    admission: Option<Arc<super::organization_limits::ActorAdmission>>,
    runtime_images: Option<super::process::RuntimeImages>,
    pool: Option<Arc<crate::sandbox::pool::SparePool>>,
    resource_bounds: Option<crate::sandbox::ResourceBounds>,
    runtime_access: Option<Arc<crate::bucket::access::RuntimeAccess>>,
    provider: Arc<dyn SandboxProvider>,
    runtime: HostSandboxRuntimeConfig,
    issuer: ActorJwtIssuer,
}

impl SandboxHostProvisioner {
    pub(super) fn with_actor_admission(
        mut self,
        admission: Arc<super::organization_limits::ActorAdmission>,
    ) -> Self {
        self.admission = Some(admission);
        self
    }

    pub(crate) fn with_pool(mut self, pool: Arc<crate::sandbox::pool::SparePool>) -> Self {
        self.pool = Some(pool);
        self
    }

    pub(crate) fn with_resource_bounds(mut self, bounds: crate::sandbox::ResourceBounds) -> Self {
        self.resource_bounds = Some(bounds);
        self
    }

    pub(crate) fn with_runtime_access(
        mut self,
        access: Arc<crate::bucket::access::RuntimeAccess>,
    ) -> Self {
        self.runtime_access = Some(access);
        self
    }
    pub(crate) fn new(
        provider: Arc<dyn SandboxProvider>,
        runtime: HostSandboxRuntimeConfig,
        issuer: ActorJwtIssuer,
        runtime_images: Option<super::process::RuntimeImages>,
    ) -> Self {
        Self {
            runtime_images,
            admission: None,
            pool: None,
            resource_bounds: None,
            runtime_access: None,
            provider,
            runtime,
            issuer,
        }
    }
}

#[async_trait]
impl HostProvisioner for SandboxHostProvisioner {
    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _previous: Option<&HostLaunchSpec>,
        _region: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<super::contracts::PublicActorContract>,
    )> {
        let snapshot = source
            .code_snapshot
            .as_deref()
            .context("hosted deployments require a compiled bundle")?;
        let manifest = crate::bundle::ActorBundle::decode(snapshot)?;
        self.runtime_access
            .as_ref()
            .context("hosted deployments require artifact storage access")?
            .validate_code(&manifest)?;
        let mut prepared = source.clone();
        if source.image_policy == super::admin::ImagePolicy::Platform {
            prepared.image_ref = self
                .runtime_images
                .as_ref()
                .context("hosted deployments require runtime images")?
                .for_entrypoint(manifest.entrypoint()?)?
                .into();
        } else {
            crate::sandbox::gke::validate_image(&source.image_ref)?;
        }
        prepared.validate()?;
        Ok((prepared, None))
    }

    fn check_sandboxes(&self, spec: &HostLaunchSpec) -> Result<()> {
        if let Some(bounds) = &self.resource_bounds {
            for (actor, options) in &spec.sandboxes {
                options.check_bounds(actor, bounds)?;
            }
        }
        Ok(())
    }
    async fn wait_ready(&self, host: &HostId) -> Result<()> {
        match &self.pool {
            Some(pool) => pool.wait_ready(host.as_str()).await,
            None => self.provider.wait_ready(host).await,
        }
    }

    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
        new_actor: bool,
        owner_hint: Option<&OwnershipHint>,
        operation: Option<&InitialOperation>,
    ) -> Result<(HostLease, u64, Option<Value>)> {
        self.launch(spec, region, actor, new_actor, owner_hint, operation)
            .await
    }

    async fn terminate_hosts(
        &self,
        spec: &HostLaunchSpec,
        regions: &[String],
    ) -> Result<HostTermination> {
        if let Some(pool) = &self.pool {
            return Ok(HostTermination {
                provider: "gke".into(),
                resource_ids: pool.retire_config(&spec.host_config_key()).await?,
            });
        }
        self.provider
            .terminate_hosts(&TerminateHostsRequest {
                host_config_key: spec.host_config_key(),
                canonical_regions: regions.to_vec(),
            })
            .await
    }
}

impl SandboxHostProvisioner {
    async fn launch(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
        new_actor: bool,
        owner_hint: Option<&OwnershipHint>,
        operation: Option<&InitialOperation>,
    ) -> Result<(HostLease, u64, Option<Value>)> {
        let started_at = Instant::now();
        let mut request = self.request(spec, region, actor)?;
        request.initial_operation = operation.cloned();
        request.actor_is_new = new_actor;
        request.owner_hint = owner_hint.map(serde_json::to_string).transpose()?;
        let Some(admission) = &self.admission else {
            return self
                .provision(spec, region, actor, request, started_at)
                .await;
        };
        let (host, session) = (request.host_id.clone(), request.session_id.clone());
        let organization = &spec.organization_id;
        admission
            .reserve(organization, actor, &host, &session)
            .await?;
        let launched = self
            .provision(spec, region, actor, request, started_at)
            .await;
        if launched.is_err() {
            admission
                .release(organization, actor, &host, &session)
                .await;
        }
        launched
    }

    async fn provision(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
        mut request: EnsureHostRequest,
        started_at: Instant,
    ) -> Result<(HostLease, u64, Option<Value>)> {
        let token = async {
            match &self.runtime_access {
                Some(access) => Ok(Some(
                    access
                        .bootstrap(region, actor, spec.code_snapshot.as_deref())
                        .await?,
                )),
                None => anyhow::Ok(None),
            }
        };
        let spare = async {
            match &self.pool {
                Some(pool) => {
                    pool.claim(spec, region, request.host_id.as_str(), &request.resources)
                        .await
                }
                None => Ok(None),
            }
        };
        let (runtime_config, spare) = tokio::join!(token, spare);
        request.spare = spare?;
        if let Some(pool) = &self.pool {
            if request.spare.is_none() {
                pool.reserve_host(
                    &request.session_id,
                    request.host_id.as_str(),
                    &spec.host_config_key(),
                )
                .await?;
            }
        }
        request.runtime_config = match runtime_config {
            Ok(config) => config,
            Err(error) => {
                if let Some(pool) = &self.pool {
                    let _ = pool.failed(request.host_id.as_str()).await;
                }
                return Err(error);
            }
        };
        if let Some(operation) = &mut request.initial_operation {
            operation.routing_ms += elapsed_ms(started_at);
        }
        let handle = match self.provider.ensure_host(&request).await {
            Ok(handle) => handle,
            Err(error) => {
                if let Some(pool) = &self.pool {
                    let _ = pool.failed(request.host_id.as_str()).await;
                }
                warn!(
                    event = "actor_host_provisioning",
                    project_id = %spec.project_id,
                    region,
                    host_id = %request.host_id,
                    started_at_ms = 0,
                    completed_at_ms = elapsed_ms(started_at),
                    outcome = "provider_failed",
                    error = %format!("{error:#}"),
                    "actor host provisioning failed"
                );
                return Err(error);
            }
        };
        let provisioning = handle.provisioning.clone();
        let owner_epoch = handle.owner_epoch;
        let ready = async {
            ensure!(
                owner_epoch > 0,
                "host readiness returned no ownership epoch"
            );
            if self.pool.is_some() {
                ensure!(
                    handle.host_id == request.host_id,
                    "provider returned a different host identity"
                );
            }
            let lease = ready_lease(&handle, &request)?;
            if let Some(pool) = &self.pool {
                let provisioning = provisioning
                    .as_ref()
                    .context("provider did not identify the assigned sandbox")?;
                pool.remember(
                    lease.id.as_str(),
                    &spec.host_config_key(),
                    &crate::sandbox::SpareHandle {
                        control_route: String::new(),
                        control_token: String::new(),
                        name: request
                            .spare
                            .as_ref()
                            .map(|spare| spare.name.clone())
                            .unwrap_or_else(|| format!("do-actor-{}", request.session_id)),
                        resource_id: provisioning.resource_id.clone(),
                        route: lease.route.clone(),
                        canonical_region: region.into(),
                    },
                )
                .await?;
            }
            anyhow::Ok(lease)
        }
        .await;
        let lease = match ready {
            Ok(lease) => lease,
            Err(error) => {
                if let Some(pool) = &self.pool {
                    let _ = pool.failed(request.host_id.as_str()).await;
                }
                return Err(if handle.initial_reply.is_some() {
                    error.context(AssignmentOutcomeUnknown)
                } else {
                    error
                });
            }
        };
        let lease_validated_at_ms = elapsed_ms(started_at);
        info!(
            event = "actor_host_provisioning",
            project_id = %spec.project_id,
            region,
            host_id = %lease.id,
            provider = provisioning.as_ref().map(|value| value.provider.as_str()).unwrap_or("unknown"),
            provider_resource_id = provisioning.as_ref().map(|value| value.resource_id.as_str()).unwrap_or(""),
            provider_reused = provisioning.as_ref().is_some_and(|value| value.reused),
            started_at_ms = 0,
            provider_process_spawned_at_ms = provisioning.as_ref().and_then(|value| value.command_spawned_at_ms),
            provider_request_written_at_ms = provisioning.as_ref().and_then(|value| value.request_written_at_ms),
            provider_process_completed_at_ms = provisioning.as_ref().and_then(|value| value.process_completed_at_ms),
            provider_response_decoded_at_ms = provisioning.as_ref().and_then(|value| value.response_decoded_at_ms),
            provider_provider_started_at_ms = provisioning.as_ref().map(|value| value.started_at_ms),
            provider_input_parsed_at_ms = provisioning.as_ref().and_then(|value| value.input_parsed_at_ms),
            provider_sdk_loaded_at_ms = provisioning.as_ref().and_then(|value| value.sdk_loaded_at_ms),
            provider_resources_resolved_at_ms = provisioning.as_ref().and_then(|value| value.resources_resolved_at_ms),
            provider_sandbox_scheduled_at_ms = provisioning.as_ref().and_then(|value| value.sandbox_scheduled_at_ms),
            provider_host_ready_observed_at_ms = provisioning.as_ref().and_then(|value| value.host_ready_observed_at_ms),
            provider_route_read_at_ms = provisioning.as_ref().and_then(|value| value.route_read_at_ms),
            provider_provider_completed_at_ms = provisioning.as_ref().map(|value| value.completed_at_ms),
            lease_validated_at_ms,
            completed_at_ms = elapsed_ms(started_at),
            outcome = "ready",
            "actor host provisioning completed"
        );
        Ok((lease, owner_epoch, handle.initial_reply))
    }

    fn request(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
    ) -> Result<EnsureHostRequest> {
        let options = spec
            .sandboxes
            .get(&actor.actor_name)
            .cloned()
            .unwrap_or_default();
        options.validate()?;
        let defaults = self
            .pool
            .as_ref()
            .map(|pool| pool.config.resources.clone())
            .unwrap_or_default();
        let config_key = spec.host_config_key();
        let host_id = HostId::new(format!("host.v3.{}.{}", config_key, uuid::Uuid::new_v4()));
        let session_id = uuid::Uuid::new_v4().to_string();
        let host_token = self
            .issuer
            .issue_host(
                &spec.organization_id,
                &host_id,
                &session_id,
                &config_key,
                region,
                actor,
            )?
            .token;
        Ok(EnsureHostRequest {
            initial_operation: None,
            actor_is_new: false,
            owner_hint: None,
            actor: Some(actor.clone()),
            code_snapshot: spec.code_snapshot.clone(),
            spare: None,
            resources: options.resources(defaults),
            runtime_config: None,
            host_config_key: config_key,
            canonical_region: region.to_owned(),
            host_id,
            session_id,
            host_token,
            jwt_public_keys: self.issuer.verifier_keys_json()?,
            control_plane_url: self.runtime.control_plane_url.clone(),
            otlp_endpoint: self.runtime.otlp_endpoint.clone(),
            jwt_issuer: self.runtime.jwt_issuer.clone(),
            invocation_jwt_audience: self.runtime.invocation_jwt_audience.clone(),
            image_ref: match &self.runtime_images {
                Some(images) => images.for_deployment(spec)?.into(),
                None => spec.image_ref.clone(),
            },
            working_directory: spec.working_directory.clone(),
            actor_entrypoint: spec
                .actor_entrypoint
                .clone()
                .or_else(|| spec.code_snapshot.as_ref().map(|_| "actors.mjs".into())),
            secret_refs: spec.secret_refs.clone(),
            host_idle_timeout_ms: options
                .idle_timeout_ms
                .unwrap_or(self.runtime.host_idle_timeout_ms),
        })
    }
}

fn ready_lease(
    handle: &crate::sandbox::ActorHostHandle,
    request: &EnsureHostRequest,
) -> Result<HostLease> {
    use crate::clock::{Clock, SystemClock};
    ensure!(
        handle.host_id == request.host_id && handle.canonical_region == request.canonical_region,
        "sandbox readiness scope mismatch"
    );
    validate_host_route(&handle.route)?;
    let lease = handle
        .lease
        .clone()
        .context("sandbox readiness omitted activation lease")?;
    ensure!(
        lease.id == request.host_id
            && lease.session_id == request.session_id
            && lease.route == handle.route,
        "sandbox readiness lease mismatch"
    );
    let now = SystemClock.now_ms()?;
    ensure!(
        lease.expires_at_ms > now
            && lease.expires_at_ms
                <= now.saturating_add(crate::host_leases::MAX_HOST_LEASE_DURATION_MS + 5_000),
        "sandbox readiness lease expired or invalid"
    );
    Ok(lease)
}

struct RoutedActor {
    initial_reply: Option<Value>,
    host_state: HostState,
    placement: ObjectPlacement,
    host_config_key: String,
}

impl RoutedActor {
    fn cached(&self, overrides_regions: bool) -> super::routes::CachedRoute {
        super::routes::CachedRoute {
            placement: self.placement.clone(),
            host_config_key: self.host_config_key.clone(),
            overrides_regions,
        }
    }
}

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1_000.0
}

fn host_matches_config(host: &HostId, config_key: &str) -> bool {
    host.as_str().starts_with(&format!("host.v3.{config_key}."))
}

fn validate_host_route(route: &str) -> Result<()> {
    let route = reqwest::Url::parse(route)?;
    ensure!(
        matches!(route.scheme(), "http" | "https") && route.host_str().is_some(),
        "host route must be an HTTP origin"
    );
    ensure!(
        route.username().is_empty() && route.password().is_none(),
        "host route must not contain credentials"
    );
    ensure!(
        route.path() == "/" && route.query().is_none() && route.fragment().is_none(),
        "host route must not contain a path, query, or fragment"
    );
    Ok(())
}

fn failed_precondition(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/streaming_tests.rs"]
mod streaming_tests;

#[cfg(test)]
#[path = "../../tests/unit/control_plane/deployment.rs"]
mod deployment_tests;

#[cfg(test)]
#[path = "../../tests/unit/control_plane/service.rs"]
pub(crate) mod tests;
