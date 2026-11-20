use super::{
    service::{ActorTarget, ControlPlaneService},
    socket_bus::SocketBus,
    socket_ticket::SocketTicket,
};
use crate::request_tracking::HostState;
use crate::{
    actor::{ActorKey, ActorSocketEffect, ActorSocketInvocation},
    platform_events::{
        PlatformEvents,
        sockets::{ALIVE_INTERVAL, SocketEvent},
    },
    sockets::{SocketRegistry, browser::SocketDispatcher},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub(crate) use crate::sockets::operations::{SocketOperation, SocketOperationReply};
#[derive(Serialize, Deserialize)]
pub(crate) struct SocketEventRequest {
    #[serde(default)]
    pub host_state: HostState,
    pub routing_ms: f64,
    pub owner_epoch: u64,
    pub invocation: ActorSocketInvocation,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum SocketEventReply {
    Completed { effects: Vec<ActorSocketEffect> },
    NotExecuted,
    Failed { message: String },
}

pub(super) struct SocketGateway {
    pub registry: SocketRegistry,
    pub stop: CancellationToken,
    pub(super) bus: Arc<dyn SocketBus>,
    pub(super) accepts_sockets: bool,
    pub(super) http: reqwest::Client,
    pub(super) events: Arc<dyn PlatformEvents<SocketEvent>>,
}

impl SocketGateway {
    pub(super) fn start(
        bus: Arc<dyn SocketBus>,
        accepts_sockets: bool,
        stop: CancellationToken,
        http: reqwest::Client,
        events: Arc<dyn PlatformEvents<SocketEvent>>,
    ) -> Result<Arc<Self>> {
        if accepts_sockets {
            tokio::spawn(report_alive(
                bus.registry().clone(),
                events.clone(),
                stop.clone(),
            ));
        }
        Ok(Arc::new(Self {
            registry: bus.registry().clone(),
            bus,
            accepts_sockets,
            stop,
            http,
            events,
        }))
    }

    pub(super) fn generation(&self) -> Option<u64> {
        if self.stop.is_cancelled() {
            return None;
        }
        self.bus.generation()
    }

    pub(super) fn ensure_delivery(&self, generation: u64) -> Result<()> {
        ensure!(
            self.generation() == Some(generation),
            "socket gateway lost its subscription"
        );
        Ok(())
    }

    pub(super) async fn operation(
        &self,
        actor: &ActorKey,
        operation: SocketOperation,
    ) -> Result<SocketOperationReply> {
        ensure!(!self.stop.is_cancelled(), "socket gateway stopping");
        match operation {
            SocketOperation::Publish { effects } => {
                crate::actor::validate_socket_effects(&effects)?;
                self.bus.publish(actor, effects).await?;
                Ok(SocketOperationReply::Published)
            }
            SocketOperation::Connections { tag } => Ok(SocketOperationReply::Connections {
                connections: self.bus.connections(actor, tag.as_deref()).await?,
            }),
            SocketOperation::Count => Ok(SocketOperationReply::Count {
                count: self.bus.count(actor).await?,
            }),
        }
    }

    async fn target(
        &self,
        service: &ControlPlaneService,
        ticket: &SocketTicket,
        invocation: &ActorSocketInvocation,
        started: Instant,
    ) -> Result<ActorTarget> {
        let operation = crate::sandbox::assignment::InitialOperation {
            request: crate::sandbox::assignment::InitialRequest::Socket {
                invocation: invocation.clone(),
            },
            routing_ms: started.elapsed().as_secs_f64() * 1000.0,
        };
        service
            .resolve_actor_route(
                &ticket.actor,
                ticket.home_region.as_deref(),
                None,
                None,
                Some(&operation),
            )
            .await
    }

    async fn invalidate(
        &self,
        service: &ControlPlaneService,
        actor: &ActorKey,
        target: &ActorTarget,
    ) {
        service.routes.remove(actor, &target.identity).await;
    }

    async fn dispatch(
        &self,
        service: &ControlPlaneService,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>> {
        let started = Instant::now();
        let retry_deadline = started + super::CONTROL_PLANE_REQUEST_TIMEOUT;
        let mut host_state = HostState::Warm;
        loop {
            ensure!(!self.stop.is_cancelled(), "socket gateway stopping");
            ensure!(
                Instant::now() < retry_deadline,
                "actor remained unavailable during socket handoff"
            );
            let target = tokio::time::timeout_at(
                retry_deadline,
                self.target(service, ticket, &invocation, started),
            )
            .await
            .context("actor remained unavailable during socket handoff")?;
            let mut target = match target {
                Ok(target) => target,
                Err(error) if error.is::<crate::sandbox::HostNotReady>() => {
                    self.wait_to_retry().await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if target.host_state == HostState::Cold {
                host_state = HostState::Cold;
            }
            let actor = &ticket.actor;
            let url = format!(
                "{}/v1/projects/{}/actors/{}/{}/socket-events",
                target.route.trim_end_matches('/'),
                actor.project_id,
                actor.actor_name,
                actor.actor_id
            );
            let reply = if let Some(reply) = target.initial_reply.take() {
                reply.decode::<SocketEventReply>()?
            } else {
                let reply = self
                    .http
                    .post(url)
                    .bearer_auth(&target.token)
                    .json(&SocketEventRequest {
                        host_state,
                        routing_ms: started.elapsed().as_secs_f64() * 1000.0,
                        owner_epoch: target.owner_epoch,
                        invocation: invocation.clone(),
                    })
                    .send()
                    .await;
                match reply {
                    Err(error) if error.is_connect() => {
                        self.invalidate(service, actor, &target).await;
                        self.wait_to_retry().await?;
                        continue;
                    }
                    Err(error) => {
                        self.invalidate(service, actor, &target).await;
                        return Err(error)
                            .context("socket event outcome unknown; event was not replayed");
                    }
                    Ok(reply)
                        if matches!(
                            reply.status(),
                            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                        ) =>
                    {
                        self.invalidate(service, actor, &target).await;
                        self.wait_to_retry().await?;
                        continue;
                    }
                    Ok(reply) => {
                        match async { reply.error_for_status()?.json::<SocketEventReply>().await }
                            .await
                        {
                            Ok(reply) => reply,
                            Err(error) => {
                                self.invalidate(service, actor, &target).await;
                                return Err(error).context(
                                    "socket event outcome unknown; event was not replayed",
                                );
                            }
                        }
                    }
                }
            };
            match reply {
                SocketEventReply::Completed { effects } => return Ok(effects),
                SocketEventReply::NotExecuted => self.invalidate(service, actor, &target).await,
                SocketEventReply::Failed { message } => {
                    anyhow::bail!("socket handler failed: {message}")
                }
            }
            self.wait_to_retry().await?;
        }
    }

    async fn wait_to_retry(&self) -> Result<()> {
        tokio::select! {
            _ = self.stop.cancelled() => anyhow::bail!("socket gateway stopped"),
            _ = tokio::time::sleep(Duration::from_millis(100)) => Ok(()),
        }
    }
}

pub(super) struct GatewaySocketDispatcher {
    pub gateway: Arc<SocketGateway>,
    pub service: ControlPlaneService,
    pub generation: u64,
}

#[async_trait]
impl SocketDispatcher for GatewaySocketDispatcher {
    fn ensure_authority(&self) -> Result<()> {
        self.gateway.ensure_delivery(self.generation)
    }
    async fn dispatch(
        &self,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>> {
        let result = self
            .gateway
            .dispatch(&self.service, ticket, invocation)
            .await;
        if let Err(error) = &result {
            tracing::warn!(actor = %ticket.actor.storage_key().as_str(), error = %format!("{error:#}"), "socket event delivery failed");
        }
        result
    }
    async fn deliver(
        &self,
        _: &SocketRegistry,
        actor: &ActorKey,
        effects: Vec<ActorSocketEffect>,
    ) -> Result<()> {
        let bus = self.gateway.bus.clone();
        let actor = actor.clone();
        tokio::spawn(async move {
            if let Err(error) = bus.publish(&actor, effects).await {
                tracing::warn!(actor = %actor.storage_key().as_str(), error = %format!("{error:#}"), "socket effects were not published");
            }
        });
        Ok(())
    }
    fn notify(&self, ticket: &SocketTicket, event: &crate::actor::ActorSocketEvent) {
        self.service
            .log_committed_socket_message(&ticket.actor, event);
    }
}

/// Readers treat a gateway's open connections as ended once these reports stop. Each report
/// names one project, so it reaches that project's observers like its connection events.
async fn report_alive(
    registry: SocketRegistry,
    events: Arc<dyn PlatformEvents<SocketEvent>>,
    stop: CancellationToken,
) {
    use crate::clock::{Clock, SystemClock};
    let mut ticks = tokio::time::interval(ALIVE_INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = ticks.tick() => {
                let at_ms = SystemClock.now_ms().unwrap_or_default();
                for project_id in registry.project_ids().await {
                    events.record(SocketEvent::Alive { project_id, at_ms });
                }
            }
        }
    }
}
