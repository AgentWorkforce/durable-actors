mod access;
pub(crate) mod admin;
mod alarms;
mod auth;
mod client;
mod contract_api;
pub(crate) mod contracts;
pub(crate) use client::{HostAuthorization, LeaseFence};
mod discovery;
mod event_sink;
mod gateway;
mod inspection;
#[cfg(test)]
#[path = "../../tests/unit/control_plane/inspection_tests.rs"]
mod inspection_tests;
mod invocation;
mod issuer;
mod local;
mod local_build;
mod local_telemetry;
mod local_watch;
mod process;
pub(crate) use process::RuntimeImages;
mod organization_limits;
mod protocol;
mod public_api;
mod regions;
mod routes;
mod service;
pub(crate) mod session;
pub(crate) mod socket_ticket;

use std::time::Duration;

pub(crate) const SUPPORTED_CONTROL_PLANE_PAYLOAD_BYTES: usize = usize::MAX;
pub(crate) const MAX_CONTROL_PLANE_MESSAGE_BYTES: usize = SUPPORTED_CONTROL_PLANE_PAYLOAD_BYTES;
pub(crate) const CONTROL_PLANE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub use local::{DevOptions, serve_local};
pub use organization_limits::OrganizationLimits;

#[cfg(test)]
pub(crate) use self::auth::ActorInvocationCapability;
pub use self::process::{
    AnalyticsBackend, ControlPlaneProcessConfig, ControlPlaneStorageConfig, serve_control_plane,
    serve_pool_manager,
};
pub(crate) use self::{
    admin::PostgresAdminRegistry,
    auth::{ActorJwtVerifier, ActorPrincipal, ActorTokenPurpose},
    client::ControlPlaneClient,
    issuer::ActorJwtIssuer,
    service::ControlPlaneService,
};
pub use crate::request_traces::persistence::clickhouse::ClickHouseConfig;

mod socket_bus;
pub(crate) mod socket_gateway;
mod socket_inventory;
