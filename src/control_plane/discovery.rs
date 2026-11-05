use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    http::header,
    routing::{MethodRouter, get},
};

use super::{ActorJwtIssuer, issuer::JWKS_PATH};

/// OpenID discovery for verifiers outside this process, such as the telemetry collector.
pub(super) fn router(issuer: &ActorJwtIssuer) -> Result<Router> {
    Ok(Router::new()
        .route(
            "/.well-known/openid-configuration",
            json(issuer.openid_configuration()?),
        )
        .route(JWKS_PATH, json(issuer.jwks_json()?)))
}

fn json(body: Vec<u8>) -> MethodRouter {
    let body = Bytes::from(body);
    get(move || std::future::ready(([(header::CONTENT_TYPE, "application/json")], body.clone())))
}
