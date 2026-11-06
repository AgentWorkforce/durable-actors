use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    http::header,
    routing::{MethodRouter, get},
};

use super::{ActorJwtIssuer, issuer::JWKS_PATH};

/// OpenID discovery for verifiers outside this process, such as the telemetry collector.
pub(super) fn router(issuer: &ActorJwtIssuer, public_origin: &str) -> Result<Router> {
    Ok(Router::new()
        .route(
            "/.well-known/openid-configuration",
            json(openid_configuration(public_origin)?),
        )
        .route(JWKS_PATH, json(issuer.jwks_json()?)))
}

// Discovery must name the URL that serves it, but tokens keep the configured `iss` so tokens
// already held by running hosts and older control planes stay valid. The collector therefore
// skips the issuer check and trusts these keys and the telemetry audience.
fn openid_configuration(public_origin: &str) -> Result<Vec<u8>> {
    let origin = public_origin.trim_end_matches('/');
    Ok(serde_json::to_vec(&serde_json::json!({
        "issuer": origin,
        "jwks_uri": format!("{origin}{JWKS_PATH}"),
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["EdDSA"],
    }))?)
}

fn json(body: Vec<u8>) -> MethodRouter {
    let body = Bytes::from(body);
    get(move || std::future::ready(([(header::CONTENT_TYPE, "application/json")], body.clone())))
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/discovery.rs"]
mod tests;
