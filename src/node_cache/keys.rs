use std::{
    sync::{Arc, RwLock, Weak},
    time::Duration,
};

use anyhow::{Context, Result};

use crate::control_plane::{ActorJwtVerifier, ActorPrincipal, ActorTokenPurpose};

const REFRESH_INTERVAL: Duration = Duration::from_secs(600);
const RETRY_INTERVAL: Duration = Duration::from_secs(2);

pub(super) struct VerifierConfig {
    pub jwks_url: String,
    pub issuer: String,
    pub audience: String,
    pub max_lifetime: Duration,
}

/// Verifies host JWTs with the control plane's published keys, refetched so rotations reach
/// running nodes. Requests fail closed until the first fetch succeeds.
pub(super) struct HostTokens {
    verifier: RwLock<Option<ActorJwtVerifier>>,
}

impl HostTokens {
    pub fn load(config: VerifierConfig) -> Arc<Self> {
        let tokens = Arc::new(Self {
            verifier: RwLock::new(None),
        });
        tokio::spawn(refresh(Arc::downgrade(&tokens), config));
        tokens
    }

    pub fn fixed(verifier: ActorJwtVerifier) -> Arc<Self> {
        Arc::new(Self {
            verifier: RwLock::new(Some(verifier)),
        })
    }

    pub fn verify(&self, authorization: &str) -> Result<ActorPrincipal> {
        self.verifier
            .read()
            .map_err(|_| anyhow::anyhow!("host token verifier poisoned"))?
            .as_ref()
            .context("control plane keys are not loaded")?
            .authenticate_authorization(authorization)
    }
}

async fn refresh(tokens: Weak<HostTokens>, config: VerifierConfig) {
    let http = reqwest::Client::new();
    loop {
        let fetched = fetch(&http, &config).await;
        let Some(tokens) = tokens.upgrade() else {
            return;
        };
        let delay = match fetched {
            Ok(verifier) => {
                *tokens.verifier.write().unwrap() = Some(verifier);
                REFRESH_INTERVAL
            }
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "could not load control plane keys");
                RETRY_INTERVAL
            }
        };
        drop(tokens);
        tokio::time::sleep(delay).await;
    }
}

async fn fetch(http: &reqwest::Client, config: &VerifierConfig) -> Result<ActorJwtVerifier> {
    let keys = http
        .get(&config.jwks_url)
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    ActorJwtVerifier::for_scope(
        keys,
        config.issuer.clone(),
        config.audience.clone(),
        ActorTokenPurpose::ControlPlane,
        config.max_lifetime,
    )
}
