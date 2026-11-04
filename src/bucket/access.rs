use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use reqsign_core::{ErrorKind, Granter, OsEnv, ProvideCredential, ProvideCredentialChain};
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_google::{
    ClientSideCredentialAccessBoundaryGranter, Credential, CredentialAccessBoundaryGrant,
    CredentialAccessBoundaryPermissions as Permissions, EnvCredentialProvider,
    ServiceAccountTokenCredentialProvider, VmMetadataCredentialProvider,
};
use reqsign_http_send_reqwest::ReqwestHttpSend;
use serde::{Deserialize, Serialize};

/// Tokens expire with the intermediary credential, which expires with the source token. The
/// metadata server serves one source token until about 5 minutes before it expires, so the
/// library's 30-minute default would refuse to mint for much of each hour. Hosts refresh 60 s
/// before expiry.
const MINIMUM_TOKEN_LIFETIME: Duration = Duration::from_secs(120);

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostStorageConfig {
    pub bucket: BucketLocation,
    pub persistence: super::SnapshotStoreConfig,
    pub region: String,
    pub tokens: Option<StorageTokens>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BucketLocation {
    Gcs {
        bucket: String,
        artifact_bucket: String,
    },
    S3 {
        bucket: String,
        artifact_bucket: String,
        s3: super::S3Endpoint,
    },
    File {
        directory: PathBuf,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageToken {
    pub access_token: String,
    pub expires_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsSessionCredentials>,
}

/// Temporary STS credentials; on S3 they replace the GCS bearer `access_token`, which is empty.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AwsSessionCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

/// One boundary spanning all of an actor's storage exceeds reqsign's 2,048-character limit once
/// identifiers and bucket names are long, so hosts hold two, each used by its own storage client.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageTokens {
    pub ownership: StorageToken,
    pub state: StorageToken,
}

impl std::fmt::Debug for StorageToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageToken")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

// Scoped tokens are credentials to customer data, so each request mints a fresh one and nothing
// keeps it. They are minted locally (client-side Credential Access Boundaries) from an
// intermediary credential held only in this process's memory, so a launch costs no STS call.
pub(crate) struct RuntimeAccess {
    location: BucketLocation,
    token_source: Option<Box<dyn StorageTokenSource>>,
    persistence: super::SnapshotStoreConfig,
}

impl RuntimeAccess {
    pub fn new(location: BucketLocation, persistence: super::SnapshotStoreConfig) -> Result<Self> {
        Ok(Self {
            token_source: match &location {
                BucketLocation::Gcs { .. } => Some(Box::new(ClientSideTokens::from_environment()?)),
                BucketLocation::S3 { .. } => None,
                BucketLocation::File { .. } => None,
            },
            location,
            persistence,
        })
    }

    /// S3 storage whose per-actor credentials come from `sts:AssumeRole` on `role_arn`.
    pub async fn s3(
        location: BucketLocation,
        persistence: super::SnapshotStoreConfig,
        role_arn: &str,
    ) -> Result<Self> {
        let BucketLocation::S3 { s3, .. } = &location else {
            anyhow::bail!("S3 credentials require an S3 bucket location");
        };
        ensure!(
            persistence.rapid_settings().is_none(),
            "Rapid persistence requires GCS"
        );
        Ok(Self {
            token_source: Some(Box::new(
                super::s3_access::AssumeRoleTokens::from_environment(s3, role_arn).await?,
            )),
            location,
            persistence,
        })
    }

    pub(crate) fn validate_code(&self, code: &crate::bundle::ActorBundle) -> Result<()> {
        let (BucketLocation::Gcs {
            artifact_bucket, ..
        }
        | BucketLocation::S3 {
            artifact_bucket, ..
        }) = &self.location
        else {
            anyhow::bail!("compiled bundles require object storage");
        };
        ensure!(
            code.bucket == *artifact_bucket,
            "code artifact belongs to another bucket"
        );
        artifact_prefix(code)?;
        Ok(())
    }

    pub async fn bootstrap(
        &self,
        region: &str,
        actor: &crate::actor::ActorKey,
        code_snapshot: Option<&str>,
    ) -> Result<String> {
        Ok(serde_json::to_string(&HostStorageConfig {
            bucket: self.location.clone(),
            persistence: self.persistence.clone(),
            region: region.into(),
            tokens: self.issue(actor, code_snapshot).await?,
        })?)
    }

    pub async fn issue(
        &self,
        actor: &crate::actor::ActorKey,
        code_snapshot: Option<&str>,
    ) -> Result<Option<StorageTokens>> {
        let (BucketLocation::Gcs {
            bucket,
            artifact_bucket,
        }
        | BucketLocation::S3 {
            bucket,
            artifact_bucket,
            ..
        }) = &self.location
        else {
            return Ok(None);
        };
        let code = code_snapshot
            .map(crate::bundle::ActorBundle::decode)
            .transpose()?;
        let scopes = scopes(
            bucket,
            &self.persistence,
            Some(artifact_bucket),
            actor,
            code.as_ref(),
        )?;
        let source = self
            .token_source
            .as_ref()
            .context("storage token issuer missing")?;
        let (ownership, state) =
            tokio::try_join!(source.issue(&scopes.ownership), source.issue(&scopes.state))?;
        Ok(Some(StorageTokens { ownership, state }))
    }
}

#[async_trait]
pub(super) trait StorageTokenSource: Send + Sync {
    async fn issue(&self, rules: &[Rule]) -> Result<StorageToken>;
}

struct ClientSideTokens {
    granter: Granter<Credential>,
    boundaries: ClientSideCredentialAccessBoundaryGranter,
}

impl ClientSideTokens {
    /// Signs in with the service-account key named by `GOOGLE_APPLICATION_CREDENTIALS`, or
    /// otherwise through the metadata server (Workload Identity on GKE).
    fn from_environment() -> Result<Self> {
        let context = reqsign_core::Context::new()
            .with_http_send(ReqwestHttpSend::new(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(5))
                    .build()?,
            ))
            .with_file_read(TokioFileRead)
            .with_env(OsEnv);
        let source = ProvideCredentialChain::new()
            .push(ServiceAccountTokenCredentialProvider::from_provider(
                EnvCredentialProvider::new(),
            ))
            .push(VmMetadataCredentialProvider::new());
        Ok(Self::new(context, source))
    }

    fn new(
        context: reqsign_core::Context,
        source: impl ProvideCredential<Credential = Credential> + 'static,
    ) -> Self {
        // Never granted: each issue swaps in the actor's grant, and an empty bucket name fails
        // validation. Clones share the source token and intermediary caches.
        let boundaries = ClientSideCredentialAccessBoundaryGranter::new(
            CredentialAccessBoundaryGrant::for_bucket("", Permissions::OBJECT_VIEWER),
        )
        .with_minimum_token_lifetime(MINIMUM_TOKEN_LIFETIME);
        Self {
            granter: Granter::new(context, source, boundaries.clone()),
            boundaries,
        }
    }
}

#[async_trait]
impl StorageTokenSource for ClientSideTokens {
    async fn issue(&self, rules: &[Rule]) -> Result<StorageToken> {
        let granter = self
            .granter
            .clone()
            .with_credential_granter(self.boundaries.clone().with_grant(grant(rules)?));
        let mut attempt = 1;
        let credential = (|| granter.grant(None))
            .retry(
                ExponentialBuilder::default()
                    .with_min_delay(Duration::from_millis(250))
                    .with_max_times(3)
                    .with_jitter(),
            )
            .when(reqsign_core::Error::is_retryable)
            .adjust(|error, delay| throttled_delay(error.kind(), delay))
            .notify(|error, delay| {
                attempt += 1;
                tracing::warn!(attempt, %error, delay_ms = delay.as_millis(),
                    "retrying scoped GCS credential grant");
            })
            .await?;
        storage_token(credential.token.context("GCS credential has no token")?)
    }
}

/// A 429 means the project-wide STS quota is exhausted, so retrying on the transient
/// schedule only adds to the next rejection.
fn throttled_delay(kind: ErrorKind, delay: Option<Duration>) -> Option<Duration> {
    delay.map(|delay| match kind {
        ErrorKind::RateLimited => delay + Duration::from_secs(1),
        _ => delay,
    })
}

fn storage_token(token: reqsign_google::Token) -> Result<StorageToken> {
    let expires_at = token.expires_at.context("GCS credential has no expiry")?;
    Ok(StorageToken {
        aws: None,
        access_token: token.access_token,
        expires_at_ms: expires_at
            .as_system_time()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            .try_into()?,
    })
}

fn grant(rules: &[Rule]) -> Result<CredentialAccessBoundaryGrant> {
    let (first, rest) = rules
        .split_first()
        .context("credential boundary has no rules")?;
    Ok(rest.iter().fold(
        CredentialAccessBoundaryGrant::for_object_prefix(
            &first.bucket,
            &first.prefix,
            first.permissions,
        ),
        |grant, rule| grant.with_object_prefix_rule(&rule.bucket, &rule.prefix, rule.permissions),
    ))
}

/// Grants `permissions` on the objects in `bucket` whose names start with `prefix`, including
/// listing them.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Rule {
    pub bucket: String,
    pub prefix: String,
    pub permissions: Permissions,
}

/// The rules behind each of [`StorageTokens`].
#[derive(Debug, Clone, PartialEq)]
struct Scopes {
    ownership: Vec<Rule>,
    state: Vec<Rule>,
}

fn scopes(
    bucket: &str,
    persistence: &super::SnapshotStoreConfig,
    artifact_bucket: Option<&str>,
    actor: &crate::actor::ActorKey,
    code: Option<&crate::bundle::ActorBundle>,
) -> Result<Scopes> {
    // Owner paths end in `<base64url id>.json`, and base64url has no `.`, so this prefix matches
    // only this actor's owner object.
    let mut ownership = vec![rule(
        bucket,
        crate::storage_paths::owner(&actor.storage_key())?,
        Permissions::OBJECT_USER,
    )];
    if let Some(code) = code {
        ensure!(
            Some(code.bucket.as_str()) == artifact_bucket,
            "code artifact belongs to another bucket"
        );
        ownership.push(rule(
            &code.bucket,
            artifact_prefix(code)?,
            Permissions::OBJECT_VIEWER,
        ));
    }
    Ok(Scopes {
        ownership,
        state: state_rules(bucket, persistence, actor)?,
    })
}

fn state_rules(
    bucket: &str,
    persistence: &super::SnapshotStoreConfig,
    actor: &crate::actor::ActorKey,
) -> Result<Vec<Rule>> {
    let snapshots = Permissions::OBJECT_VIEWER | Permissions::OBJECT_CREATOR;
    let prefix = crate::storage_paths::snapshots(actor)?;
    Ok(match persistence {
        super::SnapshotStoreConfig::Local => vec![rule(bucket, prefix, snapshots)],
        super::SnapshotStoreConfig::Rapid {
            archive_bucket,
            buckets,
            ..
        } => {
            let prefix = super::rapid::object_name(&prefix)?;
            let logs = prefix.replacen("snapshots-", "logs-", 1);
            std::iter::once(rule(archive_bucket, prefix, snapshots))
                .chain(buckets.iter().map(|placement| {
                    rule(&placement.bucket, logs.clone(), Permissions::OBJECT_USER)
                }))
                .collect()
        }
    })
}

fn rule(bucket: &str, prefix: String, permissions: Permissions) -> Rule {
    Rule {
        bucket: bucket.into(),
        prefix,
        permissions,
    }
}

fn artifact_prefix(code: &crate::bundle::ActorBundle) -> Result<String> {
    let root = format!("{}artifacts/", crate::storage_paths::ROOT);
    let first = code.files.first().context("code artifact is empty")?;
    let relative = first
        .object
        .strip_prefix(&root)
        .context("invalid artifact location")?;
    let (deployment, _) = relative
        .split_once('/')
        .context("artifact deployment missing")?;
    uuid::Uuid::parse_str(deployment).context("invalid artifact deployment")?;
    let prefix = format!("{root}{deployment}/");
    ensure!(
        code.files
            .iter()
            .all(|file| file.object == format!("{prefix}{}", file.path)),
        "code files must belong to one artifact deployment"
    );
    Ok(prefix)
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/access.rs"]
mod tests;
