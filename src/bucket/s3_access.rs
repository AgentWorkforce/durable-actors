use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_sdk_sts::config::{BehaviorVersion, Region};
use reqsign_google::CredentialAccessBoundaryPermissions as Permissions;
use serde_json::{Value, json};

use super::access::{AwsSessionCredentials, Rule, StorageToken, StorageTokenSource};

/// STS refuses shorter sessions; hosts refresh 30 s before expiry.
const SESSION_SECONDS: i32 = 900;
/// STS limits the packed inline session policy; this is the plain-text bound we enforce.
pub(super) const MAX_POLICY_BYTES: usize = 2048;

/// How actor hosts get S3 credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum S3ActorCredentials {
    /// Per-actor `sts:AssumeRole` credentials narrowed by an inline session policy (default).
    AssumeRole { role_arn: String },
    /// The control plane's own short-lived credentials, for accounts where the control plane may
    /// not call STS. Hosts can then reach every actor's objects the control-plane role can.
    ControlPlane,
}

impl S3ActorCredentials {
    pub(super) async fn source(
        &self,
        endpoint: &super::S3Endpoint,
    ) -> Result<Box<dyn StorageTokenSource>> {
        Ok(match self {
            Self::AssumeRole { role_arn } => {
                Box::new(AssumeRoleTokens::from_environment(endpoint, role_arn).await?)
            }
            Self::ControlPlane => {
                tracing::warn!(
                    "actor hosts receive the control plane's S3 credentials; actors are isolated by bucket prefix, not per actor"
                );
                Box::new(ControlPlaneTokens::from_environment(endpoint).await?)
            }
        })
    }
}

/// Hands hosts the control plane's current credentials (EKS Pod Identity, IRSA), which expire and
/// are refreshed through the same RPC as scoped credentials.
pub(super) struct ControlPlaneTokens {
    provider: aws_credential_types::provider::SharedCredentialsProvider,
}

impl ControlPlaneTokens {
    async fn from_environment(endpoint: &super::S3Endpoint) -> Result<Self> {
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(endpoint.region.clone()))
            .load()
            .await;
        Ok(Self {
            provider: shared
                .credentials_provider()
                .context("no AWS credentials for the control plane")?,
        })
    }
}

#[async_trait]
impl StorageTokenSource for ControlPlaneTokens {
    async fn issue(&self, _: &[Rule]) -> Result<StorageToken> {
        use aws_credential_types::provider::ProvideCredentials;
        let credentials = self.provider.provide_credentials().await?;
        token(
            credentials.access_key_id(),
            credentials.secret_access_key(),
            credentials.session_token().unwrap_or_default(),
            credentials.expiry(),
        )
    }
}

/// Static keys never expire; hosts still refresh them on this cadence.
const STATIC_REFRESH: std::time::Duration = std::time::Duration::from_secs(900);

fn token(
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    expiry: Option<std::time::SystemTime>,
) -> Result<StorageToken> {
    let expires_at = expiry.unwrap_or_else(|| std::time::SystemTime::now() + STATIC_REFRESH);
    Ok(StorageToken {
        access_token: String::new(),
        expires_at_ms: expires_at
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            .try_into()?,
        aws: Some(AwsSessionCredentials {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: session_token.into(),
        }),
    })
}

/// Mints each actor's credentials with `sts:AssumeRole`, narrowing `role_arn` (which may reach every
/// actor's objects) to the actor's prefixes with an inline session policy, the S3 counterpart of
/// a GCS Credential Access Boundary.
pub(super) struct AssumeRoleTokens {
    client: aws_sdk_sts::Client,
    role_arn: String,
    create_only: bool,
}

impl AssumeRoleTokens {
    pub async fn from_environment(endpoint: &super::S3Endpoint, role_arn: &str) -> Result<Self> {
        ensure!(
            role_arn.starts_with("arn:"),
            "DURABLE_ACTORS_S3_ACTOR_ROLE_ARN must be an IAM role ARN"
        );
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(endpoint.region.clone()))
            .load()
            .await;
        let mut config = aws_sdk_sts::config::Builder::from(&shared);
        if let Some(url) = std::env::var("DURABLE_ACTORS_STS_ENDPOINT")
            .ok()
            .or_else(|| endpoint.endpoint.clone())
        {
            config = config.endpoint_url(url);
        }
        Ok(Self {
            client: aws_sdk_sts::Client::from_conf(config.build()),
            role_arn: role_arn.into(),
            create_only: std::env::var("DURABLE_ACTORS_S3_CREATE_ONLY_CONDITION")
                .map(|value| value != "false")
                .unwrap_or(true),
        })
    }
}

#[async_trait]
impl StorageTokenSource for AssumeRoleTokens {
    async fn issue(&self, rules: &[Rule]) -> Result<StorageToken> {
        let policy = session_policy(rules, self.create_only)?;
        let response = self
            .client
            .assume_role()
            .role_arn(&self.role_arn)
            .role_session_name(session_name())
            .duration_seconds(SESSION_SECONDS)
            .policy(policy)
            .send()
            .await
            .context("could not assume the actor storage role")?;
        let credentials = response
            .credentials()
            .context("STS returned no credentials")?;
        let expires_at = std::time::SystemTime::try_from(*credentials.expiration())
            .context("invalid STS expiry")?;
        token(
            credentials.access_key_id(),
            credentials.secret_access_key(),
            credentials.session_token(),
            Some(expires_at),
        )
    }
}

fn session_name() -> String {
    format!("durable-actors-{}", uuid::Uuid::new_v4().simple())
}

/// The inline policy granting exactly `rules`. Create-only grants (GCS `objectCreator`) require
/// `If-None-Match: *` through the `s3:if-none-match` condition key, so a host cannot overwrite an
/// immutable snapshot or artifact.
pub(super) fn session_policy(rules: &[Rule], create_only: bool) -> Result<String> {
    ensure!(!rules.is_empty(), "credential boundary has no rules");
    let mut statements = Vec::new();
    for rule in rules {
        let objects = format!("arn:aws:s3:::{}/{}*", rule.bucket, rule.prefix);
        let can_read = rule.permissions.contains(Permissions::OBJECT_VIEWER)
            || rule.permissions.contains(Permissions::OBJECT_USER);
        let can_replace = rule.permissions.contains(Permissions::OBJECT_USER);
        let can_create = rule.permissions.contains(Permissions::OBJECT_CREATOR);
        if can_read {
            statements.push(json!({"Effect":"Allow","Action":"s3:GetObject","Resource":objects}));
            statements.push(json!({
                "Effect":"Allow","Action":"s3:ListBucket","Resource":format!("arn:aws:s3:::{}", rule.bucket),
                "Condition":{"StringLike":{"s3:prefix":format!("{}*", rule.prefix)}}
            }));
        }
        if can_replace || (can_create && !create_only) {
            statements.push(json!({"Effect":"Allow","Action":"s3:PutObject","Resource":objects}));
        } else if can_create {
            statements.push(json!({
                "Effect":"Allow","Action":"s3:PutObject","Resource":objects,
                "Condition":{"StringEquals":{"s3:if-none-match":"*"}}
            }));
        }
    }
    let policy = serde_json::to_string(&Value::Object(
        [
            ("Version".to_owned(), json!("2012-10-17")),
            ("Statement".to_owned(), Value::Array(statements)),
        ]
        .into_iter()
        .collect(),
    ))?;
    ensure!(
        policy.len() <= MAX_POLICY_BYTES,
        "S3 session policy exceeds {MAX_POLICY_BYTES} bytes"
    );
    Ok(policy)
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/s3_access.rs"]
mod tests;
