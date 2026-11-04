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
        Ok(StorageToken {
            access_token: String::new(),
            expires_at_ms: expires_at
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis()
                .try_into()?,
            aws: Some(AwsSessionCredentials {
                access_key_id: credentials.access_key_id().into(),
                secret_access_key: credentials.secret_access_key().into(),
                session_token: credentials.session_token().into(),
            }),
        })
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
