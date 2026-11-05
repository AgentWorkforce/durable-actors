use std::time::Duration;

use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};

use super::*;
use crate::{
    actor::ActorKey,
    control_plane::{ActorJwtIssuer, ActorTokenPurpose, issuer::TELEMETRY_AUDIENCE},
    host::HostId,
    request_traces::{logs::LogQuery, persistence::TraceQueries},
};

#[tokio::test]
async fn exports_require_a_host_token_for_the_telemetry_audience() -> anyhow::Result<()> {
    let issuer = issuer()?;
    let (sink, _) = sink(&issuer)?;
    let invocation = issuer.issue_invocation_target(
        &actor(),
        &HostId::new("host.v3.r1.one"),
        "session",
        "r1",
        "us-east",
        1,
        None,
        "http://127.0.0.1:7101",
    )?;
    for token in [None, Some("not-a-jwt".into()), Some(invocation.token)] {
        let status = LogsService::export(&sink, request(token.as_deref()))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }
    Ok(())
}

#[tokio::test]
async fn exports_store_the_verified_identity_instead_of_the_payload() -> anyhow::Result<()> {
    let issuer = issuer()?;
    let (sink, store) = sink(&issuer)?;
    let host = issuer.issue_host(
        "org",
        &HostId::new("host.v3.r1.one"),
        &uuid::Uuid::new_v4().to_string(),
        "r1",
        "us-east",
        &actor(),
    )?;
    LogsService::export(&sink, request(Some(&host.token))).await?;
    assert!(
        store
            .logs("victim", &LogQuery::default())
            .await?
            .records
            .is_empty()
    );
    let stored = store.logs("default", &LogQuery::default()).await?.records;
    assert_eq!(stored.len(), 1);
    assert_eq!(
        (stored[0].actor_name.as_str(), stored[0].actor_id.as_str()),
        ("Counter", "one")
    );
    assert_eq!(stored[0].host_id, "host.v3.r1.one");
    assert_eq!(stored[0].message, "hello");
    Ok(())
}

fn issuer() -> anyhow::Result<ActorJwtIssuer> {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(pkcs8.as_ref()),
        "key",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(1800),
    )
}

fn sink(
    issuer: &ActorJwtIssuer,
) -> anyhow::Result<(LocalTelemetrySink, Arc<SqliteTracePersistence>)> {
    let store = Arc::new(SqliteTracePersistence::in_memory());
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        TELEMETRY_AUDIENCE,
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(1800),
    )?;
    Ok((
        LocalTelemetrySink::new(auth, store.clone(), InProcessFeed::default()),
        store,
    ))
}

fn actor() -> ActorKey {
    ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    }
}

fn request(token: Option<&str>) -> Request<ExportLogsServiceRequest> {
    let attribute = |key: &str, value: &str| KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..Default::default()
    };
    let mut request = Request::new(ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    attribute(otlp::PROJECT_ID, "victim"),
                    attribute(otlp::ACTOR_NAME, "Forged"),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_000_000,
                    severity_text: "info".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("hello".into())),
                    }),
                    attributes: vec![attribute(otlp::LOG_ID, "log-1")],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    });
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    request
}
