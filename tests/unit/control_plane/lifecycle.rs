use super::*;
use crate::actor_lifecycle::{
    LifecycleEvent, LifecycleKind, LifecyclePersistence, LifecycleReport, LifecycleStore,
};

#[tokio::test]
async fn lifecycle_reports_use_authenticated_identity() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let host = HostId::new("host.v3.test.one");
    let session = uuid::Uuid::new_v4().to_string();
    let token = issuer
        .issue_host("org-test", &host, &session, "test", "us-east", &actor)?
        .token;
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let mut service = ControlPlaneService::new(
        Arc::new(NoOwner),
        auth,
        Arc::new(LocalAdminRegistry::default()),
        issuer,
        Arc::new(UnavailableProvisioner),
    );
    service.lifecycle = LifecycleStore::new(Arc::new(Persistence(sent)));
    for (sequence, kind) in [
        (1, LifecycleKind::Started),
        (2, LifecycleKind::Draining),
        (3, LifecycleKind::Stopped),
    ] {
        let mut request = Request::new(super::super::super::protocol::encode_command(
            ControlPlaneCommand::Lifecycle {
                report: LifecycleReport {
                    kind,
                    owner_epoch: 1,
                    sequence,
                    started_at_ms: 1000,
                    occurred_at_ms: sequence * 1000,
                    resources: Default::default(),
                    resident: Some(kind != LifecycleKind::Stopped),
                    last_active_at_ms: None,
                    database_bytes: None,
                    waiting: Vec::new(),
                },
            },
        )?);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse()?);
        let response = service.execute(request).await?;
        assert!(matches!(
            super::super::super::protocol::decode_reply(response.into_inner())?,
            ControlPlaneCommandReply::Unit
        ));
        let event = received.try_recv()?;
        assert_eq!(event.actor, actor);
        assert_eq!(event.organization_id.as_deref(), Some("org-test"));
        assert_eq!(event.host_id, host.as_str());
        assert_eq!(event.session_id, session);
        assert_eq!(event.region, "us-east");
        assert_eq!(event.event_id, format!("{session}:{sequence}"));
        assert_eq!(event.report.kind, kind);
        assert!(event.received_at_ms > 0);
    }
    Ok(())
}

struct Persistence(tokio::sync::mpsc::UnboundedSender<LifecycleEvent>);

#[async_trait]
impl LifecyclePersistence for Persistence {
    async fn record(&self, event: &LifecycleEvent) -> Result<()> {
        self.0.send(event.clone())?;
        Ok(())
    }

    async fn latest(&self, _: &str) -> Result<Vec<LifecycleEvent>> {
        unreachable!("this test only records reports")
    }
}

struct NoOwner;

#[async_trait]
impl ObjectPlacementStore for NoOwner {
    async fn get_owner_with_hint(
        &self,
        _: &ActorStorageKey,
    ) -> Result<(Option<ObjectPlacement>, Option<OwnershipHint>)> {
        Ok((None, None))
    }

    async fn get(&self, _: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        unreachable!("lifecycle telemetry must not read ownership")
    }
}
