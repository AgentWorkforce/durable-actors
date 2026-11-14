use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use tokio_util::sync::CancellationToken;

use super::{
    HostId,
    actor_database::{ActorActivation, ActorStorage},
};
use crate::{
    actor::ActorKey,
    bucket::{
        Bucket, BucketObject, BucketSnapshots, GcsBucket, RapidSnapshots, RuntimeStorage, S3Bucket,
        SnapshotStore, WarmStorage,
        access::{CheckpointLocation, HostStorageConfig, StorageToken, StorageTokens},
        checkpoint::{CheckpointedBucket, Checkpointer, Mirror},
    },
    clock::{Clock, SystemClock},
    control_plane::{ControlPlaneClient, LeaseFence},
    host_leases::{HostLease, HostLeaseRegistry, HostLeaseRequest},
    storage::WritePlan,
};

pub(crate) struct HostStorage {
    pub runtime: Arc<RuntimeStorage>,
    objects: Option<CodeObjects>,
    /// Copies this actor's writes to the regional checkpoint bucket, when one is configured.
    checkpointer: Option<Arc<Checkpointer>>,
    pub(super) stop: CancellationToken,
    observer: Arc<ControlPlaneClient>,
    host: HostId,
    session: String,
    region: String,
    actor: ActorKey,
    new_actor: bool,
    owner_hint: Option<crate::bucket::OwnershipHint>,
    activation: Mutex<Option<ActorActivation>>,
    checkpoint: Mutex<Option<crate::bucket::recovery::PrefetchedCheckpoint>>,
    fence: Mutex<LeaseFence>,
    lease: Mutex<Option<HostLease>>,
    renewal: tokio::sync::Mutex<()>,
}

impl HostStorage {
    pub async fn new(
        config: HostStorageConfig,
        host: HostId,
        session: String,
        client: Arc<ControlPlaneClient>,
        stop: CancellationToken,
        warm: Option<WarmStorage>,
        actor: ActorKey,
    ) -> Result<Self> {
        let credentials = HostCredentials::new(config.tokens, client.clone(), stop.clone());
        let mut checkpointer = None;
        let (runtime, objects) = match config.bucket {
            crate::bucket::access::BucketLocation::Gcs { bucket, .. } => {
                Self::open_gcs(
                    &bucket,
                    config.persistence,
                    credentials,
                    warm,
                    &stop,
                    &actor,
                )
                .await?
            }
            crate::bucket::access::BucketLocation::S3 {
                bucket,
                artifact_bucket,
                s3,
                checkpoint,
            } => {
                ensure!(
                    config.persistence.rapid_settings().is_none(),
                    "Rapid persistence requires GCS"
                );
                let (runtime, objects, checkpoints) = Self::open_s3(
                    &bucket,
                    &artifact_bucket,
                    &s3,
                    checkpoint.as_ref(),
                    config.persistence,
                    credentials,
                    &stop,
                    &actor,
                )?;
                checkpointer = checkpoints;
                (runtime, objects)
            }
            crate::bucket::access::BucketLocation::File { directory } => {
                ensure!(
                    config.persistence.rapid_settings().is_none(),
                    "Rapid persistence requires GCS"
                );
                let authority = Arc::new(crate::bucket::FileBucket::new(directory)?);
                (RuntimeStorage::new(authority, Arc::new(SystemClock))?, None)
            }
        };
        Ok(Self {
            objects,
            checkpointer,
            observer: client,
            stop,
            runtime: Arc::new(runtime),
            host,
            session,
            region: config.region,
            actor,
            new_actor: false,
            owner_hint: None,
            activation: Mutex::new(None),
            checkpoint: Mutex::new(None),
            fence: Mutex::new(LeaseFence::default()),
            lease: Mutex::new(None),
            renewal: tokio::sync::Mutex::new(()),
        })
    }

    async fn open_gcs(
        bucket: &str,
        persistence: crate::bucket::SnapshotStoreConfig,
        credentials: HostCredentials,
        warm: Option<WarmStorage>,
        stop: &CancellationToken,
        actor: &ActorKey,
    ) -> Result<(RuntimeStorage, Option<CodeObjects>)> {
        let ownership = credentials.scoped(|tokens| &tokens.ownership);
        let state = credentials.scoped(|tokens| &tokens.state);
        let (authority, state) = match warm {
            Some(warm) => (
                warm.ownership.bind(bucket, ownership)?,
                warm.state.bind(bucket, state)?,
            ),
            None => tokio::try_join!(
                GcsBucket::with_credentials(bucket, ownership),
                GcsBucket::with_credentials(bucket, state),
            )?,
        };
        let snapshots: Arc<dyn SnapshotStore> = match persistence.rapid_settings() {
            Some(_) => Arc::new(RapidSnapshots::gcs(
                &persistence,
                state.clients(),
                stop.clone(),
                Some(actor),
            )?),
            None => Arc::new(BucketSnapshots(Arc::new(state))),
        };
        let objects = authority.clients().storage;
        let runtime = RuntimeStorage::new(Arc::new(authority), Arc::new(SystemClock))?
            .with_persistence(persistence, snapshots)?;
        Ok((runtime, Some(CodeObjects::Gcs(Box::new(objects)))))
    }

    #[allow(clippy::too_many_arguments)]
    fn open_s3(
        bucket: &str,
        artifact_bucket: &str,
        endpoint: &crate::bucket::S3Endpoint,
        checkpoint: Option<&CheckpointLocation>,
        persistence: crate::bucket::SnapshotStoreConfig,
        credentials: HostCredentials,
        stop: &CancellationToken,
        actor: &ActorKey,
    ) -> Result<(
        RuntimeStorage,
        Option<CodeObjects>,
        Option<Arc<Checkpointer>>,
    )> {
        let ownership = credentials.aws_scoped(|tokens| &tokens.ownership);
        let state = credentials.aws_scoped(|tokens| &tokens.state);
        let primary = Arc::new(ByScope {
            snapshots: crate::storage_paths::snapshots(actor)?,
            ownership: Arc::new(S3Bucket::with_credentials(
                bucket,
                endpoint,
                ownership.clone(),
            )?),
            state: Arc::new(S3Bucket::with_credentials(bucket, endpoint, state.clone())?),
        });
        let code = Arc::new(S3Bucket::with_credentials(
            artifact_bucket,
            endpoint,
            ownership.clone(),
        )?);
        let (authority, state, checkpointer): (Arc<dyn Bucket>, Arc<dyn Bucket>, _) =
            match checkpoint {
                None => (primary.ownership.clone(), primary.state.clone(), None),
                Some(checkpoint) => {
                    // One queue for both scopes keeps snapshots ahead of the owner records that
                    // point at them; each object is copied with the credentials of its scope.
                    let copy = Arc::new(ByScope {
                        snapshots: primary.snapshots.clone(),
                        ownership: Arc::new(S3Bucket::with_credentials(
                            &checkpoint.bucket,
                            endpoint,
                            ownership,
                        )?),
                        state: Arc::new(S3Bucket::with_credentials(
                            &checkpoint.bucket,
                            endpoint,
                            state,
                        )?),
                    });
                    let checkpointer =
                        Checkpointer::spawn(primary.clone(), copy, checkpoint.config(), stop);
                    let reconcile = checkpointer.clone();
                    let snapshots = primary.snapshots.clone();
                    tokio::spawn(async move {
                        if let Err(error) = reconcile.reconcile(&snapshots).await {
                            tracing::warn!(
                                event = "storage_checkpoint_reconcile_failed",
                                error = format!("{error:#}"),
                            );
                        }
                    });
                    (
                        Arc::new(CheckpointedBucket::new(
                            primary.ownership.clone(),
                            checkpointer.clone(),
                        )),
                        Arc::new(CheckpointedBucket::new(
                            primary.state.clone(),
                            checkpointer.clone(),
                        )),
                        Some(checkpointer),
                    )
                }
            };
        let runtime = RuntimeStorage::new(authority, Arc::new(SystemClock))?
            .with_persistence(persistence, Arc::new(BucketSnapshots(state)))?;
        Ok((runtime, Some(CodeObjects::Bucket(code)), checkpointer))
    }

    pub(crate) async fn install_code(&self, artifact: &crate::bundle::ActorBundle) -> Result<()> {
        let root = std::path::Path::new("/customer");
        match self.objects.as_ref().context("artifact client missing")? {
            CodeObjects::Gcs(storage) => artifact.install(root, storage).await,
            CodeObjects::Bucket(bucket) => {
                artifact.install_from_bucket(root, bucket.as_ref()).await
            }
        }
    }

    pub(crate) fn with_activation(
        mut self,
        new_actor: bool,
        owner_hint: Option<crate::bucket::OwnershipHint>,
    ) -> Self {
        self.new_actor = new_actor;
        self.owner_hint = owner_hint;
        self
    }

    pub(super) fn current_lease(&self) -> Result<HostLease> {
        self.ensure_authority()?;
        self.lease
            .lock()
            .unwrap()
            .clone()
            .context("activation lease missing")
    }

    pub(super) async fn get_alarm(&self) -> Result<Option<i64>> {
        self.ensure_authority()?;
        let timestamp_ms = self.observer.get_alarm().await?;
        self.ensure_authority()?;
        Ok(timestamp_ms)
    }

    pub(super) async fn update_alarm(&self, timestamp_ms: Option<i64>) -> Result<()> {
        self.ensure_authority()?;
        self.observer.update_alarm(timestamp_ms).await?;
        self.ensure_authority()
    }

    pub(super) async fn finish(&self, deadline: tokio::time::Instant) -> Result<()> {
        let started = Instant::now();
        let actor = &self.actor;
        let drain_deadline = deadline - Duration::from_secs(3);
        let completed = tokio::time::timeout_at(
            drain_deadline,
            self.runtime
                .drain_activation(actor, &self.host, &self.session, drain_deadline),
        )
        .await
        .context("snapshot drain timed out")
        .and_then(|result| result);
        let checkpoint = match completed {
            Ok(checkpoint) => Some(checkpoint),
            Err(error) => {
                tracing::warn!(%error, "clean session shutdown deferred to recovery");
                None
            }
        };
        let sealed = checkpoint.is_some();
        let storage_ms = started.elapsed().as_secs_f64() * 1000.0;
        let release_started = Instant::now();
        self.fence.lock().unwrap().fenced = true;
        // Ownership CAS prevents an in-flight renewal from reviving the released lease.
        let released = tokio::time::timeout_at(
            deadline,
            self.runtime
                .release_with_checkpoint(actor, &self.host, &self.session, checkpoint),
        )
        .await
        .context("ownership release timed out")
        .and_then(|result| result);
        let checkpointed = match &self.checkpointer {
            Some(checkpointer) => Some(checkpointer.flush(deadline).await),
            None => None,
        };
        tracing::info!(
            event = "host_storage_shutdown",
            checkpointed,
            sealed = sealed && released.is_ok(),
            released = released.is_ok(),
            error = released.as_ref().err().map(|error| format!("{error:#}")),
            storage_ms,
            release_ms = release_started.elapsed().as_secs_f64() * 1000.0,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        released
    }
}

#[async_trait]
impl ActorStorage for HostStorage {
    async fn get_alarm(&self) -> Result<Option<i64>> {
        HostStorage::get_alarm(self).await
    }

    async fn update_alarm(&self, timestamp_ms: Option<i64>) -> Result<()> {
        HostStorage::update_alarm(self, timestamp_ms).await
    }

    fn ensure_authority(&self) -> Result<()> {
        ensure!(
            !self.stop.is_cancelled(),
            "host session is permanently fenced"
        );
        self.fence
            .lock()
            .map_err(|_| anyhow::anyhow!("lease fence poisoned"))?
            .check(Instant::now())
    }

    async fn acquire_actor(&self) -> Result<ActorActivation> {
        self.ensure_authority()?;
        if let Some(activation) = self.activation.lock().unwrap().take() {
            return Ok(activation);
        }
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let loaded = self
            .runtime
            .activate_actor(&self.actor, &lease, &self.region)
            .await?;
        self.ensure_authority()?;
        Ok(ActorActivation {
            owner_epoch: loaded.placement.owner_epoch,
            state_version: loaded.placement.state_version,
            state: loaded.state,
        })
    }

    async fn load_actor_state(&self, epoch: u64) -> Result<(u64, bytes::Bytes)> {
        self.ensure_authority()?;
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let loaded = self
            .runtime
            .load_owned_actor(&self.actor, &lease, epoch)
            .await?;
        self.ensure_authority()?;
        Ok((
            loaded.placement.state_version,
            loaded.state.unwrap_or_default(),
        ))
    }

    async fn read_sqlite(&self, bytes: &[u8]) -> Result<crate::state_log::SqliteSnapshot> {
        self.ensure_authority()?;
        let snapshot = crate::state_log::StateSnapshot::decode(bytes)?;
        let object = crate::storage::snapshot_object_name(
            &self.actor,
            snapshot.state_version,
            &format!("{:032x}", snapshot.owner_epoch),
        )?;
        let checkpoint = self.checkpoint.lock().unwrap().take();
        self.runtime
            .read_sqlite_prefetched(&object, bytes::Bytes::copy_from_slice(bytes), checkpoint)
            .await
    }

    async fn prepare_state_write(&self, epoch: u64, version: u64) -> Result<WritePlan> {
        self.ensure_authority()?;
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let ticket = self
            .runtime
            .prepare_actor_write(
                &self.actor,
                &lease,
                epoch,
                version.checked_add(1).context("state version overflow")?,
            )
            .await?;
        self.ensure_authority()?;
        Ok(ticket)
    }
}

#[async_trait]
impl HostLeaseRegistry for HostStorage {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        ensure!(
            request.id == self.host && request.session_id == self.session,
            "host lease scope mismatch"
        );
        let _renewal = self.renewal.lock().await;
        let started = Instant::now();
        self.fence.lock().unwrap().begin(started)?;
        let actor = &self.actor;
        let first = self.lease.lock().unwrap().is_none();
        let lease = if first {
            let loaded = self
                .runtime
                .register_activation(
                    actor,
                    request,
                    &self.region,
                    self.new_actor,
                    self.owner_hint.as_ref(),
                )
                .await?;
            let lease = loaded.placement.lease;
            *self.checkpoint.lock().unwrap() = loaded.checkpoint;
            *self.activation.lock().unwrap() = Some(ActorActivation {
                owner_epoch: loaded.placement.owner_epoch,
                state_version: loaded.placement.state_version,
                state: loaded.state,
            });
            lease
        } else {
            self.runtime.renew_activation(actor, request).await?
        };
        self.fence.lock().unwrap().confirm(
            started,
            Duration::from_millis(request.duration_ms),
            Instant::now(),
        )?;
        self.lease.lock().unwrap().replace(lease.clone());
        Ok(lease)
    }
    async fn unregister(&self, host: &HostId, session: &str) -> Result<()> {
        ensure!(
            *host == self.host && session == self.session,
            "host lease scope mismatch"
        );
        self.finish(tokio::time::Instant::now() + Duration::from_secs(25))
            .await
    }
}

/// Where a host downloads its compiled code from.
enum CodeObjects {
    Gcs(Box<google_cloud_storage::client::Storage>),
    Bucket(Arc<S3Bucket>),
}

#[derive(Clone)]
struct HostCredentials {
    tokens: Arc<std::sync::RwLock<Option<StorageTokens>>>,
    scope: fn(&StorageTokens) -> &StorageToken,
}
impl std::fmt::Debug for HostCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HostCredentials")
    }
}
impl HostCredentials {
    fn new(
        tokens: Option<StorageTokens>,
        client: Arc<ControlPlaneClient>,
        stop: CancellationToken,
    ) -> Self {
        let credentials = Self {
            tokens: Arc::new(std::sync::RwLock::new(tokens)),
            scope: |tokens| &tokens.ownership,
        };
        let refreshed = credentials.tokens.clone();
        tokio::spawn(async move {
            loop {
                let now = SystemClock.now_ms().unwrap_or(u64::MAX);
                let host_expiry = client.token_expires_at_ms().unwrap_or(now);
                let expires = refreshed
                    .read()
                    .unwrap()
                    .as_ref()
                    .map_or(host_expiry, |tokens| {
                        tokens
                            .ownership
                            .expires_at_ms
                            .min(tokens.state.expires_at_ms)
                            .min(host_expiry)
                    });
                let delay = Duration::from_millis(
                    expires
                        .saturating_sub(now.saturating_add(30_000))
                        .clamp(1_000, 600_000),
                );
                tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(delay) => {} }
                match client.refresh_storage_access().await {
                    Ok(tokens) => {
                        *refreshed.write().unwrap() = tokens;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "could not refresh host storage credentials")
                    }
                }
            }
        });
        credentials
    }

    fn aws_scoped(
        &self,
        scope: fn(&StorageTokens) -> &StorageToken,
    ) -> aws_credential_types::provider::SharedCredentialsProvider {
        aws_credential_types::provider::SharedCredentialsProvider::new(Self {
            tokens: self.tokens.clone(),
            scope,
        })
    }

    fn aws_credentials(&self) -> aws_credential_types::provider::Result {
        use aws_credential_types::provider::error::CredentialsError;
        let stored = self.tokens.read().unwrap();
        let token = stored
            .as_ref()
            .map(self.scope)
            .ok_or_else(|| CredentialsError::not_loaded("S3 credentials missing"))?;
        let aws = token
            .aws
            .as_ref()
            .ok_or_else(|| CredentialsError::not_loaded("storage credential is not for S3"))?;
        Ok(aws_credential_types::Credentials::new(
            aws.access_key_id.clone(),
            aws.secret_access_key.clone(),
            Some(aws.session_token.clone()).filter(|token| !token.is_empty()),
            Some(std::time::UNIX_EPOCH + Duration::from_millis(token.expires_at_ms)),
            "durable-actors-host",
        ))
    }

    fn scoped(&self, scope: fn(&StorageTokens) -> &StorageToken) -> Credentials {
        Self {
            tokens: self.tokens.clone(),
            scope,
        }
        .into()
    }
}
impl aws_credential_types::provider::ProvideCredentials for HostCredentials {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::ready(self.aws_credentials())
    }
}

impl CredentialsProvider for HostCredentials {
    async fn headers(
        &self,
        _: axum::http::Extensions,
    ) -> std::result::Result<
        CacheableResource<axum::http::HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        let stored = self.tokens.read().unwrap();
        let token = stored.as_ref().map(self.scope).ok_or_else(|| {
            google_cloud_auth::errors::CredentialsError::from_msg(false, "GCS credentials missing")
        })?;
        let mut headers = axum::http::HeaderMap::new();
        let mut value: axum::http::HeaderValue = format!("Bearer {}", token.access_token)
            .parse()
            .map_err(|_| {
            google_cloud_auth::errors::CredentialsError::from_msg(
                false,
                "invalid storage credential",
            )
        })?;
        value.set_sensitive(true);
        headers.insert(axum::http::header::AUTHORIZATION, value);
        Ok(CacheableResource::New {
            entity_tag: EntityTag::new(),
            data: headers,
        })
    }
    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".into())
    }
}

/// Routes each key to the client whose scoped credentials cover it: the actor's snapshots to the
/// state client, everything else (the owner record) to the ownership client.
struct ByScope<B> {
    snapshots: String,
    ownership: Arc<B>,
    state: Arc<B>,
}

impl<B> ByScope<B> {
    fn client(&self, key: &str) -> &Arc<B> {
        if key.starts_with(&self.snapshots) {
            &self.state
        } else {
            &self.ownership
        }
    }
}

#[async_trait]
impl<B: Bucket + 'static> Bucket for ByScope<B> {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.client(key).get(key).await
    }

    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        self.client(key).range(key, start, length).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: bytes::Bytes,
    ) -> Result<bool> {
        self.client(key)
            .compare_and_swap(key, generation, bytes)
            .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.client(prefix).list(prefix).await
    }
}

#[async_trait]
impl<B: Mirror + 'static> Mirror for ByScope<B> {
    async fn mirror(&self, key: &str, generation: i64, bytes: bytes::Bytes) -> Result<()> {
        self.client(key).mirror(key, generation, bytes).await
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/storage.rs"]
mod tests;
