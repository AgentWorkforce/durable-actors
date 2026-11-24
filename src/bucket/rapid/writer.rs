use super::*;
use futures_util::FutureExt;
use std::sync::Weak;
use tokio::time::Instant;

type Preparing = AbortOnDropHandle<Result<Option<Segment>>>;

pub(super) struct Session {
    pub stream: StateStream,
    segment: Option<Segment>,
    preparing: Option<Preparing>,
    next: Option<Preparing>,
    latest: Option<(SnapshotRef, Option<Payload>)>,
    version: u64,
    /// The version the current segment opened after; equal to `version` while it holds no frames.
    installed_at: u64,
    standard: bool,
    healthy: bool,
    rotated: Instant,
    checkpoint: checkpoint::Progress,
}

impl Session {
    pub fn open(
        storage: Arc<LogStorage>,
        stream: StateStream,
        resume: Option<(&SnapshotRef, CheckpointProgress)>,
    ) -> Result<Self> {
        let version = stream.base_version;
        let preparing = prepare(
            storage,
            stream.clone(),
            version.checked_add(1).context("state version overflow")?,
            None,
        );
        Ok(Self {
            stream,
            preparing: Some(preparing),
            segment: None,
            next: None,
            latest: resume.map(|(head, _)| (head.clone(), None)),
            version,
            installed_at: version,
            standard: false,
            healthy: true,
            rotated: Instant::now(),
            checkpoint: checkpoint::Progress::resume(
                resume.map(|(_, p)| p).unwrap_or_default(),
                version,
            ),
        })
    }

    pub fn cached(&self, object: &str) -> Option<Payload> {
        self.latest
            .as_ref()
            .filter(|(head, _)| head.object == object)
            .and_then(|(_, bytes)| bytes.clone())
    }

    pub async fn put(
        &mut self,
        storage: Arc<LogStorage>,
        object: &str,
        bytes: Payload,
    ) -> Result<()> {
        let started = Instant::now();
        ensure!(self.healthy, "uncertain write requires activation recovery");
        let reference = self.stream.snapshot(&bytes)?;
        ensure!(
            reference.object == object,
            "snapshot belongs to another stream"
        );
        if self.cached(object).as_ref() == Some(&bytes) {
            return Ok(());
        }
        ensure!(
            self.version.checked_add(1) == Some(reference.state_version),
            "nonconsecutive state version"
        );
        self.rotate(storage.clone()).await;
        self.healthy = false;
        if let Some(preparing) = self.preparing.take() {
            self.install(preparing.await.context("Rapid preparation failed")??);
        }
        if self.standard {
            bounded_transfer(
                bytes.len() as u64,
                storage.snapshots.put(object, bytes.clone()),
            )
            .await?;
        } else {
            let frame = self
                .segment
                .as_mut()
                .context("Rapid stream missing")?
                .append(reference.state_version, bytes.clone())
                .await?;
            storage.remember(
                self.segment.as_ref().unwrap(),
                reference.state_version,
                frame.len(),
            );
        }
        self.version = reference.state_version;
        let size = bytes.len();
        self.checkpoint.logged(size as u64);
        self.latest = Some((reference, Some(bytes)));
        self.healthy = true;
        tracing::info!(
            event = "rapid_log_snapshot",
            state_version = self.version,
            owner_epoch = self.stream.owner_epoch,
            ack_zones = self
                .segment
                .as_ref()
                .map_or(0, |s| s.manifest.replicas.len()),
            bytes = size,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }

    pub async fn restored(
        &mut self,
        snapshot: &crate::storage::VerifiedSnapshot,
        progress: CheckpointProgress,
        base: Option<super::base::BaseName>,
    ) -> Result<()> {
        let object = &snapshot.reference.object;
        if self
            .latest
            .as_ref()
            .is_some_and(|(_, bytes)| bytes.is_some())
            || snapshot.reference.state_version != self.stream.base_version
            || crate::storage_paths::actor_from_snapshot(object)?
                != crate::storage_paths::actor_from_snapshot(&self.stream.object(self.version))?
        {
            return Ok(());
        }
        self.latest = Some((snapshot.reference.clone(), Some(snapshot.bytes.clone())));
        self.checkpoint.restored(progress, base);
        Ok(())
    }

    fn install(&mut self, segment: Option<Segment>) {
        self.standard = segment.is_none();
        self.installed_at = self.version;
        self.segment = segment;
    }

    pub async fn rotate(&mut self, storage: Arc<LogStorage>) {
        self.install_prepared().await;
        if !self.healthy
            || self.standard
            || self.segment.is_none()
            || self.version == self.installed_at
            || self.rotated.elapsed() < ROTATION_INTERVAL
        {
            return;
        }
        let Some(first_version) = self.version.checked_add(1) else {
            return;
        };
        // Rotations stay in the zones the session was placed in.
        let placed = self.segment.as_ref().map(|segment| segment.zones.clone());
        let next = self.next.get_or_insert_with(|| {
            prepare(storage.clone(), self.stream.clone(), first_version, placed)
        });
        let Some(result) = next.now_or_never() else {
            return;
        };
        self.next = None;
        match result {
            Ok(Ok(Some(segment))) => self.install(Some(segment)),
            Ok(Ok(None)) => {}
            Ok(Err(error)) => tracing::warn!(%error, "Rapid rotation preparation deferred"),
            Err(error) => tracing::warn!(%error, "Rapid rotation preparation failed"),
        }
        self.rotated = Instant::now();
    }

    /// Installs a finished preparation before the first write, so an idle session's segment
    /// ages, and rotates, like any other.
    async fn install_prepared(&mut self) {
        if !self.preparing.as_ref().is_some_and(|p| p.is_finished()) {
            return;
        }
        let error = match self.preparing.take().unwrap().await {
            Ok(Ok(segment)) => return self.install(segment),
            Ok(Err(error)) => error,
            Err(error) => error.into(),
        };
        tracing::warn!(error = %format!("{error:#}"), "Rapid preparation failed");
        self.healthy = false;
    }

    /// Seals the session: the head's snapshot becomes its own object, and the progress says
    /// whether a base of it is due.
    pub async fn finish(
        &mut self,
        storage: Arc<LogStorage>,
        deadline: Instant,
    ) -> Result<CheckpointProgress> {
        ensure!(self.healthy, "uncertain write cannot be sealed");
        self.healthy = false;
        let started = Instant::now();
        tokio::time::timeout_at(deadline, async {
            self.seal_head(storage.clone()).await?;
            self.find_base(&storage).await;
            anyhow::Ok(())
        })
        .await
        .context("head seal timed out")??;
        let progress = self.checkpoint.seal(self.version);
        tracing::info!(
            event = "rapid_log_sealed",
            state_version = self.version,
            base_version = progress.version,
            log_bytes = progress.log_bytes,
            base_due = progress.base_due,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        self.segment = None;
        self.next = None;
        self.healthy = true;
        Ok(progress)
    }

    /// A session that reopened a parked database never listed the actor's bases, so it lists them
    /// before deciding whether one is due.
    async fn find_base(&mut self, storage: &LogStorage) {
        let Some((head, _)) = self.latest.as_ref().filter(|_| !self.checkpoint.based()) else {
            return;
        };
        match storage.newest_base(head).await {
            Ok(base) => self.checkpoint.found(base.map(|base| base.name)),
            Err(error) => tracing::warn!(error = %format!("{error:#}"),
                "SQLite bases unavailable; a base is due"),
        }
    }

    async fn seal_head(&mut self, storage: Arc<LogStorage>) -> Result<()> {
        if let Some(preparing) = self.preparing.take() {
            self.install(preparing.await.context("Rapid preparation failed")??);
        }
        if let Some((head, Some(bytes))) = &self.latest {
            bounded_transfer(
                bytes.len() as u64,
                storage.snapshots.put(&head.object, bytes.clone()),
            )
            .await?;
        }
        Ok(())
    }
}

fn prepare(
    storage: Arc<LogStorage>,
    stream: StateStream,
    first: u64,
    placed: Option<Vec<Arc<dyn LogZone>>>,
) -> Preparing {
    AbortOnDropHandle::new(tokio::spawn(async move {
        Segment::open(&storage, stream, first, placed).await
    }))
}

pub(super) fn start_rotation(
    storage: Arc<LogStorage>,
    session: Weak<Mutex<Option<Session>>>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            let Some(session) = session.upgrade() else {
                return;
            };
            if let Some(session) = session.lock().await.as_mut() {
                session.rotate(storage.clone()).await;
            }
        }
    });
}
