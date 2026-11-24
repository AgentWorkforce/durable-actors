//! Deletes history no restore can need, inside the pool manager. Versions only grow across owner
//! epochs, so everything in an epoch older than a base's epoch lies below that base. The sweep
//! keeps the newest base older than the observer's lookback, so every version inside it still has
//! a base below it, and deletes the history and bases below that one once they are older too.

use super::{
    base::{self, BaseName},
    *,
};
use std::time::{Instant, SystemTime};

const SNAPSHOTS: &str = "durable-actors-v3-snapshots-";

pub struct SweepConfig {
    pub archive_bucket: String,
    pub rapid_buckets: Vec<crate::bucket::RapidBucket>,
    pub min_age: Duration,
    pub interval: Duration,
    pub dry_run: bool,
}

impl SweepConfig {
    /// The sweep of a Rapid installation's buckets, timed by the `DURABLE_ACTORS_SWEEP_*` settings.
    pub fn new(
        archive_bucket: String,
        rapid_buckets: Vec<crate::bucket::RapidBucket>,
        mut get: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self> {
        let number = |value: Option<String>, name: &str, default: u64| -> Result<u64> {
            value
                .map(|value| value.parse())
                .transpose()
                .with_context(|| format!("{name} must be an integer"))
                .map(|value| value.unwrap_or(default))
        };
        Ok(Self {
            archive_bucket,
            rapid_buckets,
            min_age: Duration::from_secs(
                3600 * number(
                    get("DURABLE_ACTORS_SWEEP_MIN_AGE_HOURS"),
                    "DURABLE_ACTORS_SWEEP_MIN_AGE_HOURS",
                    30 * 24,
                )?,
            ),
            interval: Duration::from_secs(number(
                get("DURABLE_ACTORS_SWEEP_INTERVAL_SECONDS"),
                "DURABLE_ACTORS_SWEEP_INTERVAL_SECONDS",
                3600,
            )?),
            dry_run: get("DURABLE_ACTORS_SWEEP_DRY_RUN")
                .map(|s| s.parse::<bool>())
                .transpose()?
                .unwrap_or(true),
        })
    }
}

/// An archive object as a listing describes it.
pub(super) struct Listed {
    pub name: String,
    pub created: SystemTime,
    pub bytes: u64,
}

#[async_trait]
pub(super) trait SweepStore: Send + Sync {
    /// One page of the archive's objects under `prefix`, after `page`, and the next page's token,
    /// empty after the last.
    async fn page(&self, prefix: &str, page: &str) -> Result<(Vec<Listed>, String)>;
    async fn get(&self, name: &str) -> Result<Option<Payload>>;
    async fn remember(&self, name: &str, bytes: Payload) -> Result<()>;
    fn allows_replica(&self, bucket: &str) -> bool;
    /// Deletes an object of `bucket`, the archive when `None`; a missing object is deleted.
    async fn delete(&self, bucket: Option<&str>, name: &str, generation: Option<i64>)
    -> Result<()>;
}

#[derive(Default, Debug, PartialEq, Eq)]
pub(super) struct Summary {
    pub actors: u64,
    pub objects: u64,
    pub bytes: u64,
    pub failed: u64,
}

/// Sweeps every interval until `shutdown`, which also cuts a pass short: deletions are journaled,
/// so the next pass finishes what a cut one started. A store that fails to open is retried then.
pub async fn serve(config: SweepConfig, shutdown: impl Future<Output = ()>) -> Result<()> {
    tokio::pin!(shutdown);
    let store = tokio::sync::OnceCell::new();
    loop {
        let started = Instant::now();
        let pass = async {
            let store = store
                .get_or_try_init(|| GcsSweep::new(&config.archive_bucket, &config.rapid_buckets))
                .await?;
            sweep(store, SystemTime::now() - config.min_age, config.dry_run).await
        };
        let swept = tokio::select! {
            () = &mut shutdown => return Ok(()),
            swept = pass => swept,
        };
        match swept {
            Ok(summary) => tracing::info!(
                event = "history_sweep",
                actors = summary.actors,
                objects = summary.objects,
                bytes = summary.bytes,
                failed = summary.failed,
                dry_run = config.dry_run,
                duration_ms = started.elapsed().as_secs_f64() * 1000.0
            ),
            Err(error) => {
                tracing::warn!(event = "history_sweep_failed", error = %format!("{error:#}"))
            }
        }
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            () = tokio::time::sleep(config.interval) => {}
        }
    }
}

/// One pass over the archive, an actor at a time: its objects are adjacent in the listing.
pub(super) async fn sweep(
    store: &dyn SweepStore,
    cutoff: SystemTime,
    dry_run: bool,
) -> Result<Summary> {
    let mut summary = Summary::default();
    let mut actor: Vec<Listed> = Vec::new();
    let mut page = String::new();
    loop {
        let (objects, next) = store.page(SNAPSHOTS, &page).await?;
        for object in objects {
            if actor
                .first()
                .is_some_and(|first| actor_of(&first.name) != actor_of(&object.name))
            {
                let done = std::mem::take(&mut actor);
                sweep_or_skip(store, &done, cutoff, dry_run, &mut summary).await;
            }
            actor.push(object);
        }
        if next.is_empty() {
            break;
        }
        page = next;
    }
    sweep_or_skip(store, &actor, cutoff, dry_run, &mut summary).await;
    Ok(summary)
}

/// An actor whose history fails to sweep is logged and retried next pass, never stopping this one.
async fn sweep_or_skip(
    store: &dyn SweepStore,
    objects: &[Listed],
    cutoff: SystemTime,
    dry_run: bool,
    summary: &mut Summary,
) {
    if let Err(error) = sweep_actor(store, objects, cutoff, dry_run, summary).await {
        summary.failed += 1;
        tracing::warn!(
            event = "history_sweep_actor_failed",
            actor = objects.first().map_or("", |object| actor_of(&object.name)),
            error = %format!("{error:#}")
        );
    }
}

async fn sweep_actor(
    store: &dyn SweepStore,
    objects: &[Listed],
    cutoff: SystemTime,
    dry_run: bool,
    summary: &mut Summary,
) -> Result<()> {
    if objects.is_empty() {
        return Ok(());
    }
    summary.actors += 1;
    for object in plan(objects, cutoff) {
        summary.objects += 1;
        summary.bytes += object.bytes;
        tracing::info!(
            event = "history_sweep_object",
            key = object.name,
            bytes = object.bytes,
            dry_run
        );
        if object.name.ends_with(".manifest") || object.name.ends_with(".manifest.sweeping") {
            sweep_segment(store, &object.name, dry_run).await?;
        } else if !dry_run {
            store.delete(None, &object.name, None).await?;
        }
    }
    Ok(())
}

/// The objects of one actor no restore can need, manifests first so the segments they name go
/// with them.
fn plan(objects: &[Listed], cutoff: SystemTime) -> Vec<&Listed> {
    let mut bases: Vec<(BaseName, &Listed)> = objects
        .iter()
        .filter_map(|object| Some((base::parse(&object.name)?, object)))
        .collect();
    bases.sort_by_key(|(name, _)| std::cmp::Reverse(name.version));
    let Some((newest, _)) = bases.first() else {
        return Vec::new();
    };
    // The fallback base must predate the lookback so every retained version has a restore path.
    let earliest_recent_epoch = objects
        .iter()
        .filter(|object| object.created >= cutoff && !object.name.ends_with(".sweeping"))
        .filter_map(|object| epoch_of(&object.name))
        .min();
    let Some((floor, _)) = bases.iter().find(|(base, object)| {
        base.version < newest.version
            && object.created < cutoff
            && earliest_recent_epoch.is_none_or(|epoch| base.owner_epoch < epoch)
    }) else {
        return Vec::new();
    };
    let mut doomed: Vec<&Listed> = bases
        .iter()
        .filter(|(base, object)| base.version < floor.version && object.created < cutoff)
        .map(|(_, object)| *object)
        .collect();
    let mut epochs: BTreeMap<u64, Vec<&Listed>> = BTreeMap::new();
    for object in objects {
        if let Some(epoch) = epoch_of(&object.name) {
            epochs.entry(epoch).or_default().push(object);
        }
    }
    for epoch in epochs.range(..floor.owner_epoch).map(|(_, epoch)| epoch) {
        if epoch
            .iter()
            .all(|obj| obj.created < cutoff || obj.name.ends_with(".sweeping"))
        {
            doomed.extend(epoch);
        }
    }
    doomed.sort_by_key(|object| !object.name.ends_with(".manifest"));
    doomed
}

async fn sweep_segment(store: &dyn SweepStore, key: &str, dry_run: bool) -> Result<()> {
    let original = key.strip_suffix(".sweeping").unwrap_or(key);
    let journal = format!("{original}.sweeping");
    let Some(bytes) = store.get(key).await? else {
        return Ok(());
    };
    let manifest: Manifest = serde_json::from_reader(bytes.reader())?;
    let (actor_and_epoch, id) = original
        .strip_suffix(".manifest")
        .and_then(|s| s.rsplit_once("~log~"))
        .context("invalid manifest name")?;
    let actor = actor_of(actor_and_epoch)
        .strip_prefix(SNAPSHOTS)
        .context("invalid actor prefix")?;
    let expected = format!("durable-actors-v3-logs-{actor}~{id}.segment");
    ensure!(
        manifest.id == id && manifest.format == 1 && !manifest.replicas.is_empty(),
        "invalid sweep manifest"
    );
    for replica in &manifest.replicas {
        ensure!(
            replica.object == expected
                && replica.generation >= 0
                && store.allows_replica(&replica.bucket),
            "invalid sweep replica"
        );
    }
    if !dry_run {
        // Keep the descriptors across a restart after the manifest has been unlisted.
        if key == original {
            store.remember(&journal, bytes).await?;
        }
        store.delete(None, original, None).await?;
    }
    for replica in &manifest.replicas {
        tracing::info!(
            event = "history_sweep_object",
            key = replica.object,
            bucket = replica.bucket,
            dry_run
        );
        if !dry_run {
            store
                .delete(
                    Some(&replica.bucket),
                    &replica.object,
                    // A replica recorded without its generation goes by name: segment names are
                    // never reused.
                    (replica.generation > 0).then_some(replica.generation),
                )
                .await?;
        }
    }
    if !dry_run {
        store.delete(None, &journal, None).await?;
    }
    Ok(())
}

fn actor_of(name: &str) -> &str {
    name.split_once('~').map_or(name, |(actor, _)| actor)
}

/// The owner epoch an object of the actor's history belongs to; bases have none.
fn epoch_of(name: &str) -> Option<u64> {
    let epoch = name.split('~').nth(1)?;
    (epoch.len() == 32)
        .then(|| u64::from_str_radix(epoch, 16).ok())
        .flatten()
}

struct GcsSweep {
    archive: String,
    replicas: std::collections::HashSet<String>,
    objects: super::super::GcsBucket,
    clients: super::super::gcs::GcsClients,
}

impl GcsSweep {
    async fn new(bucket: &str, placements: &[crate::bucket::RapidBucket]) -> Result<Self> {
        let objects = super::super::GcsBucket::new(bucket).await?;
        Ok(Self {
            archive: format!("projects/_/buckets/{bucket}"),
            replicas: placements
                .iter()
                .map(|p| format!("projects/_/buckets/{}", p.bucket))
                .collect(),
            clients: objects.clients(),
            objects,
        })
    }
}

#[async_trait]
impl SweepStore for GcsSweep {
    async fn page(&self, prefix: &str, page: &str) -> Result<(Vec<Listed>, String)> {
        let response = self
            .clients
            .control
            .list_objects()
            .set_parent(&self.archive)
            .set_prefix(prefix)
            .set_page_token(page)
            .send()
            .await?;
        let objects = response
            .objects
            .into_iter()
            .map(|object| Listed {
                created: object
                    .create_time
                    .and_then(|time| SystemTime::try_from(time).ok())
                    .unwrap_or_else(SystemTime::now),
                bytes: u64::try_from(object.size).unwrap_or_default(),
                name: object.name,
            })
            .collect();
        Ok((objects, response.next_page_token))
    }

    async fn get(&self, name: &str) -> Result<Option<Payload>> {
        Ok(self.objects.get(name).await?.map(|object| object.bytes))
    }

    async fn remember(&self, name: &str, bytes: Payload) -> Result<()> {
        ensure!(
            replace(&self.objects, name, None, bytes).await?,
            "conflicting sweep journal"
        );
        Ok(())
    }

    fn allows_replica(&self, bucket: &str) -> bool {
        self.replicas.contains(bucket)
    }

    async fn delete(
        &self,
        bucket: Option<&str>,
        name: &str,
        generation: Option<i64>,
    ) -> Result<()> {
        let mut request = self
            .clients
            .control
            .delete_object()
            .set_bucket(bucket.unwrap_or(&self.archive))
            .set_object(name);
        if let Some(generation) = generation {
            request = request.set_if_generation_match(generation);
        }
        match request.send().await {
            Ok(_) => Ok(()),
            Err(error)
                if error.http_status_code() == Some(404)
                    || error.status().is_some_and(|s| s.code.name() == "NOT_FOUND") =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/rapid/sweep.rs"]
mod tests;
