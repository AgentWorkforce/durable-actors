use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use moka::{Expiry, future::Cache, ops::compute::Op};
use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorKey, actor_lifecycle::HEARTBEAT_INTERVAL, bucket::OwnershipHint, host::HostId,
    host_leases::MAX_HOST_LEASE_DURATION_MS, placement::ObjectPlacement, redis_client::Redis,
};

// Heartbeats keep a live host's route; one that dies without reporting `Stopped` ages out.
const LIVENESS: Duration = Duration::from_secs(3 * HEARTBEAT_INTERVAL.as_secs());
// Outlives any lease the host renewed while draining, so discovery never trusts that lease.
const SHUTDOWN_RETENTION: Duration = Duration::from_millis(2 * MAX_HOST_LEASE_DURATION_MS);
// Only bounds the store: the ownership CAS rejects a sealed record that changed since.
const SEALED_OWNER_RETENTION: Duration = Duration::from_secs(72 * 60 * 60);

#[async_trait]
pub(super) trait RouteStore: Send + Sync {
    async fn get(&self, actor: &ActorKey) -> Option<RouteEntry>;
    async fn generation(&self, project: &str) -> Option<u64>;
    // Drops a route resolved before the project's latest deployment change.
    async fn remember(&self, actor: &ActorKey, route: CachedRoute, generation: Option<u64>);
    async fn put(&self, actor: &ActorKey, entry: RouteEntry);
    async fn renew(&self, actor: &ActorKey, identity: &RouteIdentity);
    async fn remove(&self, actor: &ActorKey, identity: &RouteIdentity);
    async fn remove_project(&self, project: &str);
    async fn keep_sealed_owner(&self, actor: &ActorKey, owner: &OwnershipHint);
    async fn take_sealed_owner(&self, actor: &ActorKey) -> Option<OwnershipHint>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum RouteEntry {
    Active(CachedRoute),
    Draining(RouteIdentity),
    Stopped(RouteIdentity),
}

impl RouteEntry {
    fn supersedes(&self, current: &RouteEntry) -> bool {
        let (next, previous) = (self.identity(), current.identity());
        if next == previous {
            self.rank() >= current.rank()
        } else {
            next.epoch > previous.epoch
        }
    }

    fn is_active(&self, identity: &RouteIdentity) -> bool {
        matches!(self, Self::Active(_)) && self.identity() == *identity
    }

    fn identity(&self) -> RouteIdentity {
        match self {
            Self::Active(route) => RouteIdentity::of(&route.placement),
            Self::Draining(identity) | Self::Stopped(identity) => identity.clone(),
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Self::Active(_) => 0,
            Self::Draining(_) => 1,
            Self::Stopped(_) => 2,
        }
    }

    fn state(&self) -> &'static str {
        ["active", "draining", "stopped"][usize::from(self.rank())]
    }

    fn lifetime(&self) -> Duration {
        match self {
            Self::Active(_) => LIVENESS,
            Self::Draining(_) | Self::Stopped(_) => SHUTDOWN_RETENTION,
        }
    }
}

// One generation covers every project; a deployment only costs other projects a cache miss.
pub(super) struct MemoryRoutes {
    entries: Cache<ActorKey, Stamped>,
    sealed: Cache<ActorKey, OwnershipHint>,
    generation: AtomicU64,
}

impl Default for MemoryRoutes {
    fn default() -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(100_000)
                .expire_after(EntryLifetime)
                .build(),
            sealed: Cache::builder()
                .max_capacity(100_000)
                .time_to_live(SEALED_OWNER_RETENTION)
                .build(),
            generation: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl RouteStore for MemoryRoutes {
    async fn get(&self, actor: &ActorKey) -> Option<RouteEntry> {
        let stamped = self.entries.get(actor).await?;
        stamped.visible(self.current()).then_some(stamped.entry)
    }

    async fn generation(&self, _: &str) -> Option<u64> {
        Some(self.current())
    }

    async fn remember(&self, actor: &ActorKey, route: CachedRoute, generation: Option<u64>) {
        if let Some(generation) = generation {
            self.write(actor, RouteEntry::Active(route), Some(generation))
                .await;
        }
    }

    async fn put(&self, actor: &ActorKey, entry: RouteEntry) {
        self.write(actor, entry, None).await;
    }

    async fn renew(&self, actor: &ActorKey, identity: &RouteIdentity) {
        self.update_if_active(actor, identity, Op::Put).await;
    }

    async fn remove(&self, actor: &ActorKey, identity: &RouteIdentity) {
        self.update_if_active(actor, identity, |_| Op::Remove).await;
    }

    async fn remove_project(&self, _: &str) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    async fn keep_sealed_owner(&self, actor: &ActorKey, owner: &OwnershipHint) {
        self.sealed.insert(actor.clone(), owner.clone()).await;
    }

    async fn take_sealed_owner(&self, actor: &ActorKey) -> Option<OwnershipHint> {
        self.sealed.remove(actor).await
    }
}

impl MemoryRoutes {
    fn current(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    async fn write(&self, actor: &ActorKey, entry: RouteEntry, observed: Option<u64>) {
        self.entries
            .entry(actor.clone())
            .and_compute_with(|current| async move {
                let generation = self.current();
                if observed.is_some_and(|observed| observed != generation)
                    || current
                        .filter(|current| current.value().visible(generation))
                        .is_some_and(|current| !entry.supersedes(&current.value().entry))
                {
                    Op::Nop
                } else {
                    Op::Put(Stamped { generation, entry })
                }
            })
            .await;
    }

    async fn update_if_active(
        &self,
        actor: &ActorKey,
        identity: &RouteIdentity,
        op: fn(Stamped) -> Op<Stamped>,
    ) {
        self.entries
            .entry(actor.clone())
            .and_compute_with(|current| async move {
                match current {
                    Some(current) if current.value().entry.is_active(identity) => {
                        op(current.into_value())
                    }
                    _ => Op::Nop,
                }
            })
            .await;
    }
}

#[derive(Clone)]
struct Stamped {
    generation: u64,
    entry: RouteEntry,
}

impl Stamped {
    fn visible(&self, generation: u64) -> bool {
        self.generation == generation || !matches!(self.entry, RouteEntry::Active(_))
    }
}

struct EntryLifetime;

impl Expiry<ActorKey, Stamped> for EntryLifetime {
    fn expire_after_create(&self, _: &ActorKey, stamped: &Stamped, _: Instant) -> Option<Duration> {
        Some(stamped.entry.lifetime())
    }

    fn expire_after_update(
        &self,
        _: &ActorKey,
        stamped: &Stamped,
        _: Instant,
        _: Option<Duration>,
    ) -> Option<Duration> {
        Some(stamped.entry.lifetime())
    }
}

// One INCR of the project generation retires its active routes; shutdown states outlive deploys.
const CURRENT_ENTRY: &str = "
local entry = redis.call('HMGET', KEYS[1], 'generation', 'state', 'entry')
if entry[3] and (entry[2] ~= 'active' or entry[1] == (redis.call('GET', KEYS[2]) or '0')) then
  return entry[3]
end
return false";
// Mirrors `MemoryRoutes::write`; ARGV[6] is the generation a resolved route was read under.
const PUT_ENTRY: &str = "
local generation = redis.call('GET', KEYS[2]) or '0'
if ARGV[6] ~= '' and ARGV[6] ~= generation then return 0 end
local rank = {active = 0, draining = 1, stopped = 2}
local current = redis.call('HMGET', KEYS[1], 'identity', 'epoch', 'state', 'generation')
if current[3] == 'active' and current[4] ~= generation then current = {} end
if current[1] == ARGV[1] then
  if rank[ARGV[3]] < rank[current[3]] then return 0 end
elseif current[1] and tonumber(ARGV[2]) <= tonumber(current[2]) then
  return 0
end
redis.call('HSET', KEYS[1], 'generation', generation,
  'identity', ARGV[1], 'epoch', ARGV[2], 'state', ARGV[3], 'entry', ARGV[4])
return redis.call('PEXPIRE', KEYS[1], ARGV[5])";
const IF_ACTIVE: &str = "
local current = redis.call('HMGET', KEYS[1], 'identity', 'state')
if current[1] ~= ARGV[1] or current[2] ~= 'active' then return 0 end
return redis.call(ARGV[2], KEYS[1], unpack(ARGV, 3))";

pub(super) struct RedisRoutes {
    redis: Redis,
    namespace: String,
}

impl RedisRoutes {
    pub(super) fn new(redis: Redis, namespace: &str) -> Self {
        Self {
            redis,
            namespace: namespace.into(),
        }
    }
}

#[async_trait]
impl RouteStore for RedisRoutes {
    async fn get(&self, actor: &ActorKey) -> Option<RouteEntry> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(CURRENT_ENTRY)
            .arg(2)
            .arg(self.entry_key(actor))
            .arg(self.generation_key(&actor.project_id));
        let entry: Option<String> = self.run(command).await.flatten();
        serde_json::from_str(&entry?).ok()
    }

    async fn generation(&self, project: &str) -> Option<u64> {
        let mut command = redis::cmd("GET");
        command.arg(self.generation_key(project));
        self.run::<Option<u64>>(command)
            .await
            .map(Option::unwrap_or_default)
    }

    async fn remember(&self, actor: &ActorKey, route: CachedRoute, generation: Option<u64>) {
        if let Some(generation) = generation {
            self.write(actor, &RouteEntry::Active(route), &generation.to_string())
                .await;
        }
    }

    async fn put(&self, actor: &ActorKey, entry: RouteEntry) {
        self.write(actor, &entry, "").await;
    }

    async fn renew(&self, actor: &ActorKey, identity: &RouteIdentity) {
        let mut command = self.if_active(actor, identity, "PEXPIRE");
        command.arg(LIVENESS.as_millis() as u64);
        self.run::<()>(command).await;
    }

    async fn remove(&self, actor: &ActorKey, identity: &RouteIdentity) {
        self.run::<()>(self.if_active(actor, identity, "DEL")).await;
    }

    async fn remove_project(&self, project: &str) {
        let mut command = redis::cmd("INCR");
        command.arg(self.generation_key(project));
        self.run::<()>(command).await;
    }

    async fn keep_sealed_owner(&self, actor: &ActorKey, owner: &OwnershipHint) {
        let mut command = redis::cmd("SET");
        command
            .arg(self.sealed_owner_key(actor))
            .arg(serde_json::to_string(owner).expect("ownership hint is serializable"))
            .arg("PX")
            .arg(SEALED_OWNER_RETENTION.as_millis() as u64);
        self.run::<()>(command).await;
    }

    async fn take_sealed_owner(&self, actor: &ActorKey) -> Option<OwnershipHint> {
        let mut command = redis::cmd("GETDEL");
        command.arg(self.sealed_owner_key(actor));
        let owner: Option<String> = self.run(command).await.flatten();
        serde_json::from_str(&owner?).ok()
    }
}

impl RedisRoutes {
    async fn write(&self, actor: &ActorKey, entry: &RouteEntry, generation: &str) {
        let identity = entry.identity();
        let mut command = redis::cmd("EVAL");
        command
            .arg(PUT_ENTRY)
            .arg(2)
            .arg(self.entry_key(actor))
            .arg(self.generation_key(&actor.project_id))
            .arg(identity.key())
            .arg(identity.epoch)
            .arg(entry.state())
            .arg(serde_json::to_string(entry).expect("route entry is serializable"))
            .arg(entry.lifetime().as_millis() as u64)
            .arg(generation);
        self.run::<()>(command).await;
    }

    fn if_active(&self, actor: &ActorKey, identity: &RouteIdentity, operation: &str) -> redis::Cmd {
        let mut command = redis::cmd("EVAL");
        command
            .arg(IF_ACTIVE)
            .arg(1)
            .arg(self.entry_key(actor))
            .arg(identity.key())
            .arg(operation);
        command
    }

    async fn run<T: redis::FromRedisValue>(&self, command: redis::Cmd) -> Option<T> {
        self.redis
            .run(command)
            .await
            .inspect_err(|error| tracing::warn!(%error, "actor route store unavailable"))
            .ok()
    }

    fn entry_key(&self, actor: &ActorKey) -> String {
        format!(
            "{}:actor:{}",
            self.namespace,
            serde_json::to_string(actor).expect("actor key is serializable")
        )
    }

    fn generation_key(&self, project: &str) -> String {
        format!("{}:generation:{project}", self.namespace)
    }

    fn sealed_owner_key(&self, actor: &ActorKey) -> String {
        format!(
            "{}:sealed-owner:{}",
            self.namespace,
            serde_json::to_string(actor).expect("actor key is serializable")
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct CachedRoute {
    pub placement: ObjectPlacement,
    pub host_config_key: String,
    pub overrides_regions: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RouteIdentity {
    pub host: HostId,
    pub session: String,
    pub epoch: u64,
}

impl RouteIdentity {
    pub(super) fn of(placement: &ObjectPlacement) -> Self {
        Self {
            host: placement.owner.clone(),
            session: placement.lease.session_id.clone(),
            epoch: placement.owner_epoch,
        }
    }

    fn key(&self) -> String {
        serde_json::to_string(self).expect("route identity is serializable")
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/routes.rs"]
mod tests;
