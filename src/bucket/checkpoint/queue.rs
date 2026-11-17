use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use tokio::time::{Duration, Instant};

/// What the copy of a queued write must hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Expect {
    /// Exactly this generation; a newer one on the source goes to the back of its lane.
    Exact(i64),
    /// Whatever the source holds; only for immutable objects found missing by reconciliation.
    Current,
    /// The write's outcome is unknown: read the source, then queue the generation found.
    Unknown,
}

pub(super) struct Pending {
    pub key: String,
    pub expect: Expect,
    /// `None` when the payload was dropped to bound memory or was never known.
    pub bytes: Option<Bytes>,
    pub since: Instant,
}

pub(super) struct Job {
    pub lane: String,
    pub key: String,
    pub expect: Expect,
    pub bytes: Option<Bytes>,
}

pub(super) enum Outcome {
    Copied,
    Dropped,
    Requeue(i64),
    Failed,
}

/// One actor's writes, copied strictly in order: SQLite snapshots depend on their parents and
/// owner records on the snapshots they name, so a later write never reaches the copy first.
#[derive(Default)]
struct Lane {
    entries: VecDeque<Pending>,
    in_flight: bool,
    gated: bool,
    failures: u32,
    retry_at: Option<Instant>,
}

#[derive(Default)]
pub(super) struct Queue {
    lanes: HashMap<String, Lane>,
    bytes: usize,
}

const RETRY_MIN: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(5);

impl Queue {
    pub fn push(&mut self, mut entry: Pending, max_bytes: usize) {
        let lane = self.lanes.entry(lane_of(&entry.key)).or_default();
        let movable = usize::from(lane.in_flight);
        if let Some(index) = lane
            .entries
            .iter()
            .skip(movable)
            .position(|queued| queued.key == entry.key)
            .map(|index| index + movable)
        {
            let queued = &mut lane.entries[index];
            if supersedes(queued.expect, entry.expect) {
                queued.since = queued.since.min(entry.since);
                return;
            }
            let queued = lane.entries.remove(index).expect("index is in range");
            entry.since = entry.since.min(queued.since);
            self.bytes -= payload(&queued);
        }
        if let Some(bytes) = &entry.bytes {
            if self.bytes + bytes.len() > max_bytes {
                entry.bytes = None;
            } else {
                self.bytes += bytes.len();
            }
        }
        lane.entries.push_back(entry);
    }

    /// Queues `entries` ahead of everything already waiting in their lane, in order.
    pub fn push_front(&mut self, lane: &str, entries: Vec<Pending>) {
        let lane = self.lanes.entry(lane.into()).or_default();
        let at = usize::from(lane.in_flight);
        for (offset, entry) in entries.into_iter().enumerate() {
            lane.entries.insert(at + offset, entry);
        }
    }

    pub fn gate(&mut self, lane: &str) {
        self.lanes.entry(lane.into()).or_default().gated = true;
    }

    pub fn ungate(&mut self, lane: &str) {
        if let Some(lane) = self.lanes.get_mut(lane) {
            lane.gated = false;
        }
    }

    /// The oldest write of every lane that may copy now, marked in flight.
    pub fn take(&mut self, limit: usize, now: Instant) -> Vec<Job> {
        let mut jobs = Vec::new();
        for (name, lane) in &mut self.lanes {
            if jobs.len() == limit {
                break;
            }
            if lane.in_flight || lane.gated || lane.retry_at.is_some_and(|at| at > now) {
                continue;
            }
            let Some(head) = lane.entries.front() else {
                continue;
            };
            lane.in_flight = true;
            jobs.push(Job {
                lane: name.clone(),
                key: head.key.clone(),
                expect: head.expect,
                bytes: head.bytes.clone(),
            });
        }
        jobs
    }

    pub fn complete(&mut self, job: &Job, outcome: Outcome, now: Instant) {
        let Some(lane) = self.lanes.get_mut(&job.lane) else {
            return;
        };
        lane.in_flight = false;
        if let Outcome::Failed = outcome {
            lane.failures += 1;
            lane.retry_at = Some(now + backoff(lane.failures));
            return;
        }
        lane.failures = 0;
        lane.retry_at = None;
        let head = lane.entries.pop_front();
        if let Some(mut head) = head {
            self.bytes -= payload(&head);
            if let Outcome::Requeue(generation) = outcome {
                head.bytes = None;
                head.expect = Expect::Exact(generation);
                self.push(head, 0);
            }
        }
        if self.lanes.get(&job.lane).is_some_and(Lane::is_empty) {
            self.lanes.remove(&job.lane);
        }
    }

    pub fn next_retry(&self) -> Option<Instant> {
        self.lanes
            .values()
            .filter(|lane| !lane.in_flight && !lane.gated && !lane.entries.is_empty())
            .filter_map(|lane| lane.retry_at)
            .min()
    }

    pub fn pending(&self) -> usize {
        self.lanes.values().map(|lane| lane.entries.len()).sum()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn oldest(&self) -> Option<Instant> {
        self.lanes
            .values()
            .flat_map(|lane| lane.entries.iter().map(|entry| entry.since))
            .min()
    }

    pub fn is_idle(&self) -> bool {
        self.lanes
            .values()
            .all(|lane| lane.entries.is_empty() && !lane.in_flight)
    }
}

impl Lane {
    fn is_empty(&self) -> bool {
        self.entries.is_empty() && !self.in_flight && !self.gated
    }
}

/// A late completion of an older write never replaces the newer write already queued.
fn supersedes(queued: Expect, incoming: Expect) -> bool {
    matches!((queued, incoming), (Expect::Exact(a), Expect::Exact(b)) if a > b)
}

fn payload(entry: &Pending) -> usize {
    entry.bytes.as_ref().map_or(0, Bytes::len)
}

fn backoff(failures: u32) -> Duration {
    RETRY_MIN
        .saturating_mul(1 << failures.min(10).saturating_sub(1))
        .min(RETRY_MAX)
}

/// The actor an object belongs to, as its storage path; other objects are their own lane.
pub(super) fn lane_of(key: &str) -> String {
    let Some(path) = key.strip_prefix(crate::storage_paths::ROOT) else {
        return key.into();
    };
    if let Some(owner) = path.strip_prefix("owners/") {
        return owner.strip_suffix(".json").unwrap_or(owner).into();
    }
    if let Some(snapshot) = path.strip_prefix("snapshots/") {
        return snapshot
            .splitn(5, '/')
            .take(4)
            .collect::<Vec<_>>()
            .join("/");
    }
    key.into()
}

/// The snapshot prefix of the actor whose owner record is `key`, if it is one.
pub(super) fn owner_snapshots(key: &str) -> Option<String> {
    let path = key
        .strip_prefix(crate::storage_paths::ROOT)?
        .strip_prefix("owners/")?
        .strip_suffix(".json")?;
    Some(format!("{}snapshots/{path}/", crate::storage_paths::ROOT))
}
