use super::{LifecycleEvent, LifecycleKind, STALE_AFTER_MS};
use crate::placement::{ActorResidency, ResidencyHistory, ResidencyPeriod};

/// A live or dormant report expires after this long, so older reports can't draw a live period
/// in the window.
pub(crate) const LOOKBACK_MS: u64 = STALE_AFTER_MS;
pub(crate) const RUN_LIMIT: usize = 100_000;

/// One lifecycle report, reduced to what residency needs.
#[derive(Clone, Debug)]
pub(crate) struct ResidencyReport {
    pub actor_id: String,
    pub owner_epoch: u64,
    pub sequence: u64,
    pub occurred_at_ms: u64,
    pub status: ActorResidency,
    pub stopped: bool,
}

impl From<&LifecycleEvent> for ResidencyReport {
    fn from(event: &LifecycleEvent) -> Self {
        let stopped = event.report.kind == LifecycleKind::Stopped;
        Self {
            actor_id: event.actor.actor_id.clone(),
            owner_epoch: event.report.owner_epoch,
            sequence: event.report.sequence,
            occurred_at_ms: event.report.occurred_at_ms,
            status: match (stopped, event.report.resident) {
                (true, _) | (_, Some(false)) => ActorResidency::Dormant,
                (_, Some(true)) => ActorResidency::Live,
                _ => ActorResidency::Unknown,
            },
            stopped,
        }
    }
}

/// Consecutive reports from one instance with the same status and no stale gap. Its interior
/// reports never change the drawn periods, so one run stands in for all of them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResidencyRun {
    pub actor_id: String,
    pub owner_epoch: u64,
    pub sequence: u64,
    pub status: ActorResidency,
    pub started_at_ms: u64,
    pub last_at_ms: u64,
    /// Whether the run's last report was a stop, which stays known until the next report.
    pub stopped: bool,
}

/// Groups reports into runs; the reference for what `residency.sql` returns.
pub(crate) fn runs(reports: Vec<ResidencyReport>) -> Vec<ResidencyRun> {
    let mut runs = Vec::<ResidencyRun>::new();
    for report in in_order(reports) {
        match runs.last_mut() {
            Some(run) if run.continues_with(&report) => run.extend(&report),
            _ => runs.push(ResidencyRun::from(report)),
        }
    }
    runs
}

/// The periods each instance spent in each status within `[from, to]`.
pub(crate) fn history(runs: Vec<ResidencyRun>, from: u64, to: u64) -> ResidencyHistory {
    let (runs, truncated) = limit(runs);
    let mut periods = Vec::new();
    for (index, run) in runs.iter().enumerate() {
        let next = runs
            .get(index + 1)
            .filter(|next| next.actor_id == run.actor_id);
        run.draw(&mut periods, next.map(|next| next.started_at_ms), from, to);
    }
    ResidencyHistory { periods, truncated }
}

/// Each instance's reports in send order, without redeliveries or reports that occurred before
/// one sent earlier.
fn in_order(mut reports: Vec<ResidencyReport>) -> Vec<ResidencyReport> {
    reports.sort_by(|a, b| a.order().cmp(&b.order()));
    reports.dedup_by(|a, b| a.order() == b.order());
    let mut latest: Option<(String, u64)> = None;
    reports.retain(|report| {
        let floor = match &latest {
            Some((actor_id, at)) if *actor_id == report.actor_id => *at,
            _ => 0,
        };
        latest = Some((report.actor_id.clone(), floor.max(report.occurred_at_ms)));
        report.occurred_at_ms >= floor
    });
    reports
}

/// Keeps the newest runs.
fn limit(mut runs: Vec<ResidencyRun>) -> (Vec<ResidencyRun>, bool) {
    let total = runs.len();
    runs.sort_by_key(|run| std::cmp::Reverse(run.recency()));
    runs.truncate(RUN_LIMIT);
    let truncated = runs.len() < total;
    runs.sort_by(|a, b| a.order().cmp(&b.order()));
    (runs, truncated)
}

impl ResidencyReport {
    fn order(&self) -> (&str, u64, u64) {
        (&self.actor_id, self.owner_epoch, self.sequence)
    }
}

impl From<ResidencyReport> for ResidencyRun {
    fn from(report: ResidencyReport) -> Self {
        Self {
            actor_id: report.actor_id,
            owner_epoch: report.owner_epoch,
            sequence: report.sequence,
            status: report.status,
            started_at_ms: report.occurred_at_ms,
            last_at_ms: report.occurred_at_ms,
            stopped: report.stopped,
        }
    }
}

impl ResidencyRun {
    fn continues_with(&self, report: &ResidencyReport) -> bool {
        self.actor_id == report.actor_id
            && self.status == report.status
            && (self.stopped || report.occurred_at_ms - self.last_at_ms <= STALE_AFTER_MS)
    }

    fn extend(&mut self, report: &ResidencyReport) {
        self.last_at_ms = report.occurred_at_ms;
        self.stopped = report.stopped;
    }

    fn order(&self) -> (&str, u64, u64) {
        (&self.actor_id, self.owner_epoch, self.sequence)
    }

    fn recency(&self) -> (u64, u64, u64) {
        (self.started_at_ms, self.owner_epoch, self.sequence)
    }

    /// The run's status until it is no longer known, then unknown until the next run starts.
    fn draw(&self, periods: &mut Vec<ResidencyPeriod>, next: Option<u64>, from: u64, to: u64) {
        let start = self.started_at_ms.max(from);
        let end = next.map_or(to, |next| next.min(to));
        let known_until = if self.stopped {
            end
        } else {
            self.last_at_ms.saturating_add(STALE_AFTER_MS).min(end)
        };
        push(periods, &self.actor_id, self.status, start, known_until);
        let unknown_from = start.max(known_until);
        push(
            periods,
            &self.actor_id,
            ActorResidency::Unknown,
            unknown_from,
            end,
        );
    }
}

fn push(
    periods: &mut Vec<ResidencyPeriod>,
    actor_id: &str,
    status: ActorResidency,
    from_ms: u64,
    to_ms: u64,
) {
    if from_ms >= to_ms {
        return;
    }
    if let Some(previous) = periods.last_mut()
        && previous.actor_id == actor_id
        && previous.status == status
        && previous.to_ms == from_ms
    {
        previous.to_ms = to_ms;
    } else {
        periods.push(ResidencyPeriod {
            actor_id: actor_id.to_owned(),
            status,
            from_ms,
            to_ms,
        });
    }
}
