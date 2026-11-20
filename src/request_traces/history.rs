use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

use super::{RequestOutcome, TRACE_CAPACITY, logs::MAX_HISTOGRAM_BUCKETS};

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct HistoryQuery {
    pub request_id: Option<String>,
    pub connection_id: Option<String>,
    pub actor_name: Option<String>,
    pub actor_id: Option<String>,
    pub outcome: Option<RequestOutcome>,
    pub from_ms: Option<u64>,
    pub to_ms: Option<u64>,
    #[serde(default = "page_size")]
    pub limit: usize,
    pub cursor: Option<String>,
}

impl Default for HistoryQuery {
    fn default() -> Self {
        Self {
            request_id: None,
            connection_id: None,
            actor_name: None,
            actor_id: None,
            outcome: None,
            from_ms: None,
            to_ms: None,
            limit: page_size(),
            cursor: None,
        }
    }
}

impl HistoryQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.request_id
                .iter()
                .chain(self.connection_id.iter())
                .all(|id| !id.is_empty() && id.len() <= 256),
            "invalid request or connection ID"
        );
        ensure!(
            (1..=TRACE_CAPACITY).contains(&self.limit),
            "limit must be between 1 and 500"
        );
        ensure!(
            self.cursor
                .as_ref()
                .is_none_or(|c| !c.is_empty() && c.len() <= 4096),
            "invalid history cursor"
        );
        ensure!(
            self.from_ms
                .into_iter()
                .chain(self.to_ms)
                .all(|ms| ms <= 9_007_199_254_740_991),
            "invalid timestamp"
        );
        ensure!(
            self.from_ms
                .zip(self.to_ms)
                .is_none_or(|(from, to)| from <= to),
            "fromMs must not exceed toMs"
        );
        crate::actor::ActorKey {
            project_id: "history".into(),
            actor_name: self.actor_name.clone().unwrap_or_else(|| "Actor".into()),
            actor_id: self.actor_id.clone().unwrap_or_else(|| "actor".into()),
        }
        .validate()
    }

    pub(super) fn filter_key(&self) -> Result<String> {
        Ok(serde_json::to_string(&(
            &self.request_id,
            &self.connection_id,
            &self.actor_name,
            &self.actor_id,
            self.outcome,
            self.from_ms,
            self.to_ms,
        ))?)
    }
}

fn page_size() -> usize {
    100
}

/// Request counts and timings in equal time buckets, for the project, one class or one instance.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RequestHistogramQuery {
    pub actor_name: Option<String>,
    pub actor_id: Option<String>,
    pub from_ms: u64,
    pub to_ms: u64,
    pub bucket_ms: u64,
}

impl RequestHistogramQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(self.from_ms < self.to_ms, "fromMs must be before toMs");
        ensure!(
            self.bucket_ms > 0 && self.buckets() <= MAX_HISTOGRAM_BUCKETS,
            "bucketMs must split the range into 1 to {MAX_HISTOGRAM_BUCKETS} buckets"
        );
        self.requests().validate()
    }

    /// The requests it counts: from fromMs up to but excluding toMs.
    pub(crate) fn requests(&self) -> HistoryQuery {
        HistoryQuery {
            actor_name: self.actor_name.clone(),
            actor_id: self.actor_id.clone(),
            from_ms: Some(self.from_ms),
            to_ms: Some(self.to_ms - 1),
            ..Default::default()
        }
    }

    fn buckets(&self) -> u64 {
        (self.to_ms - self.from_ms).div_ceil(self.bucket_ms)
    }
}

/// Bucket `i` covers `fromMs + i * bucketMs` up to the next bucket.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestHistogram {
    pub from_ms: u64,
    pub bucket_ms: u64,
    pub buckets: Vec<RequestCounts>,
}

impl RequestHistogram {
    pub(crate) fn empty(query: &RequestHistogramQuery) -> Self {
        Self {
            from_ms: query.from_ms,
            bucket_ms: query.bucket_ms,
            buckets: vec![RequestCounts::default(); query.buckets() as usize],
        }
    }

    pub(crate) fn bucket(&mut self, index: u64) -> Option<&mut RequestCounts> {
        usize::try_from(index)
            .ok()
            .and_then(|index| self.buckets.get_mut(index))
    }
}

/// Requests by outcome, with summed timings for averages. Rerouted attempts are left out: the
/// request is retried and counted under its final outcome.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestCounts {
    pub ok: u64,
    pub failed: u64,
    pub rejected: u64,
    pub interrupted: u64,
    /// Summed queue waits of the requests that waited, and how many did.
    pub queue_wait_ms: f64,
    pub queue_waits: u64,
    /// Summed time excluding the queue wait.
    pub run_ms: f64,
}

impl RequestCounts {
    pub(crate) fn add_outcome(
        &mut self,
        outcome: RequestOutcome,
        requests: u64,
        queue_wait_ms: f64,
        queue_waits: u64,
        run_ms: f64,
    ) {
        let count = match outcome {
            RequestOutcome::Completed => &mut self.ok,
            RequestOutcome::Failed => &mut self.failed,
            RequestOutcome::Rejected => &mut self.rejected,
            RequestOutcome::Interrupted => &mut self.interrupted,
            RequestOutcome::Rerouted => return,
        };
        *count += requests;
        self.queue_wait_ms += queue_wait_ms;
        self.queue_waits += queue_waits;
        self.run_ms += run_ms;
    }
}
