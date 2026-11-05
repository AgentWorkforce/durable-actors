use anyhow::{Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use super::{InvalidTraceQuery, TRACE_CAPACITY, history::HistoryQuery};
use crate::actor::LogLevel;

const MAX_SEARCH_CHARS: usize = 256;
const MAX_HISTOGRAM_BUCKETS: u64 = 1_000;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActorLog {
    pub log_id: String,
    #[serde(skip)]
    pub project_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub host_id: String,
    pub request_id: Option<String>,
    pub timestamp_ms: u64,
    pub level: String,
    pub message: String,
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LogQuery {
    pub actor_name: Option<String>,
    pub actor_id: Option<String>,
    pub request_id: Option<String>,
    /// The least severe level to include.
    pub level: Option<LogLevel>,
    /// Text the message must contain, ignoring case.
    pub search: Option<String>,
    pub from_ms: Option<u64>,
    pub to_ms: Option<u64>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

impl LogQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.search
                .as_ref()
                .is_none_or(|text| (1..=MAX_SEARCH_CHARS).contains(&text.chars().count())),
            "search must be 1 to {MAX_SEARCH_CHARS} characters"
        );
        HistoryQuery {
            request_id: self.request_id.clone(),
            actor_name: self.actor_name.clone(),
            actor_id: self.actor_id.clone(),
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            limit: self.limit(),
            cursor: self.cursor.clone(),
            ..Default::default()
        }
        .validate()
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(TRACE_CAPACITY)
    }

    pub(crate) fn matches(&self, log: &ActorLog) -> bool {
        self.actor_name
            .as_ref()
            .is_none_or(|name| *name == log.actor_name)
            && self.actor_id.as_ref().is_none_or(|id| *id == log.actor_id)
            && self
                .request_id
                .as_ref()
                .is_none_or(|id| log.request_id.as_ref() == Some(id))
            && self
                .levels()
                .is_none_or(|levels| levels.contains(&log.level.as_str()))
            && self
                .search
                .as_ref()
                .is_none_or(|text| log.message.to_lowercase().contains(&text.to_lowercase()))
    }

    pub(crate) fn levels(&self) -> Option<Vec<&'static str>> {
        self.level.map(LogLevel::and_above)
    }

    pub(crate) fn after(&self) -> Result<Option<LogCursor>> {
        self.cursor.as_deref().map(LogCursor::decode).transpose()
    }
}

/// Line counts per level in equal time buckets, filtered like a log query.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LogHistogramQuery {
    pub actor_name: Option<String>,
    pub actor_id: Option<String>,
    pub request_id: Option<String>,
    pub search: Option<String>,
    pub from_ms: u64,
    pub to_ms: u64,
    pub bucket_ms: u64,
}

impl LogHistogramQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(self.from_ms < self.to_ms, "fromMs must be before toMs");
        ensure!(
            self.bucket_ms > 0 && self.buckets() <= MAX_HISTOGRAM_BUCKETS,
            "bucketMs must split the range into 1 to {MAX_HISTOGRAM_BUCKETS} buckets"
        );
        self.lines().validate()
    }

    /// The lines it counts: every level, from fromMs up to but excluding toMs.
    pub(crate) fn lines(&self) -> LogQuery {
        LogQuery {
            actor_name: self.actor_name.clone(),
            actor_id: self.actor_id.clone(),
            request_id: self.request_id.clone(),
            search: self.search.clone(),
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
pub(crate) struct LogHistogram {
    pub from_ms: u64,
    pub bucket_ms: u64,
    pub buckets: Vec<LevelCounts>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub(crate) struct LevelCounts {
    pub debug: u64,
    pub info: u64,
    pub warn: u64,
    pub error: u64,
}

impl LogHistogram {
    pub(crate) fn empty(query: &LogHistogramQuery) -> Self {
        Self {
            from_ms: query.from_ms,
            bucket_ms: query.bucket_ms,
            buckets: vec![LevelCounts::default(); query.buckets() as usize],
        }
    }

    pub(crate) fn add(&mut self, bucket: u64, level: &str, lines: u64) {
        let Some(counts) = usize::try_from(bucket)
            .ok()
            .and_then(|index| self.buckets.get_mut(index))
        else {
            return;
        };
        match level {
            "debug" => counts.debug += lines,
            "info" => counts.info += lines,
            "warn" => counts.warn += lines,
            "error" => counts.error += lines,
            _ => {}
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LogPage {
    pub records: Vec<ActorLog>,
    pub next_cursor: Option<String>,
}

impl LogPage {
    pub(crate) fn new(mut records: Vec<ActorLog>, limit: usize) -> Result<Self> {
        let more = records.len() > limit;
        records.truncate(limit);
        let next_cursor = match records.last() {
            Some(last) if more => Some(LogCursor::after(last).encode()?),
            _ => None,
        };
        Ok(Self {
            records,
            next_cursor,
        })
    }
}

/// Logs page newest first; the cursor is the last row returned.
#[derive(Serialize, Deserialize)]
pub(crate) struct LogCursor {
    pub timestamp_ms: u64,
    pub log_id: String,
}

impl LogCursor {
    fn after(log: &ActorLog) -> Self {
        Self {
            timestamp_ms: log.timestamp_ms,
            log_id: log.log_id.clone(),
        }
    }

    fn decode(value: &str) -> Result<Self> {
        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| InvalidTraceQuery)?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| InvalidTraceQuery)?;
        ensure!(cursor.log_id.len() <= 64, InvalidTraceQuery);
        Ok(cursor)
    }

    fn encode(&self) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?))
    }
}
