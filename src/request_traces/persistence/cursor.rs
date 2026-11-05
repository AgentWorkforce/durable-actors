use anyhow::Result;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::request_traces::{
    InvalidTraceQuery, TracePage, TraceRecord, history::HistoryQuery, replay::ReplayQuery,
};

pub(super) struct Metadata {
    pub generation: String,
    pub head: u64,
    pub pruned: u64,
}

pub(super) struct HistoryPage {
    project_id: String,
    metadata: Metadata,
    filters: String,
    pub after: Option<HistoryCursor>,
    pub watermark: u64,
    reset: bool,
}

impl HistoryPage {
    pub fn new(project_id: &str, query: &HistoryQuery, metadata: Metadata) -> Result<Self> {
        let filters = query.filter_key()?;
        let cursor = query
            .cursor
            .as_deref()
            .map(|value| HistoryCursor::decode(value, project_id, &filters, &metadata))
            .transpose()?;
        let reset = cursor
            .as_ref()
            .is_some_and(|c| c.generation != metadata.generation || c.pruned < metadata.pruned);
        let after = cursor.filter(|_| !reset);
        let watermark = after.as_ref().map_or(metadata.head, |c| c.watermark);
        Ok(Self {
            project_id: project_id.into(),
            metadata,
            filters,
            after,
            watermark,
            reset,
        })
    }

    pub fn finish(self, limit: usize, mut records: Vec<TraceRecord>) -> Result<TracePage> {
        let more = records.len() > limit;
        records.truncate(limit);
        let next_cursor = if more {
            let last = records.last().unwrap();
            Some(encode(&HistoryCursor {
                project_id: self.project_id.clone(),
                generation: self.metadata.generation.clone(),
                watermark: self.watermark,
                pruned: self.metadata.pruned,
                time: last.event.trace.started_at_ms,
                sequence: last.sequence,
                filters: self.filters,
            })?)
        } else {
            None
        };
        Ok(TracePage {
            resume_cursor: resume_cursor(
                &self.project_id,
                &self.metadata.generation,
                self.watermark,
            )?,
            epoch: self.metadata.generation,
            cursor: self.watermark,
            capacity: limit,
            records,
            next_cursor,
            reset: self.reset,
            snapshot: false,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HistoryCursor {
    project_id: String,
    generation: String,
    watermark: u64,
    pruned: u64,
    pub time: u64,
    pub sequence: u64,
    filters: String,
}

impl HistoryCursor {
    fn decode(value: &str, project_id: &str, filters: &str, metadata: &Metadata) -> Result<Self> {
        let cursor: Self = decode(value)?;
        if cursor.project_id != project_id
            || cursor.generation.is_empty()
            || cursor.generation.len() > 64
            || cursor.watermark > i64::MAX as u64
            || cursor.pruned > cursor.watermark
            || cursor.sequence == 0
            || cursor.sequence > cursor.watermark
            || cursor.time > 9_007_199_254_740_991
            || cursor.filters != filters
            || (cursor.generation == metadata.generation
                && (cursor.watermark > metadata.head || cursor.pruned > metadata.pruned))
        {
            return Err(InvalidTraceQuery.into());
        }
        Ok(cursor)
    }
}

pub(super) struct ReplayPage {
    project_id: String,
    metadata: Metadata,
    after: Option<u64>,
    reset: bool,
}

impl ReplayPage {
    pub fn new(project_id: &str, query: &ReplayQuery, metadata: Metadata) -> Result<Self> {
        let cursor = query
            .cursor
            .as_deref()
            .map(|value| -> Result<ReplayCursor> {
                let cursor: ReplayCursor = decode(value)?;
                if cursor.project_id != project_id
                    || cursor.position > i64::MAX as u64
                    || (cursor.generation == metadata.generation && cursor.position > metadata.head)
                {
                    return Err(InvalidTraceQuery.into());
                }
                Ok(cursor)
            })
            .transpose()?;
        let reset = cursor
            .as_ref()
            .is_some_and(|c| c.generation != metadata.generation || c.position < metadata.pruned);
        let after = cursor.filter(|_| !reset).map(|c| c.position);
        Ok(Self {
            project_id: project_id.into(),
            metadata,
            after,
            reset,
        })
    }

    pub fn after(&self) -> Option<u64> {
        self.after
    }

    pub fn head(&self) -> u64 {
        self.metadata.head
    }

    pub fn finish(self, limit: usize, mut records: Vec<TraceRecord>) -> Result<TracePage> {
        let more = self.after.is_some() && records.len() > limit;
        records.truncate(limit);
        let position = if more {
            records.last().unwrap().sequence
        } else {
            self.metadata.head
        };
        let resume_cursor = resume_cursor(&self.project_id, &self.metadata.generation, position)?;
        Ok(TracePage {
            epoch: self.metadata.generation,
            cursor: position,
            capacity: limit,
            records,
            next_cursor: more.then(|| resume_cursor.clone()),
            resume_cursor,
            reset: self.reset,
            snapshot: false,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct ReplayCursor {
    project_id: String,
    generation: String,
    position: u64,
}

fn resume_cursor(project_id: &str, generation: &str, position: u64) -> Result<String> {
    encode(&ReplayCursor {
        project_id: project_id.into(),
        generation: generation.into(),
        position,
    })
}

fn encode(value: &impl Serialize) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(value)?))
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| InvalidTraceQuery)?;
    serde_json::from_slice(&bytes).map_err(|_| InvalidTraceQuery.into())
}
