//! The project's past events, read back from the store in the shapes `observe/events` streams
//! them. An event that happens once needs no query or route of its own: it gets a case in the
//! stream's decoder and an entry in `RECORDED`.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use super::observe_events::{ObserveEvent, platform_event};
use crate::{
    platform_events::sandboxes,
    request_traces::{TraceStore, events::EventQuery},
};

/// The stored names behind each event type that has a history. Lifecycle reports and gateway
/// heartbeats repeat every few seconds, so the residency and inventory views read theirs.
const RECORDED: &[(&str, &[&str])] = &[("kill", &[sandboxes::KILLED])];

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct EventHistoryQuery {
    /// Comma-separated event types; every type with a history when left out.
    pub types: Option<String>,
    pub actor_name: Option<String>,
    pub actor_id: Option<String>,
    pub from_ms: Option<u64>,
    pub to_ms: Option<u64>,
}

#[derive(Debug, PartialEq, Serialize)]
pub(super) struct EventHistory {
    pub events: Vec<RecordedEvent>,
    pub truncated: bool,
}

/// One event under the type the stream sends it as.
#[derive(Debug, PartialEq, Serialize)]
pub(super) struct RecordedEvent {
    #[serde(rename = "type")]
    pub name: &'static str,
    pub item: ObserveEvent,
}

/// The project's past events that the query covers, newest first.
pub(super) async fn read(
    traces: &TraceStore,
    project: &str,
    query: &EventQuery,
) -> Result<EventHistory> {
    let stored = traces.platform_events(project, query).await?;
    Ok(EventHistory {
        events: stored
            .events
            .iter()
            .filter_map(platform_event)
            .map(RecordedEvent::from)
            .collect(),
        truncated: stored.truncated,
    })
}

impl EventHistoryQuery {
    /// The stored events that hold the requested types.
    pub(super) fn stored(&self) -> Result<EventQuery> {
        Ok(EventQuery {
            names: self.names()?,
            actor_name: self.actor_name.clone(),
            actor_id: self.actor_id.clone(),
            from_ms: self.from_ms,
            to_ms: self.to_ms,
        })
    }

    fn names(&self) -> Result<Vec<&'static str>> {
        let Some(types) = &self.types else {
            return Ok(RECORDED
                .iter()
                .flat_map(|(_, names)| *names)
                .copied()
                .collect());
        };
        let mut names = Vec::new();
        for requested in types.split(',') {
            match RECORDED.iter().find(|(name, _)| *name == requested.trim()) {
                Some((_, stored)) => names.extend_from_slice(stored),
                None => bail!("unknown event type {requested:?}; use {}", recorded_types()),
            }
        }
        Ok(names)
    }
}

impl From<ObserveEvent> for RecordedEvent {
    fn from(item: ObserveEvent) -> Self {
        Self {
            name: item.name(),
            item,
        }
    }
}

fn recorded_types() -> String {
    let types: Vec<_> = RECORDED.iter().map(|(name, _)| *name).collect();
    types.join(", ")
}
