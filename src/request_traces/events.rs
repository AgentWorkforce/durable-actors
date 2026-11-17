//! Platform events as observers read them back from the store.

use anyhow::{Result, ensure};

use super::{history::HistoryQuery, otlp::PlatformLog};

pub(crate) const EVENT_LIMIT: usize = 500;

/// The project's events with these names in a time range, of one actor class or instance when
/// named.
#[derive(Clone, Default)]
pub(crate) struct EventQuery {
    pub names: Vec<&'static str>,
    pub actor_name: Option<String>,
    pub actor_id: Option<String>,
    pub from_ms: Option<u64>,
    pub to_ms: Option<u64>,
}

impl EventQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.actor_id.is_none() || self.actor_name.is_some(),
            "actorId requires actorName"
        );
        HistoryQuery {
            actor_name: self.actor_name.clone(),
            actor_id: self.actor_id.clone(),
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            ..Default::default()
        }
        .validate()
    }
}

/// The newest events a query covers, and whether older ones were left out.
pub(crate) struct StoredEvents {
    pub events: Vec<PlatformLog>,
    pub truncated: bool,
}

impl StoredEvents {
    /// Stores read one event past the limit, so a longer history shows.
    pub(crate) fn from_read(mut events: Vec<PlatformLog>) -> Self {
        let truncated = events.len() > EVENT_LIMIT;
        events.truncate(EVENT_LIMIT);
        Self { events, truncated }
    }
}
