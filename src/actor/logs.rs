use std::io::Write;

use serde::Deserialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }

    /// The names of this level and every more severe one.
    pub(crate) fn and_above(self) -> Vec<&'static str> {
        [Self::Debug, Self::Info, Self::Warn, Self::Error]
            .into_iter()
            .filter(|level| *level >= self)
            .map(Self::as_str)
            .collect()
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct ActorLogRecord {
    pub request_id: Option<String>,
    pub level: LogLevel,
    pub message: String,
}

pub(crate) trait ActorLogSink: Send + Sync {
    fn emit(&self, record: ActorLogRecord);
}

pub(crate) struct StdioLogSink;

impl ActorLogSink for StdioLogSink {
    fn emit(&self, record: ActorLogRecord) {
        Self::write(&record);
    }
}

impl StdioLogSink {
    pub(crate) fn write(record: &ActorLogRecord) {
        let _ = match record.level {
            LogLevel::Warn | LogLevel::Error => {
                writeln!(std::io::stderr().lock(), "{}", record.message)
            }
            LogLevel::Debug | LogLevel::Info => {
                writeln!(std::io::stdout().lock(), "{}", record.message)
            }
        };
    }
}
