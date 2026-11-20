use std::{cell::RefCell, io, sync::Once};

use serde_json::Value;

thread_local! {
    static CAPTURED: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

/// Captures the JSON log lines written on this thread until dropped.
///
/// One global subscriber serves every thread. A thread-scoped subscriber can miss events when a
/// parallel test reaches the same callsite first and tracing caches it as disabled.
pub(crate) struct LogCapture(());

impl LogCapture {
    pub(crate) fn start() -> Self {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .json()
                .flatten_event(true)
                .with_max_level(tracing::Level::INFO)
                .with_writer(|| ThreadBuffer)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("log capture needs the global subscriber");
        });
        CAPTURED.with(|captured| *captured.borrow_mut() = Some(Vec::new()));
        Self(())
    }

    pub(crate) fn lines(&self) -> Vec<Value> {
        CAPTURED.with(|captured| {
            let captured = captured.borrow();
            String::from_utf8_lossy(captured.as_deref().unwrap_or_default())
                .lines()
                .map(|line| serde_json::from_str(line).expect("JSON log line"))
                .collect()
        })
    }
}

impl Drop for LogCapture {
    fn drop(&mut self) {
        CAPTURED.with(|captured| *captured.borrow_mut() = None);
    }
}

struct ThreadBuffer;

impl io::Write for ThreadBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        CAPTURED.with(|captured| {
            if let Some(buffer) = captured.borrow_mut().as_mut() {
                buffer.extend_from_slice(bytes);
            }
        });
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
