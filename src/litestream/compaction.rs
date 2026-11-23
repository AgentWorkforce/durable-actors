use super::storage::LtxFile;
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use std::{
    io::{self, Read},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

// Compaction reads every input at once, so their buffers share one budget.
const READ_BUDGET_BYTES: usize = 4 * 1024 * 1024;
const MIN_READ_BYTES: usize = 8 * 1024;
const COMPACTION_TIMEOUT: Duration = Duration::from_secs(120);

#[async_trait]
pub(crate) trait LtxCompactor: Send + Sync {
    async fn compact(&self, files: &[LtxFile]) -> Result<LtxFile>;
}

pub(crate) struct RustCompactor;

#[async_trait]
impl LtxCompactor for RustCompactor {
    async fn compact(&self, files: &[LtxFile]) -> Result<LtxFile> {
        let files = files.to_vec();
        let cancel = CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        tokio::task::spawn_blocking(move || compact(files, cancel)).await?
    }
}

fn compact(files: Vec<LtxFile>, cancel: CancellationToken) -> Result<LtxFile> {
    let last = files.last().context("empty checkpoint")?.last;
    let input_bytes = files.iter().map(|file| file.data.len() as u64).sum();
    let deadline =
        Instant::now() + crate::bucket::transfer_timeout(COMPACTION_TIMEOUT, input_bytes);
    let capacity =
        (READ_BUDGET_BYTES / files.len()).clamp(MIN_READ_BYTES, crate::payload::BUFFER_BYTES);
    let readers = files
        .into_iter()
        .map(|file| {
            Ok(CompactionInput {
                reader: file.data.reader_with_capacity(capacity),
                cancel: cancel.clone(),
                deadline,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut data = crate::payload::Spool::new();
    let header = terse_ltx::compact(readers, &mut data).context("compact LTX checkpoint")?;
    ensure!(
        header.min_txid == 1 && header.max_txid == last,
        "compacted checkpoint transaction mismatch"
    );
    Ok(LtxFile {
        level: 9,
        first: header.min_txid,
        last: header.max_txid,
        data: data.finish()?,
    })
}

struct CompactionInput {
    reader: io::BufReader<crate::payload::PayloadReader>,
    cancel: CancellationToken,
    deadline: Instant,
}

impl Read for CompactionInput {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.cancel.is_cancelled() {
            return Err(io::Error::other("LTX compaction cancelled"));
        }
        if Instant::now() >= self.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "LTX compaction timed out",
            ));
        }
        self.reader.read(bytes)
    }
}
