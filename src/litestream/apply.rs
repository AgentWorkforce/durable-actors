use super::storage::LtxFile;
use anyhow::{Context, Result, ensure};
use std::{fs::File, os::unix::fs::FileExt, path::Path};

/// Pages gathered into one write, so a run of pages costs one system call.
const WRITE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Applied {
    pub files: usize,
    pub pages: u64,
    pub bytes: u64,
}

/// Restores the database at `path` to `txid` from `files`, newest file first, so each page is
/// written once: from the newest file that holds it. `base` is the transaction an existing file
/// already holds; without one the file is created and the files must start at a full copy.
pub(crate) fn apply(
    path: &Path,
    base: Option<u64>,
    files: &[LtxFile],
    txid: u64,
) -> Result<Applied> {
    let plan = plan(base, files, txid)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(base.is_none())
        .open(path)
        .context("open SQLite database to apply LTX files")?;
    let Some(newest) = plan.last() else {
        return Ok(Applied::default());
    };
    let format = *terse_ltx::Decoder::new(newest.data.reader())?.header();
    if base.is_some() {
        ensure!(
            database_page_size(&file)? == format.page_size,
            "base page size differs from its LTX files"
        );
    }
    let mut pages = PageWriter::new(&file, format.page_size);
    let mut limit = u32::MAX;
    for ltx in plan.iter().rev() {
        limit = apply_file(ltx, &format, limit, &mut pages)?;
    }
    let mut applied = pages.finish()?;
    applied.files = plan.len();
    file.set_len(u64::from(format.commit) * u64::from(format.page_size))?;
    Ok(applied)
}

/// Whether newest-first application can stand in for a sequential restore, which also checks
/// the transaction checksums that some LTX files carry.
pub(crate) fn supported(files: &[LtxFile]) -> Result<bool> {
    for ltx in files {
        let decoder = terse_ltx::Decoder::new(ltx.data.reader())?;
        if decoder.header().flags & terse_ltx::NO_CHECKSUM == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The files a sequential restore would apply: from `base`, each step the file that reaches
/// furthest without passing `txid`.
fn plan(base: Option<u64>, files: &[LtxFile], txid: u64) -> Result<Vec<&LtxFile>> {
    let mut end = base.unwrap_or(0);
    ensure!(end <= txid, "base is newer than the restore target");
    let mut plan = Vec::new();
    while end < txid {
        let next = end + 1;
        let ltx = files
            .iter()
            .filter(|ltx| ltx.first <= next && ltx.last > end && ltx.last <= txid)
            .max_by_key(|ltx| (ltx.last, ltx.level))
            .with_context(|| format!("LTX files have a gap after transaction {end}"))?;
        ensure!(
            end > 0 || ltx.first == 1,
            "LTX files start without a full copy"
        );
        ltx.validate()?;
        end = ltx.last;
        plan.push(ltx);
    }
    ensure!(
        base.is_some() || !plan.is_empty(),
        "no LTX files to restore"
    );
    Ok(plan)
}

/// Writes the pages of one file that no newer file already wrote and that no newer file
/// truncated away, and returns the page limit for older files.
fn apply_file(
    ltx: &LtxFile,
    format: &terse_ltx::Header,
    limit: u32,
    pages: &mut PageWriter,
) -> Result<u32> {
    let mut decoder = terse_ltx::Decoder::new(ltx.data.reader())?;
    let header = *decoder.header();
    ensure!(
        header.min_txid == ltx.first && header.max_txid == ltx.last,
        "LTX header and range disagree"
    );
    ensure!(
        header.page_size == format.page_size && header.flags == format.flags,
        "LTX format changed within the restore"
    );
    let limit = limit.min(header.commit);
    while let Some(page) = decoder.next_page()? {
        if page.number <= limit {
            pages.write(page.number, &page.data)?;
        }
    }
    Ok(limit)
}

fn database_page_size(file: &File) -> Result<u32> {
    let mut header = [0; 2];
    file.read_exact_at(&mut header, 16)
        .context("read base page size")?;
    Ok(match u16::from_be_bytes(header) {
        1 => 65536,
        size => u32::from(size),
    })
}

struct PageWriter<'a> {
    file: &'a File,
    page_size: u64,
    written: Vec<bool>,
    run_start: u32,
    run: Vec<u8>,
    applied: Applied,
}

impl<'a> PageWriter<'a> {
    fn new(file: &'a File, page_size: u32) -> Self {
        Self {
            file,
            page_size: u64::from(page_size),
            written: Vec::new(),
            run_start: 0,
            run: Vec::with_capacity(WRITE_BYTES),
            applied: Applied::default(),
        }
    }

    fn write(&mut self, number: u32, data: &[u8]) -> Result<()> {
        let index = number as usize;
        if self.written.len() <= index {
            self.written.resize(index + 1, false);
        }
        if std::mem::replace(&mut self.written[index], true) {
            return Ok(());
        }
        let next = u64::from(self.run_start) + self.run.len() as u64 / self.page_size;
        if self.run.is_empty() || u64::from(number) != next || self.run.len() >= WRITE_BYTES {
            self.flush()?;
            self.run_start = number;
        }
        self.run.extend_from_slice(data);
        self.applied.pages += 1;
        self.applied.bytes += data.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if !self.run.is_empty() {
            let offset = u64::from(self.run_start - 1) * self.page_size;
            self.file.write_all_at(&self.run, offset)?;
            self.run.clear();
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Applied> {
        self.flush()?;
        Ok(self.applied)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/litestream/apply.rs"]
mod tests;
