use super::*;
use crate::{litestream::MemoryReplica, payload::Payload};

const PAGE: u32 = 512;

/// A database history: one LTX file per transaction, from a full copy at transaction 1.
struct History {
    pages: Vec<Vec<u8>>,
    files: Vec<LtxFile>,
    seed: u64,
}

impl History {
    fn new(seed: u64, pages: u32) -> Self {
        let mut history = Self {
            pages: Vec::new(),
            files: Vec::new(),
            seed,
        };
        let numbers: Vec<u32> = (1..=pages).collect();
        history.pages = numbers.iter().map(|_| history.page()).collect();
        history.commit(&numbers);
        history
    }

    fn txid(&self) -> u64 {
        self.files.len() as u64
    }

    fn random(&mut self) -> u64 {
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        self.seed
    }

    fn page(&mut self) -> Vec<u8> {
        (0..PAGE).map(|_| self.random() as u8).collect()
    }

    /// Rewrites `count` random pages, or only the first `hot` pages when given.
    fn rewrite(&mut self, count: usize, hot: Option<u32>) {
        let size = self.pages.len() as u64;
        let mut numbers: Vec<u32> = (0..count)
            .map(|_| 1 + (self.random() % hot.map_or(size, |hot| size.min(hot.into()))) as u32)
            .collect();
        numbers.push(1);
        for &number in &numbers {
            self.pages[number as usize - 1] = self.page();
        }
        self.commit(&numbers);
    }

    fn grow(&mut self, count: u32) {
        let first = self.pages.len() as u32 + 1;
        for _ in 0..count {
            let page = self.page();
            self.pages.push(page);
        }
        let mut numbers: Vec<u32> = (first..first + count).collect();
        numbers.push(1);
        self.commit(&numbers);
    }

    fn shrink(&mut self, to: u32) {
        self.pages.truncate(to as usize);
        self.pages[0] = self.page();
        self.commit(&[1]);
    }

    fn commit(&mut self, numbers: &[u32]) {
        // Page 1 carries the page size where SQLite's header does.
        self.pages[0][16..18].copy_from_slice(&(PAGE as u16).to_be_bytes());
        let mut numbers = numbers.to_vec();
        numbers.sort_unstable();
        numbers.dedup();
        let txid = self.txid() + 1;
        let header = terse_ltx::Header {
            flags: terse_ltx::NO_CHECKSUM,
            page_size: PAGE,
            commit: self.pages.len() as u32,
            min_txid: txid,
            max_txid: txid,
            timestamp: 1,
            ..Default::default()
        };
        let mut encoder = terse_ltx::Encoder::new(Vec::new(), header).unwrap();
        for number in numbers {
            encoder
                .write_page(number, &self.pages[number as usize - 1])
                .unwrap();
        }
        self.files.push(LtxFile {
            level: 0,
            first: txid,
            last: txid,
            data: Payload::from(encoder.finish(0).unwrap()),
        });
    }

    fn evolve(&mut self, steps: usize) {
        for _ in 0..steps {
            let (kind, amount) = (self.random() % 6, self.random());
            match kind {
                0 => self.grow(1 + (amount % 8) as u32),
                1 if self.pages.len() > 4 => {
                    self.shrink(2 + (amount % (self.pages.len() as u64 - 2)) as u32)
                }
                2 => self.rewrite(4, Some(3)),
                _ => self.rewrite(1 + (amount % 6) as usize, None),
            }
        }
    }

    fn database(&self) -> Vec<u8> {
        self.pages.concat()
    }
}

fn sequential(files: &[LtxFile], txid: u64, path: &Path) {
    let replica = MemoryReplica::decode(files).unwrap();
    terse_litestream::restore_with(
        &replica,
        path,
        Some(txid),
        terse_litestream::Durability::Scratch,
    )
    .unwrap();
}

#[test]
fn newest_first_matches_a_sequential_restore() {
    for seed in [7, 11, 2026] {
        let mut history = History::new(seed, 24);
        history.evolve(60);
        let directory = tempfile::tempdir().unwrap();
        let (expected, actual) = (directory.path().join("a"), directory.path().join("b"));
        sequential(&history.files, history.txid(), &expected);
        apply(&actual, None, &history.files, history.txid()).unwrap();
        assert_eq!(
            std::fs::read(&actual).unwrap(),
            std::fs::read(&expected).unwrap()
        );
        assert_eq!(std::fs::read(&actual).unwrap(), history.database());
    }
}

#[test]
fn applies_the_changes_after_a_base() {
    let mut history = History::new(42, 32);
    history.evolve(40);
    let base = history.txid();
    history.evolve(40);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db");
    sequential(&history.files[..base as usize], base, &path);
    let applied = apply(
        &path,
        Some(base),
        &history.files[base as usize..],
        history.txid(),
    );
    assert_eq!(applied.unwrap().files, 40);
    assert_eq!(std::fs::read(&path).unwrap(), history.database());
}

#[test]
fn writes_each_page_once() {
    let mut history = History::new(3, 64);
    for _ in 0..200 {
        history.rewrite(4, Some(3));
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db");
    let applied = apply(&path, None, &history.files, history.txid()).unwrap();
    assert_eq!(applied.pages, 64);
    assert_eq!(std::fs::read(&path).unwrap(), history.database());
}

#[test]
fn keeps_a_shrink_that_newer_files_do_not_undo() {
    let mut history = History::new(5, 10);
    history.shrink(5);
    history.grow(3);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db");
    apply(&path, None, &history.files, history.txid()).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), history.database());
}

#[test]
fn refuses_a_gap_in_the_history() {
    let mut history = History::new(9, 8);
    history.evolve(5);
    let mut files = history.files.clone();
    files.remove(3);
    let directory = tempfile::tempdir().unwrap();
    let error = apply(&directory.path().join("db"), None, &files, history.txid()).unwrap_err();
    assert!(error.to_string().contains("gap"), "{error:#}");
}

#[test]
fn refuses_to_restore_over_an_existing_file_without_a_base() {
    let history = History::new(1, 4);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db");
    std::fs::write(&path, b"stale").unwrap();
    assert!(apply(&path, None, &history.files, history.txid()).is_err());
}
