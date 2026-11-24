use super::*;
use std::sync::Mutex as StdMutex;

const DAY: Duration = Duration::from_secs(24 * 3600);

struct Memory {
    objects: StdMutex<BTreeMap<String, (SystemTime, Payload)>>,
    deleted: StdMutex<Vec<(Option<String>, String)>>,
    page_size: usize,
}

impl Memory {
    fn new(page_size: usize) -> Self {
        Self {
            objects: StdMutex::new(BTreeMap::new()),
            deleted: StdMutex::new(Vec::new()),
            page_size,
        }
    }

    fn put(&self, name: String, age: Duration, bytes: Payload) {
        let created = SystemTime::now() - age;
        self.objects.lock().unwrap().insert(name, (created, bytes));
    }

    fn names(&self) -> Vec<String> {
        self.objects.lock().unwrap().keys().cloned().collect()
    }
}

#[async_trait]
impl SweepStore for Memory {
    async fn page(&self, prefix: &str, page: &str) -> Result<(Vec<Listed>, String)> {
        let objects = self.objects.lock().unwrap();
        let listed: Vec<Listed> = objects
            .iter()
            .filter(|(name, _)| name.starts_with(prefix) && name.as_str() > page)
            .take(self.page_size)
            .map(|(name, (created, bytes))| Listed {
                name: name.clone(),
                created: *created,
                bytes: bytes.len() as u64,
            })
            .collect();
        let next = match listed.last() {
            Some(last) if listed.len() == self.page_size => last.name.clone(),
            _ => String::new(),
        };
        Ok((listed, next))
    }

    async fn get(&self, name: &str) -> Result<Option<Payload>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(name)
            .map(|(_, bytes)| bytes.clone()))
    }

    async fn remember(&self, key: &str, bytes: Payload) -> Result<()> {
        self.put(key.into(), Duration::ZERO, bytes);
        Ok(())
    }

    fn allows_replica(&self, bucket: &str) -> bool {
        bucket == "projects/_/buckets/rapid-a"
    }

    async fn delete(&self, bucket: Option<&str>, name: &str, _: Option<i64>) -> Result<()> {
        self.deleted
            .lock()
            .unwrap()
            .push((bucket.map(str::to_owned), name.to_owned()));
        if bucket.is_none() {
            self.objects.lock().unwrap().remove(name);
        }
        Ok(())
    }
}

fn name(actor: &str, epoch: u64, rest: &str) -> String {
    format!("{SNAPSHOTS}{actor}~{epoch:032x}~{rest}")
}

fn base(actor: &str, epoch: u64, version: u64) -> String {
    format!("{SNAPSHOTS}{actor}~bases~{epoch:032x}~{version}~{version}~100.sqlite")
}

fn manifest(id: &str) -> Payload {
    manifest_at(id, 7)
}

fn manifest_at(id: &str, generation: i64) -> Payload {
    let manifest = Manifest {
        format: 1,
        id: id.into(),
        stream: StateStream {
            session: "session".into(),
            prefix: "prefix/".into(),
            owner_epoch: 1,
            base_version: 0,
        },
        first_version: 1,
        replicas: vec![Replica {
            bucket: "projects/_/buckets/rapid-a".into(),
            object: format!("durable-actors-v3-logs-aaa~{id}.segment"),
            generation,
        }],
    };
    serde_json::to_vec(&manifest).unwrap().into()
}

/// Actor `aaa` has bases at versions 2, 5 and 9 in epochs 1, 2 and 3; `bbb` has one base.
fn history(store: &Memory, recent_in_first_epoch: bool) {
    let old = 40 * DAY;
    store.put(name("aaa", 1, "1.json"), old, b"head".as_slice().into());
    store.put(name("aaa", 1, "log~s1.manifest"), old, manifest("s1"));
    store.put(
        name("aaa", 1, "index~x.idx"),
        old,
        b"index".as_slice().into(),
    );
    if recent_in_first_epoch {
        store.put(name("aaa", 1, "2.json"), DAY, b"head".as_slice().into());
    }
    store.put(name("aaa", 2, "5.json"), old, b"head".as_slice().into());
    store.put(name("aaa", 3, "9.json"), old, b"head".as_slice().into());
    store.put(base("aaa", 1, 2), old, b"base".as_slice().into());
    store.put(base("aaa", 2, 5), old, b"base".as_slice().into());
    store.put(base("aaa", 3, 9), DAY, b"base".as_slice().into());
    store.put(name("bbb", 1, "1.json"), old, b"head".as_slice().into());
    store.put(base("bbb", 1, 1), old, b"base".as_slice().into());
}

#[tokio::test]
async fn deletes_what_the_older_kept_base_covers_once_old_enough() -> Result<()> {
    let store = Memory::new(2);
    history(&store, false);
    let summary = sweep(&store, SystemTime::now() - 30 * DAY, false).await?;
    assert_eq!(summary.actors, 2);
    assert_eq!(summary.objects, 4);
    assert_eq!(
        store.names(),
        [
            name("aaa", 2, "5.json"),
            name("aaa", 3, "9.json"),
            base("aaa", 2, 5),
            base("aaa", 3, 9),
            name("bbb", 1, "1.json"),
            base("bbb", 1, 1),
        ]
    );
    let deleted = store.deleted.lock().unwrap();
    assert_eq!(
        deleted.first(),
        Some(&(None, name("aaa", 1, "log~s1.manifest")))
    );
    Ok(())
}

#[tokio::test]
async fn keeps_an_epoch_that_holds_anything_younger_than_the_lookback() -> Result<()> {
    let store = Memory::new(3);
    history(&store, true);
    sweep(&store, SystemTime::now() - 30 * DAY, false).await?;
    let names = store.names();
    assert!(names.contains(&name("aaa", 1, "log~s1.manifest")));
    assert!(names.contains(&base("aaa", 1, 2)));
    Ok(())
}

#[tokio::test]
async fn a_dry_run_counts_without_deleting() -> Result<()> {
    let store = Memory::new(5);
    history(&store, false);
    let before = store.names();
    let summary = sweep(&store, SystemTime::now() - 30 * DAY, true).await?;
    assert_eq!(summary.objects, 4);
    assert_eq!(store.names(), before);
    assert!(store.deleted.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn resumes_replica_cleanup_after_the_manifest_was_unlisted() -> Result<()> {
    let store = Memory::new(2);
    history(&store, false);
    let key = name("aaa", 1, "log~s1.manifest");
    store
        .remember(&format!("{key}.sweeping"), manifest("s1"))
        .await?;
    store.delete(None, &key, None).await?;
    sweep(&store, SystemTime::now() - 30 * DAY, false).await?;
    assert!(!store.names().contains(&format!("{key}.sweeping")));
    assert!(store.deleted.lock().unwrap().contains(&(
        Some("projects/_/buckets/rapid-a".into()),
        "durable-actors-v3-logs-aaa~s1.segment".into()
    )));
    Ok(())
}

#[tokio::test]
async fn keeps_old_history_through_the_first_rollback_window() -> Result<()> {
    let store = Memory::new(3);
    history(&store, false);
    for (epoch, version) in [(1, 2), (2, 5), (3, 9)] {
        store.put(base("aaa", epoch, version), DAY, Payload::default());
    }
    assert_eq!(
        sweep(&store, SystemTime::now() - 30 * DAY, false)
            .await?
            .objects,
        0
    );
    Ok(())
}

#[tokio::test]
async fn skips_an_actor_whose_history_fails_and_sweeps_the_rest() -> Result<()> {
    let store = Memory::new(2);
    history(&store, false);
    for (epoch, version, age) in [(1, 2, 40 * DAY), (2, 5, 40 * DAY), (3, 9, DAY)] {
        store.put(base("a00", epoch, version), age, b"base".as_slice().into());
    }
    let foreign = name("a00", 1, "log~s9.manifest");
    store.put(foreign.clone(), 40 * DAY, manifest("s9"));
    store.put(
        name("aaa", 1, "log~s1.manifest"),
        40 * DAY,
        manifest_at("s1", 0),
    );
    let summary = sweep(&store, SystemTime::now() - 30 * DAY, false).await?;
    assert_eq!(summary.failed, 1);
    assert!(store.names().contains(&foreign));
    assert!(store.deleted.lock().unwrap().contains(&(
        Some("projects/_/buckets/rapid-a".into()),
        "durable-actors-v3-logs-aaa~s1.segment".into()
    )));
    Ok(())
}
