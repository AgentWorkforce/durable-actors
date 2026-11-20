use super::*;

#[tokio::test]
async fn least_recently_used_files_are_evicted_past_capacity() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = DiskStore::open(root.path().into(), 8, u64::MAX).await?;
    for key in ["code/a", "code/b"] {
        insert(&store, key).await?;
    }
    store.entries.run_pending_tasks().await;
    assert!(store.get("code/a").await.is_some());

    insert(&store, "code/c").await?;
    store.entries.run_pending_tasks().await;

    assert!(store.get("code/a").await.is_some());
    assert!(store.get("code/b").await.is_none());
    assert!(!root.path().join("code/b").exists());
    assert_eq!(
        DiskStore::open(root.path().into(), 8, u64::MAX)
            .await?
            .existing()
            .await?
            .len(),
        2
    );
    Ok(())
}

async fn insert(store: &DiskStore, key: &str) -> Result<()> {
    let staged = store.stage()?;
    std::fs::write(staged.path(), b"four")?;
    store.insert(key, staged, 4).await
}
