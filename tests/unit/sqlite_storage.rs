use super::*;
use crate::litestream::Litestream;

#[tokio::test]
async fn capture_restores_fields_and_user_tables_at_the_requested_commit() -> Result<()> {
    let replication = Arc::new(Litestream::default());
    let mut capture = SqliteCapture::new(replication.clone(), None).await?;
    let db = rusqlite::Connection::open(capture.path())?;
    db.execute_batch(
        "BEGIN;
        INSERT INTO __terse_fields VALUES ('count','1');
        CREATE TABLE entries(value TEXT);
        INSERT INTO entries VALUES ('first');
        COMMIT;",
    )?;
    let first = replication.sync(&capture.path()).await?;
    db.execute_batch("BEGIN; UPDATE __terse_fields SET value='2'; INSERT INTO entries VALUES ('second'); COMMIT;")?;
    let second = replication.sync(&capture.path()).await?;
    assert!(second > first);
    let files = capture.capture(first).await?;
    let restored = SqliteCapture::restore(replication.clone(), &files, first, None, false).await?;
    let db = rusqlite::Connection::open(restored.path())?;
    assert_eq!(read_fields(&db)?, serde_json::json!({"count":1}));
    assert_eq!(
        db.query_row("SELECT count(*) FROM entries", [], |r| r.get::<_, i64>(0))?,
        1
    );
    let obsolete = capture
        .replica()
        .join("ltx/9")
        .join(format!("{:016x}-{first:016x}.ltx", 1));
    tokio::fs::create_dir_all(&obsolete).await?;
    let delta = capture.capture(second).await?;
    assert!(delta.iter().all(|file| file.first > first));
    let mut combined = files;
    combined.extend(delta);
    let restored = SqliteCapture::restore(replication, &combined, second, None, false).await?;
    let db = rusqlite::Connection::open(restored.path())?;
    assert_eq!(read_fields(&db)?, serde_json::json!({"count":2}));
    assert_eq!(
        db.query_row("SELECT count(*) FROM entries", [], |r| r.get::<_, i64>(0))?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn missing_replication_files_cannot_be_acknowledged() -> Result<()> {
    let replication = Arc::new(Litestream::default());
    let mut capture = SqliteCapture::new(replication.clone(), None).await?;
    let db = rusqlite::Connection::open(capture.path())?;
    db.execute("INSERT INTO __terse_fields VALUES ('count','1')", [])?;
    let position = replication.sync(&capture.path()).await?;
    let files = read_ltx_files(&capture.path(), capture.published, 0, position).await?;
    tokio::fs::remove_file(files.last().unwrap().path(&capture.replica())).await?;
    assert!(capture.capture(position).await.is_err());
    Ok(())
}

#[tokio::test]
async fn resumed_restore_captures_the_next_write_as_a_delta() -> Result<()> {
    let replication = Arc::new(Litestream::default());
    let mut capture = SqliteCapture::new(replication.clone(), None).await?;
    let db = rusqlite::Connection::open(capture.path())?;
    db.execute_batch(
        "CREATE TABLE entries(value BLOB);
        WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 64)
        INSERT INTO entries SELECT zeroblob(4000) FROM n;",
    )?;
    let first = replication.sync(&capture.path()).await?;
    let mut files = capture.capture(first).await?;
    let mut resumed =
        SqliteCapture::restore(replication.clone(), &files, first, None, true).await?;
    assert_eq!(resumed.txid(), first);
    let db = rusqlite::Connection::open(resumed.path())?;
    db.execute("INSERT INTO entries VALUES (x'01')", [])?;
    let second = replication.sync(&resumed.path()).await?;
    let delta = resumed.capture(second).await?;
    assert_eq!(delta.first().map(|file| file.first), Some(first + 1));
    let full: usize = files.iter().map(|file| file.data.len()).sum();
    assert!(delta.iter().map(|file| file.data.len()).sum::<usize>() * 8 < full);
    files.extend(delta);
    let restored = SqliteCapture::restore(replication, &files, second, None, false).await?;
    let db = rusqlite::Connection::open(restored.path())?;
    assert_eq!(
        db.query_row("SELECT count(*) FROM entries", [], |r| r.get::<_, i64>(0))?,
        65
    );
    Ok(())
}
