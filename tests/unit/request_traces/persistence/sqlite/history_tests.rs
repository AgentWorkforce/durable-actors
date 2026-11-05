use super::fixtures::event;
use super::*;
use crate::request_traces::history::HistoryQuery;

#[tokio::test]
async fn history_pages_survive_restart_and_exclude_late_arrivals() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("traces.sqlite3");
    let store = SqliteTracePersistence::new(path.clone());
    store.append(&[event("a"), event("b"), event("c")]).await?;
    let first = store
        .history(
            "default",
            &HistoryQuery {
                limit: 2,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(ids(&first), ["c", "b"]);
    drop(store);
    let store = SqliteTracePersistence::new(path);
    store.append(&[event("late")]).await?;
    let next = store
        .history(
            "default",
            &HistoryQuery {
                limit: 2,
                cursor: first.next_cursor,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(ids(&next), ["a"]);
    assert!(next.next_cursor.is_none());
    assert!(!next.reset);
    Ok(())
}

#[tokio::test]
async fn history_resets_when_retention_or_generation_changes() -> Result<()> {
    let store = SqliteTracePersistence {
        retention: 3,
        ..SqliteTracePersistence::in_memory()
    };
    store.append(&[event("a"), event("b"), event("c")]).await?;
    let first = store
        .history(
            "default",
            &HistoryQuery {
                limit: 1,
                ..Default::default()
            },
        )
        .await?;
    store.append(&[event("d")]).await?;
    let query = HistoryQuery {
        limit: 2,
        cursor: first.next_cursor,
        ..Default::default()
    };
    let reset = store.history("default", &query).await?;
    assert!(reset.reset);
    assert_eq!(ids(&reset), ["d", "c"]);
    let fresh = SqliteTracePersistence::in_memory();
    fresh.append(&[event("fresh")]).await?;
    let reset = fresh.history("default", &query).await?;
    assert!(reset.reset);
    assert_eq!(ids(&reset), ["fresh"]);
    Ok(())
}

#[tokio::test]
async fn history_keeps_only_retained_events_without_duplicate_appends() -> Result<()> {
    let store = SqliteTracePersistence {
        retention: 1,
        ..SqliteTracePersistence::in_memory()
    };
    store
        .append(&[event("a"), event("a"), event("b"), event("c")])
        .await?;
    let page = store.history("default", &HistoryQuery::default()).await?;
    assert_eq!(ids(&page), ["c"]);
    Ok(())
}

#[tokio::test]
async fn history_is_bounded_and_rejects_live_or_incompatible_cursors() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    store
        .append(&(0..502).map(|i| event(&i.to_string())).collect::<Vec<_>>())
        .await?;
    let store = crate::request_traces::TraceStore::new(
        Arc::new(store),
        Arc::new(crate::request_traces::live::InProcessFeed::default()),
    );
    let first = store.history("default", &HistoryQuery::default()).await?;
    assert_eq!(first.records.len(), 100);
    let largest = store
        .history(
            "default",
            &HistoryQuery {
                limit: 500,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(largest.records.len(), 500);
    assert!(largest.next_cursor.is_some());
    for query in [
        HistoryQuery {
            limit: 0,
            ..Default::default()
        },
        HistoryQuery {
            limit: 501,
            ..Default::default()
        },
        HistoryQuery {
            cursor: Some(first.resume_cursor),
            ..Default::default()
        },
        HistoryQuery {
            cursor: first.next_cursor,
            actor_id: Some("other".into()),
            ..Default::default()
        },
        HistoryQuery {
            cursor: Some("x".repeat(4097)),
            ..Default::default()
        },
    ] {
        assert!(store.history("default", &query).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn history_orders_by_event_time_then_sequence() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    let mut early = event("early");
    early.trace.started_at_ms = 0;
    store.append(&[event("a"), event("b"), early]).await?;
    let first = store
        .history(
            "default",
            &HistoryQuery {
                limit: 2,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(ids(&first), ["b", "a"]);
    let next = store
        .history(
            "default",
            &HistoryQuery {
                cursor: first.next_cursor,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(ids(&next), ["early"]);
    Ok(())
}

fn ids(page: &TracePage) -> Vec<&str> {
    page.records
        .iter()
        .map(|r| r.event.event_id.as_str())
        .collect()
}
