use std::{convert::Infallible, time::Duration};

use axum::response::sse::Event;
use futures_util::StreamExt;
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, trace::v1::ExportTraceServiceRequest,
};
use prost::Message;
use tokio::sync::mpsc;

use crate::request_traces::{
    TracePage, TraceRecord, TraceStore, live::LiveStream, logs::LogQuery, otlp, replay::ReplayQuery,
};

type Events = mpsc::Sender<Result<Event, Infallible>>;

pub(super) async fn requests(
    store: TraceStore,
    project: String,
    mut updates: LiveStream,
    sender: Events,
    mut page: TracePage,
) {
    let interval = Duration::from_secs(30);
    let mut reconcile = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    if !send_page(&sender, &page).await {
        return;
    }
    loop {
        let update = tokio::select! {
            _ = sender.closed() => return,
            _ = reconcile.tick() => None,
            update = updates.next() => match update {
                Some(update) => Some(update),
                None => return,
            },
        };
        if let Some(update) = update {
            if !apply_batch(&mut page, &update, &project) {
                continue;
            }
        } else {
            page = match store.replay(&project, &ReplayQuery::default()).await {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!(%error, "Live trace reconciliation failed");
                    let _ = sender
                        .send(Ok(Event::default()
                            .event("error")
                            .data("Request history unavailable")))
                        .await;
                    return;
                }
            };
        }
        if !send_page(&sender, &page).await {
            return;
        }
    }
}

pub(super) async fn logs(
    mut updates: LiveStream,
    filter: LogQuery,
    project: String,
    sender: Events,
) {
    loop {
        let update = tokio::select! {
            _ = sender.closed() => return,
            update = updates.next() => match update {
                Some(update) => update,
                None => return,
            },
        };
        let Ok(request) = ExportLogsServiceRequest::decode(update.as_slice()) else {
            continue;
        };
        let records: Vec<_> = otlp::actor_logs(request)
            .into_iter()
            .filter(|log| log.project_id == project && filter.matches(log))
            .collect();
        if records.is_empty() {
            continue;
        }
        let event = Event::default()
            .event("logs")
            .json_data(serde_json::json!({ "records": records }))
            .expect("serializable actor logs");
        if sender.send(Ok(event)).await.is_err() {
            return;
        }
    }
}

fn apply_batch(page: &mut TracePage, update: &[u8], project: &str) -> bool {
    let Ok(request) = ExportTraceServiceRequest::decode(update) else {
        return false;
    };
    page.records = otlp::trace_events(request)
        .into_iter()
        .filter(|event| event.trace.project_id == project)
        .map(|event| TraceRecord {
            sequence: event.trace.started_at_ms,
            event,
        })
        .collect();
    let Some(cursor) = page.records.iter().map(|record| record.sequence).max() else {
        return false;
    };
    page.cursor = cursor;
    page.snapshot = false;
    true
}

async fn send_page(sender: &Events, page: &TracePage) -> bool {
    sender
        .send(Ok(Event::default()
            .event("requests")
            .id(&page.resume_cursor)
            .json_data(page)
            .expect("serializable trace page")))
        .await
        .is_ok()
}
