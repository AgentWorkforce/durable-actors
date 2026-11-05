use super::*;

const FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_INSERT_ROWS: usize = 1024;

pub(super) async fn run(
    client: Client,
    mut receiver: mpsc::UnboundedReceiver<Report>,
    retention: u64,
    stop: CancellationToken,
) {
    let work = async {
        retry(|| initialize(&client, retention)).await;
        while let Some(first) = receiver.recv().await {
            let mut batch = Batch::default();
            batch.push(first);
            let deadline = tokio::time::sleep(FLUSH_INTERVAL);
            tokio::pin!(deadline);
            while batch.lifecycle.len() < MAX_INSERT_ROWS {
                tokio::select! {
                    _ = &mut deadline => break,
                    report = receiver.recv() => match report {
                        Some(report) => batch.push(report),
                        None => break,
                    },
                }
            }
            retry(|| batch.write(&client)).await;
        }
    };
    tokio::select! {
        _ = work => {},
        _ = stop.cancelled() => {
            tracing::info!(queued_reports = receiver.len(), "ClickHouse lifecycle writer stopped");
        },
    }
}

pub(super) async fn initialize(client: &Client, retention: u64) -> Result<()> {
    let schema = include_str!("schema.sql").replace("RETENTION_SECONDS", &retention.to_string());
    for statement in schema
        .split(';')
        .filter(|statement| !statement.trim().is_empty())
    {
        client.query(statement).execute().await?;
    }
    Ok(())
}

async fn retry<F, Fut>(mut operation: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut delay = Duration::from_secs(1);
    loop {
        match tokio::time::timeout(WRITE_TIMEOUT, operation()).await {
            Ok(Ok(())) => return,
            error => {
                tracing::warn!(?error, "ClickHouse write failed; retaining batch for retry");
            }
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

#[derive(Default)]
struct Batch {
    lifecycle: Vec<crate::actor_lifecycle::LifecycleEvent>,
    // Released only after the batch is written, so retried rows still count against the bound.
    permits: Vec<OwnedSemaphorePermit>,
}

impl Batch {
    fn push(&mut self, report: Report) {
        self.lifecycle.push(report.lifecycle);
        self.permits.push(report.permit);
    }

    async fn write(&self, client: &Client) -> Result<()> {
        let mut insert = client
            .insert::<lifecycle::LifecycleRow>("actor_lifecycle")
            .await?;
        for event in &self.lifecycle {
            insert
                .write(&lifecycle::LifecycleRow::from_event(event)?)
                .await?;
        }
        insert.end().await?;
        Ok(())
    }
}
