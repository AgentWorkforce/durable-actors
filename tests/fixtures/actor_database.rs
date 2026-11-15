use crate::actor::{ActorDatabase, DatabaseCall, DatabaseResult, RowSink};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};

pub struct UnusedDatabase;

#[async_trait]
impl ActorDatabase for UnusedDatabase {
    async fn call(&self, _: DatabaseCall, _: Box<dyn RowSink>) -> Result<DatabaseResult> {
        anyhow::bail!("unexpected database call in test")
    }
}

pub fn unused() -> Arc<dyn ActorDatabase> {
    Arc::new(UnusedDatabase)
}

pub struct DelayedDatabase {
    pub requested: mpsc::UnboundedSender<DatabaseCall>,
    pub release: Arc<Semaphore>,
    pub fail: bool,
}

#[async_trait]
impl ActorDatabase for DelayedDatabase {
    async fn call(&self, call: DatabaseCall, _: Box<dyn RowSink>) -> Result<DatabaseResult> {
        self.requested.send(call)?;
        self.release.acquire().await?.forget();
        anyhow::ensure!(!self.fail, "storage rejected immediate write");
        Ok(DatabaseResult::Done)
    }
}
