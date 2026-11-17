//! The actor each sandbox was claimed for. A claim is the only record that ties a sandbox to an
//! actor, so what watches sandboxes from outside reads the actor here.

use anyhow::Result;
use async_trait::async_trait;

use crate::{actor::ActorKey, postgres::PostgresDatabase};

#[async_trait]
pub(crate) trait ClaimedActors: Send + Sync {
    /// The actor the sandbox was claimed for; none for a spare nobody claimed.
    async fn actor_of(&self, sandbox: &str) -> Result<Option<ActorKey>>;
}

pub(crate) struct PoolClaims(pub PostgresDatabase);

#[async_trait]
impl ClaimedActors for PoolClaims {
    async fn actor_of(&self, sandbox: &str) -> Result<Option<ActorKey>> {
        let row = self
            .0
            .query_opt(
                "SELECT project_id, actor_name, actor_id FROM durable_actors_spares WHERE name = $1",
                &[&sandbox],
            )
            .await?;
        Ok(row.and_then(|row| {
            Some(ActorKey {
                project_id: row.get::<_, Option<String>>(0)?,
                actor_name: row.get::<_, Option<String>>(1)?,
                actor_id: row.get::<_, Option<String>>(2)?,
            })
        }))
    }
}

/// The claim's actor columns, in the order the statements that write them bind them.
pub(super) fn columns(actor: Option<&ActorKey>) -> [Option<&str>; 3] {
    match actor {
        Some(actor) => [
            Some(&actor.project_id),
            Some(&actor.actor_name),
            Some(&actor.actor_id),
        ],
        None => [None; 3],
    }
}
