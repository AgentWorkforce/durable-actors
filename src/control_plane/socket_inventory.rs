use crate::{
    actor::ActorKey,
    placement::{
        ActorConnectionInventory, ActorInstanceOverview, ActorInventory, ActorInventoryReader,
        ActorInventorySnapshot, ActorResidency,
    },
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

/// The actor inventory with each instance's open WebSocket connections, as the gateways reported
/// them through socket events.
pub(super) struct ConnectionInventoryReader {
    actors: Arc<dyn ActorInventoryReader>,
    sockets: crate::request_traces::TraceStore,
}

impl ConnectionInventoryReader {
    pub(super) fn new(
        actors: Arc<dyn ActorInventoryReader>,
        sockets: crate::request_traces::TraceStore,
    ) -> Self {
        Self { actors, sockets }
    }
}

#[async_trait]
impl ActorInventoryReader for ConnectionInventoryReader {
    async fn actor_inventory(&self, project: &str) -> Result<ActorInventorySnapshot> {
        let (actors, open) = tokio::try_join!(
            self.actors.actor_inventory(project),
            self.sockets.open_connections(project)
        )?;
        Ok(ActorInventorySnapshot {
            actors: merge_rooms(actors.actors, open.rooms),
            connections_complete: actors.connections_complete,
            gateways: open.gateways,
        })
    }

    async fn residency_history(
        &self,
        project: &str,
        query: &crate::placement::ResidencyQuery,
    ) -> Result<crate::placement::ResidencyHistory> {
        self.actors.residency_history(project, query).await
    }
}

fn merge_rooms(
    actors: Vec<ActorInventory>,
    rooms: Vec<(ActorKey, Vec<ActorConnectionInventory>)>,
) -> Vec<ActorInventory> {
    let mut actors: std::collections::BTreeMap<_, _> = actors
        .into_iter()
        .map(|actor| (actor.actor_name.clone(), actor))
        .collect();
    let mut instances: std::collections::HashMap<_, _> = actors
        .values()
        .flat_map(|actor| {
            actor.instances.iter().enumerate().map(|(index, instance)| {
                ((actor.actor_name.clone(), instance.actor_id.clone()), index)
            })
        })
        .collect();
    for (room, connections) in rooms {
        let key = (room.actor_name.clone(), room.actor_id.clone());
        let actor = actors
            .entry(room.actor_name.clone())
            .or_insert_with(|| ActorInventory {
                actor_name: room.actor_name,
                ..Default::default()
            });
        let index = *instances.entry(key).or_insert_with(|| {
            actor.dormant += 1;
            actor.instances.push(ActorInstanceOverview {
                actor_id: room.actor_id,
                status: ActorResidency::Dormant,
                last_active_at_ms: None,
                reported_at_ms: None,
                stale_at_ms: None,
                connections: vec![],
                waiting: None,
            });
            actor.instances.len() - 1
        });
        actor.instances[index].connections = connections;
    }
    actors.into_values().collect()
}
