-- The actor each sandbox was claimed for. A stopped sandbox cannot say which actor it ran.
ALTER TABLE durable_actors_spares
    ADD COLUMN project_id TEXT,
    ADD COLUMN actor_name TEXT,
    ADD COLUMN actor_id TEXT;
