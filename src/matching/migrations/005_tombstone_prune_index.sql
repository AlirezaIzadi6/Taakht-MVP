-- Support the tombstone pruner (TOMBSTONE_RETENTION).
CREATE INDEX IF NOT EXISTS ad_index_tombstones ON ad_index (removed_at) WHERE removed;
