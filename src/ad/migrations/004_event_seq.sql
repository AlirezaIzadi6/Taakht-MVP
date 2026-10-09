-- Per-ad event counter. Every event the service writes for an ad carries the next value, so consumers
-- can tell a replayed or reordered event from a newer one. Rows that existed before start at 0.
ALTER TABLE ad ADD COLUMN event_seq bigint NOT NULL DEFAULT 0;
