-- Support the housekeeping pruner (outbox and processed_events retention).
CREATE INDEX IF NOT EXISTS outbox_published ON outbox (published_at) WHERE published_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS processed_events_processed_at ON processed_events (processed_at);
