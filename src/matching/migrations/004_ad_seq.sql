-- Event ordering and tombstones. last_seq is the highest ad event seq applied for the ad; an event with a
-- seq at or below it is ignored. A removed ad (hidden, locked, closed, unpublished) keeps its row as a
-- tombstone (removed = true) so a replayed older event cannot bring it back. Rows from before this
-- migration keep last_seq 0 (events without a seq are applied with the old version check).
ALTER TABLE ad_index
  ADD COLUMN last_seq   bigint      NOT NULL DEFAULT 0,
  ADD COLUMN removed    boolean     NOT NULL DEFAULT false,
  ADD COLUMN removed_at timestamptz;
CREATE INDEX ad_index_live ON ad_index (updated_at DESC) WHERE NOT removed;
