-- Keyset pagination of ListMySwaps: (created_at, id) < cursor ORDER BY created_at DESC, id DESC per leg owner.
CREATE INDEX IF NOT EXISTS swap_leg_a_owner_created ON swap (leg_a_owner, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS swap_leg_b_owner_created ON swap (leg_b_owner, created_at DESC, id DESC);
