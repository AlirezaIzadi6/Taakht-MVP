-- Keyset pagination of ListMyAds: WHERE owner_id = $1 AND (created_at, id) < ($2, $3) ORDER BY created_at DESC, id DESC.
CREATE INDEX IF NOT EXISTS ad_owner_created ON ad (owner_id, created_at DESC, id DESC);
