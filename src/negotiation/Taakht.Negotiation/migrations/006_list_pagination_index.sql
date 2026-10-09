-- Keyset pagination of ListNegotiations: ORDER BY created_at DESC, id DESC per party.
CREATE INDEX IF NOT EXISTS negotiation_requester_user_page ON negotiation (requester_user_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS negotiation_target_user_page ON negotiation (target_user_id, created_at DESC, id DESC);
