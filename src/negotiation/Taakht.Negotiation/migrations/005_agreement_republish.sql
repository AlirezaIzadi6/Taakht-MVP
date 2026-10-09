-- AGREEMENT_PENDING escape: the agreed ad versions are kept on the row so AgreementReached can be published again,
-- and the sweeper counts how often that already happened.
ALTER TABLE negotiation
  ADD COLUMN agreed_requester_version int,
  ADD COLUMN agreed_target_version    int,
  ADD COLUMN republish_count          int NOT NULL DEFAULT 0,
  ADD COLUMN last_republished_at      timestamptz;
CREATE INDEX negotiation_agreement_pending ON negotiation (updated_at) WHERE status = 'AGREEMENT_PENDING';
