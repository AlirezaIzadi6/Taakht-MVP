CREATE TABLE negotiation (
  id                       uuid PRIMARY KEY,
  requester_user_id        text        NOT NULL,
  requester_ad_id          text        NOT NULL,
  target_user_id           text        NOT NULL,
  target_ad_id             text        NOT NULL,
  status                   text        NOT NULL,
  active_proposal_number   int         NOT NULL,
  previous_proposal_number int,
  last_proposal_number     int         NOT NULL,
  cancel_reason            text        NOT NULL DEFAULT '',
  version                  int         NOT NULL,
  created_at               timestamptz NOT NULL,
  updated_at               timestamptz NOT NULL
);

-- At most one live negotiation per ordered ad pair.
CREATE UNIQUE INDEX negotiation_live_pair
  ON negotiation (requester_ad_id, target_ad_id)
  WHERE status IN ('OPEN', 'AGREEMENT_PENDING');
CREATE INDEX negotiation_requester_ad ON negotiation (requester_ad_id, status);
CREATE INDEX negotiation_target_ad ON negotiation (target_ad_id, status);
CREATE INDEX negotiation_requester_user ON negotiation (requester_user_id, created_at DESC);
CREATE INDEX negotiation_target_user ON negotiation (target_user_id, created_at DESC);

-- Proposals are immutable once written.
CREATE TABLE proposal (
  negotiation_id   uuid        NOT NULL REFERENCES negotiation (id),
  number           int         NOT NULL,
  id               uuid        NOT NULL UNIQUE,
  leg_a            text        NOT NULL,
  leg_b            text        NOT NULL,
  price_difference bigint      NOT NULL,
  payer_user_id    text        NOT NULL DEFAULT '',
  author_user_id   text,
  created_at       timestamptz NOT NULL,
  PRIMARY KEY (negotiation_id, number)
);

-- A new approval overwrites the previous one of the same (negotiation, user, kind).
CREATE TABLE approval (
  negotiation_id uuid        NOT NULL REFERENCES negotiation (id),
  user_id        text        NOT NULL,
  kind           text        NOT NULL,
  target         int         NOT NULL,
  updated_at     timestamptz NOT NULL,
  PRIMARY KEY (negotiation_id, user_id, kind)
);

-- Local copy of the ads, kept current from ad.events and from synchronous GetAd calls.
CREATE TABLE ad_ref (
  ad_id           text PRIMARY KEY,
  owner_id        text NOT NULL,
  current_version int  NOT NULL,
  status          text NOT NULL
);

CREATE TABLE outbox (
  id           uuid PRIMARY KEY,
  topic        text        NOT NULL,
  key          text        NOT NULL,
  envelope     bytea       NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

CREATE TABLE processed_events (
  consumer     text        NOT NULL,
  event_id     uuid        NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
