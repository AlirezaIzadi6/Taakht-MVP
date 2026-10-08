CREATE TABLE swap (
  id               uuid PRIMARY KEY,
  negotiation_id   text        NOT NULL UNIQUE,
  ad_a_id          text        NOT NULL,
  ad_a_version     integer     NOT NULL,
  ad_b_id          text        NOT NULL,
  ad_b_version     integer     NOT NULL,
  status           text        NOT NULL,
  leg_a_owner      text        NOT NULL,
  leg_a_method     text        NOT NULL,
  leg_a_fee_paid   boolean     NOT NULL DEFAULT false,
  leg_b_owner      text        NOT NULL,
  leg_b_method     text        NOT NULL,
  leg_b_fee_paid   boolean     NOT NULL DEFAULT false,
  payment_deadline timestamptz,
  cancel_reason    text        NOT NULL DEFAULT '',
  created_at       timestamptz NOT NULL,
  updated_at       timestamptz NOT NULL
);
CREATE INDEX swap_leg_a_owner ON swap (leg_a_owner);
CREATE INDEX swap_leg_b_owner ON swap (leg_b_owner);
CREATE INDEX swap_awaiting_deadline ON swap (payment_deadline) WHERE status = 'AWAITING_PAYMENT';

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
