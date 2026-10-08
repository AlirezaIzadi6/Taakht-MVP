CREATE TABLE ad_index (
  ad_id            uuid PRIMARY KEY,
  owner_id         text        NOT NULL,
  status           text        NOT NULL,
  version          integer     NOT NULL,
  have_category    text        NOT NULL,
  want_categories  text[]      NOT NULL DEFAULT '{}',
  neighborhood_ids text[]      NOT NULL DEFAULT '{}',
  title            text        NOT NULL DEFAULT '',
  value_estimate   bigint      NOT NULL DEFAULT 0,
  snapshot         bytea       NOT NULL,
  updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ad_index_have_category ON ad_index (have_category);
CREATE INDEX ad_index_owner ON ad_index (owner_id);

CREATE TABLE processed_events (
  consumer     text        NOT NULL,
  event_id     uuid        NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
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

CREATE TABLE notified_pair (
  ad_id         uuid        NOT NULL,
  matched_ad_id uuid        NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ad_id, matched_ad_id)
);
