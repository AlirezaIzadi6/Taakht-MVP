CREATE TABLE ad (
  id                  uuid PRIMARY KEY,
  owner_id            text        NOT NULL,
  status              text        NOT NULL CHECK (status IN ('published', 'hidden', 'locked', 'closed')),
  current_version     int         NOT NULL,
  status_before_lock  text        CHECK (status_before_lock IN ('published', 'hidden')),
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ad_owner ON ad (owner_id, created_at);

-- Append-only: one immutable snapshot of the spec per version.
CREATE TABLE ad_version (
  ad_id       uuid        NOT NULL REFERENCES ad (id),
  version     int         NOT NULL,
  spec        jsonb       NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ad_id, version)
);

-- One row per ad claimed by a swap; the (swap_id, ad_id) key makes LockAds idempotent.
CREATE TABLE ad_lock (
  swap_id     text        NOT NULL,
  ad_id       uuid        NOT NULL REFERENCES ad (id),
  version     int         NOT NULL,
  state       text        NOT NULL DEFAULT 'locked' CHECK (state IN ('locked', 'released', 'closed')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (swap_id, ad_id)
);
CREATE INDEX ad_lock_ad ON ad_lock (ad_id);

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
