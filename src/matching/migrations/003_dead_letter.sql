-- Events a consumer skipped permanently (undecodable or rejected for good); kept for inspection and manual replay.
CREATE TABLE dead_letter (
  consumer   text        NOT NULL,
  event_id   uuid        NOT NULL,
  topic      text        NOT NULL,
  payload    bytea       NOT NULL, -- the serialized Envelope
  error      text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE INDEX dead_letter_created_at ON dead_letter (created_at);
