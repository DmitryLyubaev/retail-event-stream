CREATE TABLE projection_meta (
  id integer PRIMARY KEY CHECK (id=1),
  topic text NOT NULL,
  group_id text NOT NULL,
  fixture_sha256 text NOT NULL,
  seed_sha256 text NOT NULL,
  seed_version integer NOT NULL
);
CREATE TABLE inventory (
  product_id text PRIMARY KEY,
  balance bigint NOT NULL,
  ordinal bigint NOT NULL DEFAULT 0,
  max_occurred_at text
);
CREATE TABLE receipts (
  product_id text NOT NULL REFERENCES inventory(product_id),
  event_id uuid NOT NULL,
  raw_key bytea NOT NULL,
  raw_value bytea NOT NULL,
  fingerprint text NOT NULL,
  delta bigint NOT NULL,
  PRIMARY KEY(product_id,event_id)
);
CREATE TABLE processing (
  topic text NOT NULL,
  partition_id integer NOT NULL CHECK(partition_id>=0),
  record_offset bigint NOT NULL CHECK(record_offset>=0),
  product_id text,
  event_id uuid,
  raw_key bytea,
  raw_value bytea,
  fingerprint text NOT NULL,
  disposition text NOT NULL CHECK(disposition IN ('APPLIED','DUPLICATE','REJECTED')),
  reason text,
  late_event boolean NOT NULL,
  post_balance bigint,
  ordinal bigint,
  PRIMARY KEY(topic,partition_id,record_offset)
);
