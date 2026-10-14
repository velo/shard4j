CREATE TABLE failure_signature (
  id INTEGER PRIMARY KEY,
  fingerprint TEXT NOT NULL UNIQUE,
  exception_class TEXT NOT NULL,
  message TEXT,
  stack_trace TEXT NOT NULL,
  first_seen INTEGER NOT NULL,
  last_seen INTEGER NOT NULL
);

-- kind: UNIT (a leaf lease unit), TEMPLATE (a template leased whole, outcome aggregated
-- over its rows) or INVOCATION (one row of a TEMPLATE). ts is epoch milliseconds.
CREATE TABLE test_attempt (
  id INTEGER PRIMARY KEY,
  session TEXT NOT NULL,
  test_id TEXT NOT NULL,
  history_key TEXT NOT NULL,
  kind TEXT NOT NULL,
  shard INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  outcome TEXT NOT NULL,
  duration_ms INTEGER NOT NULL,
  reason TEXT,
  signature_id INTEGER REFERENCES failure_signature (id),
  ts INTEGER NOT NULL
);

CREATE INDEX test_attempt_ts ON test_attempt (ts);
CREATE INDEX test_attempt_unit ON test_attempt (session, test_id, attempt);
CREATE INDEX test_attempt_key ON test_attempt (history_key, ts);
CREATE INDEX test_attempt_signature ON test_attempt (signature_id);
