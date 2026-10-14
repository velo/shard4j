CREATE TABLE distinct_failure (
  id INTEGER PRIMARY KEY,
  fingerprint TEXT NOT NULL UNIQUE,
  exception_class TEXT NOT NULL,
  message TEXT,
  stack_trace TEXT NOT NULL,
  first_seen_ms INTEGER NOT NULL,
  last_seen_ms INTEGER NOT NULL
);

-- unit_kind: UNIT (a leaf lease unit), TEMPLATE (a template leased whole; its outcome is
-- aggregated over its rows) or INVOCATION (one row of a TEMPLATE).
CREATE TABLE test_attempt (
  id INTEGER PRIMARY KEY,
  session_id TEXT NOT NULL,
  test_id TEXT NOT NULL,
  history_key TEXT NOT NULL,
  unit_kind TEXT NOT NULL,
  shard INTEGER NOT NULL,
  attempt_number INTEGER NOT NULL,
  outcome TEXT NOT NULL,
  duration_ms INTEGER NOT NULL,
  reason TEXT,
  distinct_failure_id INTEGER REFERENCES distinct_failure (id),
  recorded_at_ms INTEGER NOT NULL
);

CREATE INDEX test_attempt_recorded_at ON test_attempt (recorded_at_ms);
CREATE INDEX test_attempt_unit ON test_attempt (session_id, test_id, attempt_number);
CREATE INDEX test_attempt_history_key ON test_attempt (history_key, recorded_at_ms);
CREATE INDEX test_attempt_distinct_failure ON test_attempt (distinct_failure_id);
