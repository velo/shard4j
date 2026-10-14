package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** A WHERE fragment over {@code test_attempt} that carries its own bind values. */
record Filter(String sql, List<Object> params) {

  static Filter window(Instant from, Instant to) {
    return new Filter(
        "recorded_at_ms >= ? AND recorded_at_ms < ?",
        List.of(from.toEpochMilli(), to.toEpochMilli()));
  }

  static Filter session(String sessionId) {
    return new Filter("session_id = ?", List.of(sessionId));
  }

  Filter and(String clause, Object... values) {
    List<Object> combined = new ArrayList<>(params);
    combined.addAll(List.of(values));
    return new Filter(sql + " AND " + clause, combined);
  }
}
