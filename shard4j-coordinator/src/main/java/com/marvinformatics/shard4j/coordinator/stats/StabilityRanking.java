package com.marvinformatics.shard4j.coordinator.stats;

import java.util.Locale;

/** How the unstable-tests list is ordered. */
public enum StabilityRanking {
  RETRIES("retried DESC, retry_cost_ms DESC"),
  COST("retry_cost_ms DESC, retried DESC"),
  RATE("CAST(flaky_sessions AS REAL) / sessions DESC, retried DESC"),
  FAILURES("failed_sessions DESC, failed DESC");

  final String orderBy;

  StabilityRanking(String orderBy) {
    this.orderBy = orderBy;
  }

  public static StabilityRanking parse(String value) {
    try {
      return valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "sort must be one of retries, cost, rate, failures: " + value, e);
    }
  }
}
