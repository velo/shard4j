package com.marvinformatics.shard4j.coordinator.stats;

import java.util.Locale;

public enum UnstableTestOrder {
  RETRIES("retried_attempts DESC, retry_cost_ms DESC"),
  RETRY_COST("retry_cost_ms DESC, retried_attempts DESC"),
  FLAKE_RATE("flake_rate DESC, retried_attempts DESC"),
  FAILURES("failed_sessions DESC, failed_attempts DESC");

  final String orderByClause;

  UnstableTestOrder(String orderByClause) {
    this.orderByClause = orderByClause;
  }

  public static UnstableTestOrder parse(String orderName) {
    try {
      return valueOf(orderName.toUpperCase(Locale.ROOT).replace('-', '_'));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "sort must be one of retries, retry-cost, flake-rate, failures: " + orderName, e);
    }
  }
}
