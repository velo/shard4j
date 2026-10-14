package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;

/**
 * One test over a window. A session is flaky for it when a unit failed and then passed on a retry,
 * and failed when a unit's last attempt failed.
 */
public record TestStability(
    String historyKey,
    int sessions,
    int flakySessions,
    int failedSessions,
    int attempts,
    int failedAttempts,
    int retriedAttempts,
    long retryCostMs,
    double flakeRate,
    Instant lastFailureAt,
    MostFrequentFailure mostFrequentFailure) {}
