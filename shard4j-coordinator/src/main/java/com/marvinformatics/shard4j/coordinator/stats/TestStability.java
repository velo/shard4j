package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;

/**
 * One test's record over a window. A session is flaky for a test when some unit of it
 * failed and then passed on a retry; it failed when a unit's last attempt failed.
 *
 * @param retriedAttempts failed attempts that were followed by another attempt
 * @param retryCostMs the time those attempts burned before having to run again
 */
public record TestStability(
    String test,
    int sessions,
    int flakySessions,
    int failedSessions,
    int attempts,
    int failedAttempts,
    int retriedAttempts,
    long retryCostMs,
    double flakeRate,
    Instant lastFailureAt,
    TopFailure topFailure) {}
