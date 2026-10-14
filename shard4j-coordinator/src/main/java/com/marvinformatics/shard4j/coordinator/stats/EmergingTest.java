package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;

/**
 * A test that flaked in the scope and never in the baseline before it.
 *
 * @param baselineSessions zero when the test is new, or did not run in the baseline
 */
public record EmergingTest(
    String test,
    int flakySessions,
    int retriedAttempts,
    long retryCostMs,
    int baselineSessions,
    int baselineFlakySessions,
    Instant lastFailureAt,
    TopFailure topFailure) {}
