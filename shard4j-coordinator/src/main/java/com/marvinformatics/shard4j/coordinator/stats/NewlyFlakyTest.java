package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;

/** A test that flaked in the scope and in no session of the baseline before it. */
public record NewlyFlakyTest(
    String historyKey,
    int flakySessions,
    int retriedAttempts,
    long retryCostMs,
    int baselineSessions,
    Instant lastFailureAt,
    MostFrequentFailure mostFrequentFailure) {}
