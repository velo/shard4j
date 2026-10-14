package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;

/**
 * Flakiness inside one time bucket, attributed to the bucket each unit's first attempt
 * landed in so a retry crossing a boundary is not counted twice.
 */
public record TrendBucket(
    Instant from,
    Instant to,
    int sessions,
    int flakySessions,
    int flakyTests,
    int attempts,
    int failedAttempts,
    int retriedAttempts,
    long retryCostMs) {}
