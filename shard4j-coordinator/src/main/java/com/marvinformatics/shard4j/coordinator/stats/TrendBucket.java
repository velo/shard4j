package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;

/** A unit counts in the bucket its first attempt landed in, so a retry is never counted twice. */
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
