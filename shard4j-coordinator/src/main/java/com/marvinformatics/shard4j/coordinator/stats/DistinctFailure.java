package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;

/** One fault, stored once however often it recurs; the trace kept is the latest. */
public record DistinctFailure(
    String exceptionClass,
    String message,
    String stackTrace,
    Instant firstSeen,
    Instant lastSeen) {}
