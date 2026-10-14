package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;

/** One distinct fault, stored once however many times it recurs; the trace is the latest. */
public record FailureSignature(
    String exceptionClass,
    String message,
    String stackTrace,
    Instant firstSeen,
    Instant lastSeen) {}
