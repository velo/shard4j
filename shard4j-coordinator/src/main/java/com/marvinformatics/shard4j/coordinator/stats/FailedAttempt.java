package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Instant;

/**
 * One FAILED execution.
 *
 * @param test the method-level history key, shared by every invocation of a template
 * @param retried whether a later attempt of the same unit in the same session followed --
 *     a failure that was retried is the raw material of a flake
 * @param fingerprint keys into the report's signatures; null when the engine sent no trace
 */
public record FailedAttempt(
    String session,
    String testId,
    String test,
    String kind,
    int shard,
    int attempt,
    long durationMs,
    String reason,
    Instant at,
    boolean retried,
    String fingerprint) {}
