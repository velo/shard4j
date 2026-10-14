package com.marvinformatics.shard4j.coordinator.stats;

import com.marvinformatics.shard4j.coordinator.core.AttemptKind;
import java.time.Instant;

/**
 * One FAILED execution. {@code retried} says a later attempt of the same unit followed; {@code
 * fingerprint} keys into the report's distinct failures, null when no trace was sent.
 */
public record FailedAttempt(
    String sessionId,
    String testId,
    String historyKey,
    AttemptKind unitKind,
    int shard,
    int attemptNumber,
    long durationMs,
    String reason,
    Instant failedAt,
    boolean retried,
    String fingerprint) {}
