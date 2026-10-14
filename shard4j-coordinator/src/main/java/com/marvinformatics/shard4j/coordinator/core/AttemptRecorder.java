package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.Failure;
import com.marvinformatics.shard4j.protocol.Outcome;
import java.time.Instant;
import java.util.List;

/** Where accepted attempts go once the session state is durable. Never throws. */
public interface AttemptRecorder {

  record Attempt(
      String testId,
      AttemptKind unitKind,
      Outcome outcome,
      long durationMs,
      String reason,
      Failure failure) {}

  void recordAttemptsQuietly(
      String sessionId, int shard, int attemptNumber, List<Attempt> attempts, Instant acceptedAt);
}
