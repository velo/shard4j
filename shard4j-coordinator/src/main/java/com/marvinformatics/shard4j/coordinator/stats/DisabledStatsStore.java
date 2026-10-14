package com.marvinformatics.shard4j.coordinator.stats;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
final class DisabledStatsStore implements StatsStore {

  private final String unavailableReason;

  @Override
  public void recordAttemptsQuietly(
      String sessionId, int shard, int attemptNumber, List<Attempt> attempts, Instant acceptedAt) {}

  @Override
  public FailureReport findLatestFailedAttempts(
      Instant from, Instant to, String testIdOrHistoryKey, int limit) {
    throw new StatsUnavailableException(unavailableReason);
  }

  @Override
  public List<TestStability> findTestsWithFailures(
      Instant from, Instant to, UnstableTestOrder order, int limit) {
    throw new StatsUnavailableException(unavailableReason);
  }

  @Override
  public NewlyFlakyReport findNewlyFlakyTestsInSession(String sessionId, Duration baselineSpan) {
    throw new StatsUnavailableException(unavailableReason);
  }

  @Override
  public NewlyFlakyReport findNewlyFlakyTestsInWindow(
      Instant from, Instant to, Duration baselineSpan) {
    throw new StatsUnavailableException(unavailableReason);
  }

  @Override
  public List<TrendBucket> findFlakinessPerBucket(
      Instant from, Instant to, Duration bucketSpan, String historyKey) {
    throw new StatsUnavailableException(unavailableReason);
  }

  @Override
  public void prune(Duration retention, Instant now) {}

  @Override
  public void close() {}
}
