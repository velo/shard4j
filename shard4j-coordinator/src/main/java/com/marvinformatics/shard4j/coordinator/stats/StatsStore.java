package com.marvinformatics.shard4j.coordinator.stats;

import com.marvinformatics.shard4j.coordinator.core.AttemptRecorder;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Failure and stability statistics. Never load-bearing: a store that cannot be opened is replaced
 * by one whose writes do nothing and whose reads answer 503, and scheduling goes on.
 */
public interface StatsStore extends AttemptRecorder, AutoCloseable {

  static StatsStore openOrCreateElseDisabled(Path databaseFile) {
    return SqliteStatsStore.openOrCreateElseDisabled(databaseFile);
  }

  FailureReport findLatestFailedAttempts(
      Instant from, Instant to, String testIdOrHistoryKey, int limit);

  List<TestStability> findTestsWithFailures(
      Instant from, Instant to, UnstableTestOrder order, int limit);

  NewlyFlakyReport findNewlyFlakyTestsInSession(String sessionId, Duration baselineSpan);

  NewlyFlakyReport findNewlyFlakyTestsInWindow(Instant from, Instant to, Duration baselineSpan);

  List<TrendBucket> findFlakinessPerBucket(
      Instant from, Instant to, Duration bucketSpan, String historyKey);

  void prune(Duration retention, Instant now);

  @Override
  void close();
}
