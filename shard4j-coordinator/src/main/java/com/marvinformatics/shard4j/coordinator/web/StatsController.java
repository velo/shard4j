package com.marvinformatics.shard4j.coordinator.web;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.marvinformatics.shard4j.coordinator.CoordinatorSettings;
import com.marvinformatics.shard4j.coordinator.stats.DistinctFailure;
import com.marvinformatics.shard4j.coordinator.stats.FailedAttempt;
import com.marvinformatics.shard4j.coordinator.stats.FailureReport;
import com.marvinformatics.shard4j.coordinator.stats.NewlyFlakyReport;
import com.marvinformatics.shard4j.coordinator.stats.NewlyFlakyTest;
import com.marvinformatics.shard4j.coordinator.stats.StatsStore;
import com.marvinformatics.shard4j.coordinator.stats.TestStability;
import com.marvinformatics.shard4j.coordinator.stats.TrendBucket;
import com.marvinformatics.shard4j.coordinator.stats.UnstableTestOrder;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The failure and stability read surface. It is never public, even under {@code
 * COORDINATOR_PUBLIC_READ}: a stack trace can carry a connection string or a token the test
 * happened to hold, and a session id does not give anyone permission to read it.
 */
@RestController
@RequestMapping("/stats")
@RequiredArgsConstructor
public class StatsController {

  private static final int MAX_BUCKETS = 1_000;

  private final StatsStore stats;
  private final CoordinatorSettings settings;
  private final Clock clock = Clock.systemUTC();

  public record FailuresResponse(
      @JsonUnwrapped QueryWindow window,
      List<FailedAttempt> failedAttempts,
      Map<String, DistinctFailure> distinctFailures) {}

  public record UnstableResponse(@JsonUnwrapped QueryWindow window, List<TestStability> tests) {}

  public record EmergingResponse(
      Instant from,
      Instant to,
      Instant baselineFrom,
      boolean retentionLimited,
      List<NewlyFlakyTest> tests) {}

  public record TrendResponse(
      @JsonUnwrapped QueryWindow window, Duration bucketSpan, List<TrendBucket> buckets) {}

  @GetMapping("/failures")
  public FailuresResponse findFailedAttempts(
      @RequestParam(defaultValue = "7d") String since,
      @RequestParam(required = false) String test,
      @RequestParam(defaultValue = "100") int limit) {
    QueryWindow window = parseWindowEndingNow(since);
    FailureReport report =
        stats.findLatestFailedAttempts(
            window.from(), window.to(), test, requireBetweenOneAndMax("limit", limit, 1_000));
    return new FailuresResponse(window, report.failedAttempts(), report.distinctFailures());
  }

  @GetMapping("/unstable")
  public UnstableResponse rankUnstableTests(
      @RequestParam(defaultValue = "7d") String since,
      @RequestParam(defaultValue = "retries") String sort,
      @RequestParam(defaultValue = "20") int limit) {
    QueryWindow window = parseWindowEndingNow(since);
    UnstableTestOrder order;
    try {
      order = UnstableTestOrder.parse(sort);
    } catch (IllegalArgumentException e) {
      throw new InvalidQueryParameterException(e.getMessage());
    }
    return new UnstableResponse(
        window,
        stats.findTestsWithFailures(
            window.from(), window.to(), order, requireBetweenOneAndMax("limit", limit, 500)));
  }

  /**
   * Tests that flaked in the scope (one session, or a window ending now) and in none of the
   * sessions of the baseline before it.
   */
  @GetMapping("/emerging")
  public EmergingResponse findNewlyFlakyTests(
      @RequestParam(required = false) String sessionId,
      @RequestParam(required = false) String since,
      @RequestParam(defaultValue = "14d") String baseline) {
    if (sessionId != null && since != null) {
      throw new InvalidQueryParameterException("Give sessionId or since, not both");
    }
    Duration baselineSpan = QueryWindow.parsePositiveDuration("baseline", baseline);
    NewlyFlakyReport report;
    if (sessionId != null) {
      report = stats.findNewlyFlakyTestsInSession(sessionId, baselineSpan);
    } else {
      QueryWindow window = parseWindowEndingNow(since == null ? "1d" : since);
      report = stats.findNewlyFlakyTestsInWindow(window.from(), window.to(), baselineSpan);
    }
    QueryWindow baselineWindow =
        QueryWindow.before(
            report.from(), baselineSpan, clock.instant().minus(settings.historyRetention()));
    return new EmergingResponse(
        report.from(),
        report.to(),
        report.baselineFrom(),
        baselineWindow.retentionLimited(),
        report.tests());
  }

  @GetMapping("/trend")
  public TrendResponse findFlakinessTrend(
      @RequestParam(defaultValue = "30d") String since,
      @RequestParam(defaultValue = "1d") String bucketSpan,
      @RequestParam(required = false) String test) {
    QueryWindow window = parseWindowEndingNow(since);
    Duration parsedBucketSpan = QueryWindow.parsePositiveDuration("bucketSpan", bucketSpan);
    if (Duration.between(window.from(), window.to()).dividedBy(parsedBucketSpan) >= MAX_BUCKETS) {
      throw new InvalidQueryParameterException(
          "since / bucketSpan must stay under " + MAX_BUCKETS + " buckets; widen the bucket");
    }
    return new TrendResponse(
        window,
        parsedBucketSpan,
        stats.findFlakinessPerBucket(window.from(), window.to(), parsedBucketSpan, test));
  }

  private QueryWindow parseWindowEndingNow(String since) {
    return QueryWindow.parseSinceClampedToRetention(
        clock.instant(), since, settings.historyRetention());
  }

  private static int requireBetweenOneAndMax(String parameterName, int requestedValue, int max) {
    if (requestedValue < 1 || requestedValue > max) {
      throw new InvalidQueryParameterException(parameterName + " must be between 1 and " + max);
    }
    return requestedValue;
  }
}
