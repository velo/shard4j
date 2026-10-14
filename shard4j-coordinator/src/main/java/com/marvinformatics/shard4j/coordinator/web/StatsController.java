package com.marvinformatics.shard4j.coordinator.web;

import com.marvinformatics.shard4j.coordinator.CoordinatorSettings;
import com.marvinformatics.shard4j.coordinator.stats.EmergingTest;
import com.marvinformatics.shard4j.coordinator.stats.FailedAttempt;
import com.marvinformatics.shard4j.coordinator.stats.FailureSignature;
import com.marvinformatics.shard4j.coordinator.stats.StabilityRanking;
import com.marvinformatics.shard4j.coordinator.stats.StatsStore;
import com.marvinformatics.shard4j.coordinator.stats.TestStability;
import com.marvinformatics.shard4j.coordinator.stats.TrendBucket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The failure and stability read surface. Never public, even under {@code
 * COORDINATOR_PUBLIC_READ}: a stack trace can carry a connection string or a token the test
 * happened to hold, and a session id is not a capability.
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
      Instant from,
      Instant to,
      boolean retentionLimited,
      List<FailedAttempt> failures,
      Map<String, FailureSignature> signatures) {}

  public record UnstableResponse(
      Instant from, Instant to, boolean retentionLimited, List<TestStability> tests) {}

  public record EmergingResponse(
      Instant from,
      Instant to,
      Instant baselineFrom,
      boolean retentionLimited,
      List<EmergingTest> tests) {}

  public record TrendResponse(
      Instant from,
      Instant to,
      Duration bucket,
      boolean retentionLimited,
      List<TrendBucket> buckets) {}

  @GetMapping("/failures")
  public FailuresResponse failures(
      @RequestParam(defaultValue = "7d") String since,
      @RequestParam(required = false) String test,
      @RequestParam(defaultValue = "100") int limit) {
    QueryWindow window = window(since);
    List<FailedAttempt> failures =
        stats.failures(window.from(), window.to(), test, bounded("limit", limit, 1_000));
    Set<String> fingerprints = new LinkedHashSet<>();
    failures.stream()
        .map(FailedAttempt::fingerprint)
        .filter(Objects::nonNull)
        .forEach(fingerprints::add);
    return new FailuresResponse(
        window.from(),
        window.to(),
        window.retentionLimited(),
        failures,
        stats.signatures(fingerprints));
  }

  @GetMapping("/unstable")
  public UnstableResponse unstable(
      @RequestParam(defaultValue = "7d") String since,
      @RequestParam(defaultValue = "retries") String sort,
      @RequestParam(defaultValue = "20") int limit) {
    QueryWindow window = window(since);
    StabilityRanking ranking;
    try {
      ranking = StabilityRanking.parse(sort);
    } catch (IllegalArgumentException e) {
      throw new InvalidQueryException(e.getMessage());
    }
    return new UnstableResponse(
        window.from(),
        window.to(),
        window.retentionLimited(),
        stats.unstable(window.from(), window.to(), ranking, bounded("limit", limit, 500)));
  }

  /**
   * Tests that flaked in the scope -- one session, or a window ending now -- and in none of
   * the sessions of the baseline before it.
   */
  @GetMapping("/emerging")
  public EmergingResponse emerging(
      @RequestParam(required = false) String session,
      @RequestParam(required = false) String since,
      @RequestParam(defaultValue = "14d") String baseline) {
    if (session != null && since != null) {
      throw new InvalidQueryException("Give session or since, not both");
    }
    Instant now = clock.instant();
    Duration baselineSpan = QueryWindow.parse("baseline", baseline);
    StatsStore.Emerging emerging;
    if (session != null) {
      emerging = stats.emergingInSession(session, baselineSpan);
    } else {
      QueryWindow window = window(since == null ? "1d" : since);
      emerging = stats.emergingInWindow(window.from(), window.to(), baselineSpan);
    }
    boolean limited =
        emerging.baselineFrom() != null
            && emerging.baselineFrom().isBefore(now.minus(settings.historyRetention()));
    return new EmergingResponse(
        emerging.from(), emerging.to(), emerging.baselineFrom(), limited, emerging.tests());
  }

  @GetMapping("/trend")
  public TrendResponse trend(
      @RequestParam(defaultValue = "30d") String since,
      @RequestParam(defaultValue = "1d") String bucket,
      @RequestParam(required = false) String test) {
    QueryWindow window = window(since);
    Duration bucketSpan = QueryWindow.parse("bucket", bucket);
    if (Duration.between(window.from(), window.to()).dividedBy(bucketSpan) >= MAX_BUCKETS) {
      throw new InvalidQueryException(
          "since / bucket must stay under " + MAX_BUCKETS + " buckets; widen the bucket");
    }
    return new TrendResponse(
        window.from(),
        window.to(),
        bucketSpan,
        window.retentionLimited(),
        stats.trend(window.from(), window.to(), bucketSpan, test));
  }

  private QueryWindow window(String since) {
    return QueryWindow.ending(clock.instant(), since, settings.historyRetention());
  }

  private static int bounded(String parameter, int value, int max) {
    if (value < 1 || value > max) {
      throw new InvalidQueryException(parameter + " must be between 1 and " + max);
    }
    return value;
  }
}
