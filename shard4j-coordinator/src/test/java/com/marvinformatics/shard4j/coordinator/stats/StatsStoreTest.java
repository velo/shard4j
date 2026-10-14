package com.marvinformatics.shard4j.coordinator.stats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.tuple;

import com.marvinformatics.shard4j.protocol.Failure;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Against a real SQLite file: the SQL is the logic under test, so nothing is stubbed. */
class StatsStoreTest {

  private static final String CHECKOUT = "com.example.orders.CheckoutIT";
  private static final String PAY = method("pay");
  private static final String REFUND = method("refund");
  private static final String SHIP = method("ship");
  private static final String ROWS =
      "[engine:junit-jupiter]/[class:" + CHECKOUT + "]/[test-template:rows(int)]";
  private static final Instant T0 = Instant.parse("2026-10-01T00:00:00Z");
  private static final Failure TIMEOUT =
      new Failure(
          "java.net.SocketTimeoutException",
          "Read timed out",
          "java.net.SocketTimeoutException: Read timed out\n"
              + "\tat com.example.orders.GatewayClient.call(GatewayClient.java:88)\n");

  @TempDir Path dir;
  private StatsStore store;

  @BeforeEach
  void open() {
    store = StatsStore.open(dir.resolve("stats.db"));
  }

  @AfterEach
  void close() throws Exception {
    store.close();
  }

  @Test
  void givenAFailureRetriedGreen_whenRanking_thenTheTestIsFlakyAndItsRetryIsCosted() {
    store.record("s1", failed(PAY, 4_000, TIMEOUT), 1, T0);
    store.record("s1", passed(PAY, 1_000), 2, T0.plusSeconds(10));
    store.record("s1", passed(REFUND, 500), 1, T0);

    List<TestStability> unstable =
        store.unstable(T0, T0.plus(Duration.ofDays(1)), StabilityRanking.RETRIES, 10);

    assertThat(unstable).hasSize(1);
    TestStability pay = unstable.get(0);
    assertThat(pay.test()).isEqualTo(CHECKOUT + "#pay()");
    assertThat(pay)
        .extracting(
            TestStability::sessions,
            TestStability::flakySessions,
            TestStability::failedSessions,
            TestStability::attempts,
            TestStability::retriedAttempts,
            TestStability::retryCostMs,
            TestStability::flakeRate)
        .containsExactly(1, 1, 0, 2, 1, 4_000L, 1.0);
    assertThat(pay.topFailure().exceptionClass()).isEqualTo(TIMEOUT.exceptionClass());
    assertThat(pay.topFailure().occurrences()).isOne();
  }

  @Test
  void givenABudgetExhaustedOnFailures_whenRanking_thenItIsFailedNotFlaky() {
    store.record("s1", failed(SHIP, 100, TIMEOUT), 1, T0);
    store.record("s1", failed(SHIP, 100, TIMEOUT), 2, T0.plusSeconds(1));
    store.record("s1", failed(SHIP, 100, TIMEOUT), 3, T0.plusSeconds(2));

    TestStability ship =
        store.unstable(T0, T0.plus(Duration.ofDays(1)), StabilityRanking.FAILURES, 10).get(0);

    assertThat(ship)
        .extracting(
            TestStability::flakySessions,
            TestStability::failedSessions,
            TestStability::failedAttempts,
            TestStability::retriedAttempts)
        .containsExactly(0, 1, 3, 2);
  }

  @Test
  void givenFailures_whenListing_thenNewestFirstWithRetriedAndOneSignaturePerFault() {
    store.record("s1", failed(PAY, 4_000, TIMEOUT), 1, T0);
    store.record("s1", passed(PAY, 1_000), 2, T0.plusSeconds(10));
    store.record("s1", failed(SHIP, 100, TIMEOUT), 1, T0.plusSeconds(20));

    List<FailedAttempt> failures = store.failures(T0, T0.plus(Duration.ofDays(1)), null, 10);

    assertThat(failures)
        .extracting(FailedAttempt::testId, FailedAttempt::retried)
        .containsExactly(tuple(SHIP, false), tuple(PAY, true));
    assertThat(failures).extracting(FailedAttempt::fingerprint).doesNotContainNull().hasSize(2);
    assertThat(failures.get(0).fingerprint()).isEqualTo(failures.get(1).fingerprint());
    assertThat(store.signatures(Set.of(failures.get(0).fingerprint())).values())
        .singleElement()
        .extracting(FailureSignature::stackTrace)
        .isEqualTo(TIMEOUT.stackTrace());
    assertThat(store.failures(T0, T0.plus(Duration.ofDays(1)), CHECKOUT + "#pay()", 10))
        .extracting(FailedAttempt::testId)
        .containsExactly(PAY);
  }

  @Test
  void givenATemplateWithAFailedRow_whenListing_thenTheRowCarriesTheTraceNotTheTemplate() {
    store.record(
        "s1",
        new ResultRequest(
            0,
            ROWS,
            new Fence(1, 1, 1),
            Outcome.FAILED,
            900,
            false,
            "row 2 failed",
            List.of(
                new InvocationRecord(invocation(1), Outcome.PASSED, 300, null, null),
                new InvocationRecord(invocation(2), Outcome.FAILED, 600, "row 2 failed", TIMEOUT)),
            null),
        1,
        T0);

    List<FailedAttempt> failures = store.failures(T0, T0.plus(Duration.ofDays(1)), null, 10);

    assertThat(failures)
        .extracting(FailedAttempt::testId, FailedAttempt::kind, FailedAttempt::test)
        .containsExactly(tuple(invocation(2), "INVOCATION", CHECKOUT + "#rows(int)"));
    assertThat(store.unstable(T0, T0.plus(Duration.ofDays(1)), StabilityRanking.RETRIES, 10))
        .extracting(TestStability::test, TestStability::attempts)
        .containsExactly(tuple(CHECKOUT + "#rows(int)", 1));
  }

  @Test
  void givenAFlakeAlsoSeenInTheBaseline_whenAskingWhatEmerged_thenOnlyTheNewOneIsListed() {
    store.record("old", failed(PAY, 100, TIMEOUT), 1, T0);
    store.record("old", passed(PAY, 100), 2, T0.plusSeconds(1));
    store.record("old", passed(REFUND, 100), 1, T0);
    Instant later = T0.plus(Duration.ofDays(3));
    store.record("new", failed(PAY, 100, TIMEOUT), 1, later);
    store.record("new", passed(PAY, 100), 2, later.plusSeconds(1));
    store.record("new", failed(REFUND, 2_500, TIMEOUT), 1, later);
    store.record("new", passed(REFUND, 100), 2, later.plusSeconds(2));
    store.record("new", failed(SHIP, 700, TIMEOUT), 1, later);
    store.record("new", passed(SHIP, 100), 2, later.plusSeconds(3));

    StatsStore.Emerging emerging = store.emergingInSession("new", Duration.ofDays(14));

    assertThat(emerging.from()).isEqualTo(later);
    assertThat(emerging.baselineFrom()).isEqualTo(later.minus(Duration.ofDays(14)));
    assertThat(emerging.tests())
        .extracting(
            EmergingTest::test,
            EmergingTest::retriedAttempts,
            EmergingTest::retryCostMs,
            EmergingTest::baselineSessions)
        .containsExactly(
            tuple(CHECKOUT + "#refund()", 1, 2_500L, 1), tuple(CHECKOUT + "#ship()", 1, 700L, 0));
    assertThat(store.emergingInWindow(later, later.plusSeconds(60), Duration.ofDays(14)).tests())
        .extracting(EmergingTest::test)
        .containsExactly(CHECKOUT + "#refund()", CHECKOUT + "#ship()");
  }

  @Test
  void givenASessionWithNoResults_whenAskingWhatEmerged_thenNothingDid() {
    assertThat(store.emergingInSession("never-seen", Duration.ofDays(14)).tests()).isEmpty();
  }

  @Test
  void givenFlakesOnTwoDays_whenTrending_thenEachDayIsItsOwnBucketAndGapsAreZero() {
    store.record("d1", failed(PAY, 100, TIMEOUT), 1, T0.plusSeconds(60));
    store.record("d1", passed(PAY, 100), 2, T0.plusSeconds(61));
    store.record("d3a", failed(PAY, 100, TIMEOUT), 1, T0.plus(Duration.ofDays(2)));
    store.record("d3a", passed(PAY, 100), 2, T0.plus(Duration.ofDays(2)).plusSeconds(1));
    store.record("d3b", passed(REFUND, 100), 1, T0.plus(Duration.ofDays(2)));

    List<TrendBucket> trend =
        store.trend(T0, T0.plus(Duration.ofDays(3)), Duration.ofDays(1), null);

    assertThat(trend)
        .extracting(TrendBucket::from, TrendBucket::sessions, TrendBucket::flakySessions)
        .containsExactly(
            tuple(T0, 1, 1),
            tuple(T0.plus(Duration.ofDays(1)), 0, 0),
            tuple(T0.plus(Duration.ofDays(2)), 2, 1));
    assertThat(store.trend(T0, T0.plus(Duration.ofDays(3)), Duration.ofDays(1), CHECKOUT + "#refund()"))
        .extracting(TrendBucket::sessions)
        .containsExactly(0, 0, 1);
  }

  @Test
  void givenAttemptsPastRetention_whenPruning_thenTheyAndTheirOrphanedFaultsGo() {
    store.record("old", failed(PAY, 100, TIMEOUT), 1, T0);
    store.record("new", passed(PAY, 100), 1, T0.plus(Duration.ofDays(40)));

    store.prune(Duration.ofDays(30), T0.plus(Duration.ofDays(40)));

    Instant from = T0.minus(Duration.ofDays(1));
    Instant to = T0.plus(Duration.ofDays(41));
    assertThat(store.failures(from, to, null, 10)).isEmpty();
    assertThat(store.trend(from, to, Duration.ofDays(42), null))
        .singleElement()
        .extracting(TrendBucket::attempts)
        .isEqualTo(1);
  }

  @Test
  void givenAPathThatCannotHoldADatabase_whenOpening_thenRecordingIsANoOpAndReadsSay503() {
    StatsStore broken = StatsStore.open(dir);

    assertThat(broken.available()).isFalse();
    broken.record("s1", passed(PAY, 1), 1, T0);
    assertThatExceptionOfType(StatsUnavailableException.class)
        .isThrownBy(() -> broken.failures(T0, T0.plusSeconds(1), null, 1));
  }

  @Test
  void givenAnExistingDatabase_whenReopened_thenTheSchemaIsNotReappliedAndDataSurvives()
      throws Exception {
    store.record("s1", failed(PAY, 100, TIMEOUT), 1, T0);
    store.close();

    store = StatsStore.open(dir.resolve("stats.db"));

    assertThat(store.available()).isTrue();
    assertThat(store.failures(T0, T0.plusSeconds(1), null, 10)).hasSize(1);
  }

  private static ResultRequest failed(String testId, long durationMs, Failure failure) {
    return new ResultRequest(
        0, testId, new Fence(1, 1, 1), Outcome.FAILED, durationMs, false, "boom", null, failure);
  }

  private static ResultRequest passed(String testId, long durationMs) {
    return new ResultRequest(
        0, testId, new Fence(1, 1, 1), Outcome.PASSED, durationMs, false, null, null, null);
  }

  private static String method(String name) {
    return "[engine:junit-jupiter]/[class:" + CHECKOUT + "]/[method:" + name + "()]";
  }

  private static String invocation(int index) {
    return ROWS + "/[test-template-invocation:#" + index + "]";
  }
}
