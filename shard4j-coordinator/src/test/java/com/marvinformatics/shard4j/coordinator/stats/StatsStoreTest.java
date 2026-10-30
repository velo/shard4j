package com.marvinformatics.shard4j.coordinator.stats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.tuple;

import com.marvinformatics.shard4j.coordinator.core.AttemptKind;
import com.marvinformatics.shard4j.coordinator.core.AttemptRecorder.Attempt;
import com.marvinformatics.shard4j.coordinator.core.UnknownSessionException;
import com.marvinformatics.shard4j.protocol.Failure;
import com.marvinformatics.shard4j.protocol.Outcome;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Against a real SQLite file: the SQL is the logic under test, so nothing is stubbed. */
class StatsStoreTest {

  private static final String CHECKOUT = "com.example.orders.CheckoutIT";
  private static final String PAY = buildCheckoutMethodId("pay");
  private static final String REFUND = buildCheckoutMethodId("refund");
  private static final String SHIP = buildCheckoutMethodId("ship");
  private static final String ROWS =
      "[engine:junit-jupiter]/[class:" + CHECKOUT + "]/[test-template:rows(int)]";
  private static final Instant T0 = Instant.parse("2026-10-01T00:00:00Z");
  private static final Instant DAY_LATER = T0.plus(Duration.ofDays(1));
  private static final Failure TIMEOUT =
      new Failure(
          "java.net.SocketTimeoutException",
          "Read timed out",
          "java.net.SocketTimeoutException: Read timed out\n"
              + "\tat com.example.orders.GatewayClient.call(GatewayClient.java:88)\n");
  private static final Failure REFUSED =
      new Failure(
          "java.net.ConnectException",
          "Connection refused",
          "java.net.ConnectException: Connection refused\n"
              + "\tat com.example.orders.GatewayClient.connect(GatewayClient.java:41)\n");

  @TempDir Path dir;
  private StatsStore store;

  @BeforeEach
  void openStatsStoreOnFreshDatabase() {
    store = StatsStore.openOrCreateElseDisabled(dir.resolve("stats.db"));
  }

  @AfterEach
  void closeStatsStore() {
    store.close();
  }

  @Test
  void
      givenOneTestRetriedGreenAndOneStablePass_whenRankingUnstable_thenOnlyTheRetriedTestIsFlakyAndCostsItsFailedAttempt() {
    record("s1", 1, T0, failedAttempt(PAY, 4_000, TIMEOUT));
    record("s1", 2, T0.plusSeconds(10), passedAttempt(PAY, 1_000));
    record("s1", 1, T0, passedAttempt(REFUND, 500));

    List<TestStability> unstableTests =
        store.findTestsWithFailures(T0, DAY_LATER, UnstableTestOrder.RETRIES, 10);

    assertThat(unstableTests).hasSize(1);
    TestStability payStability = unstableTests.get(0);
    assertThat(payStability.historyKey()).isEqualTo(CHECKOUT + "#pay()");
    assertThat(payStability)
        .extracting(
            TestStability::sessions,
            TestStability::flakySessions,
            TestStability::failedSessions,
            TestStability::attempts,
            TestStability::retriedAttempts,
            TestStability::retryCostMs,
            TestStability::flakeRate)
        .containsExactly(1, 1, 0, 2, 1, 4_000L, 1.0);
    assertThat(payStability.mostFrequentFailure().exceptionClass())
        .isEqualTo(TIMEOUT.exceptionClass());
    assertThat(payStability.mostFrequentFailure().occurrences()).isOne();
  }

  @Test
  void
      givenEveryAttemptFailedInOneSession_whenRankingByFailures_thenTheSessionIsFailedNotFlakyWithTwoRetriedAttempts() {
    record("s1", 1, T0, failedAttempt(SHIP, 100, TIMEOUT));
    record("s1", 2, T0.plusSeconds(1), failedAttempt(SHIP, 100, TIMEOUT));
    record("s1", 3, T0.plusSeconds(2), failedAttempt(SHIP, 100, TIMEOUT));

    TestStability shipStability =
        store.findTestsWithFailures(T0, DAY_LATER, UnstableTestOrder.FAILURES, 10).get(0);

    assertThat(shipStability)
        .extracting(
            TestStability::flakySessions,
            TestStability::failedSessions,
            TestStability::failedAttempts,
            TestStability::retriedAttempts)
        .containsExactly(0, 1, 3, 2);
  }

  @Test
  void givenTwoDistinctFaultsForOneTest_whenRanking_thenTheMoreFrequentOneIsNamed() {
    record("s1", 1, T0, failedAttempt(PAY, 100, REFUSED));
    record("s1", 2, T0.plusSeconds(1), passedAttempt(PAY, 100));
    record("s2", 1, T0.plusSeconds(2), failedAttempt(PAY, 100, TIMEOUT));
    record("s2", 2, T0.plusSeconds(3), failedAttempt(PAY, 100, TIMEOUT));

    TestStability payStability =
        store.findTestsWithFailures(T0, DAY_LATER, UnstableTestOrder.RETRIES, 10).get(0);

    assertThat(payStability.mostFrequentFailure())
        .extracting(MostFrequentFailure::exceptionClass, MostFrequentFailure::occurrences)
        .containsExactly(TIMEOUT.exceptionClass(), 2);
  }

  @Test
  void
      givenTwoTestsFailingWithTheSameFaultOneRetried_whenListingFailures_thenNewestFirstOnlyTheRetriedFlaggedAndOneSharedDistinctFailure() {
    record("s1", 1, T0, failedAttempt(PAY, 4_000, TIMEOUT));
    record("s1", 2, T0.plusSeconds(10), passedAttempt(PAY, 1_000));
    record("s1", 1, T0.plusSeconds(20), failedAttempt(SHIP, 100, TIMEOUT));

    FailureReport report = store.findLatestFailedAttempts(T0, DAY_LATER, null, 10);

    assertThat(report.failedAttempts())
        .extracting(FailedAttempt::testId, FailedAttempt::retried)
        .containsExactly(tuple(SHIP, false), tuple(PAY, true));
    assertThat(report.failedAttempts())
        .extracting(FailedAttempt::fingerprint)
        .containsOnly(report.failedAttempts().get(0).fingerprint());
    assertThat(report.distinctFailures().values())
        .singleElement()
        .extracting(DistinctFailure::stackTrace)
        .isEqualTo(TIMEOUT.stackTrace());
  }

  @Test
  void givenFailuresOfTwoTests_whenListingFailuresForOneTest_thenOnlyThatTestsFailuresAreListed() {
    record("s1", 1, T0, failedAttempt(PAY, 100, TIMEOUT));
    record("s1", 1, T0, failedAttempt(SHIP, 100, TIMEOUT));

    assertThat(
            store.findLatestFailedAttempts(T0, DAY_LATER, CHECKOUT + "#pay()", 10).failedAttempts())
        .extracting(FailedAttempt::testId)
        .containsExactly(PAY);
  }

  @Test
  void
      givenATemplateWithAPassedAndAFailedRow_whenListingFailuresAndRanking_thenOnlyTheFailedRowIsListedUnderTheTemplateMethodRankedOnce() {
    record(
        "s1",
        1,
        T0,
        new Attempt(ROWS, AttemptKind.TEMPLATE, Outcome.FAILED, 900, "row 2 failed", null),
        new Attempt(
            buildRowsInvocationTestId(1), AttemptKind.INVOCATION, Outcome.PASSED, 300, null, null),
        new Attempt(
            buildRowsInvocationTestId(2),
            AttemptKind.INVOCATION,
            Outcome.FAILED,
            600,
            "row 2 failed",
            TIMEOUT));

    assertThat(store.findLatestFailedAttempts(T0, DAY_LATER, null, 10).failedAttempts())
        .extracting(FailedAttempt::testId, FailedAttempt::unitKind, FailedAttempt::historyKey)
        .containsExactly(
            tuple(buildRowsInvocationTestId(2), AttemptKind.INVOCATION, CHECKOUT + "#rows(int)"));
    assertThat(store.findTestsWithFailures(T0, DAY_LATER, UnstableTestOrder.RETRIES, 10))
        .extracting(TestStability::historyKey, TestStability::attempts)
        .containsExactly(tuple(CHECKOUT + "#rows(int)", 1));
  }

  @Test
  void givenATemplateThatFailedWithItsOwnTrace_whenListingFailures_thenTheTemplateIsListed() {
    record(
        "s1",
        1,
        T0,
        new Attempt(ROWS, AttemptKind.TEMPLATE, Outcome.FAILED, 900, "provider threw", TIMEOUT));

    assertThat(store.findLatestFailedAttempts(T0, DAY_LATER, null, 10).failedAttempts())
        .extracting(FailedAttempt::testId, FailedAttempt::unitKind)
        .containsExactly(tuple(ROWS, AttemptKind.TEMPLATE));
  }

  @Test
  void
      givenOneOfASessionsThreeFlakesAlsoFlakedInTheBaseline_whenAskingWhatEmerged_thenOnlyTheOtherTwoAreListed() {
    record("old", 1, T0, failedAttempt(PAY, 100, TIMEOUT));
    record("old", 2, T0.plusSeconds(1), passedAttempt(PAY, 100));
    record("old", 1, T0, passedAttempt(REFUND, 100));
    Instant newSessionStart = T0.plus(Duration.ofDays(3));
    record("new", 1, newSessionStart, failedAttempt(PAY, 100, TIMEOUT));
    record("new", 2, newSessionStart.plusSeconds(1), passedAttempt(PAY, 100));
    record("new", 1, newSessionStart, failedAttempt(REFUND, 2_500, TIMEOUT));
    record("new", 2, newSessionStart.plusSeconds(2), passedAttempt(REFUND, 100));
    record("new", 1, newSessionStart, failedAttempt(SHIP, 700, TIMEOUT));
    record("new", 2, newSessionStart.plusSeconds(3), passedAttempt(SHIP, 100));

    NewlyFlakyReport report = store.findNewlyFlakyTestsInSession("new", Duration.ofDays(14));

    assertThat(report.from()).isEqualTo(newSessionStart);
    assertThat(report.baselineFrom()).isEqualTo(newSessionStart.minus(Duration.ofDays(14)));
    assertThat(report.tests())
        .extracting(
            NewlyFlakyTest::historyKey,
            NewlyFlakyTest::retriedAttempts,
            NewlyFlakyTest::retryCostMs,
            NewlyFlakyTest::baselineSessions)
        .containsExactly(
            tuple(CHECKOUT + "#refund()", 1, 2_500L, 1), tuple(CHECKOUT + "#ship()", 1, 700L, 0));
    assertThat(
            store
                .findNewlyFlakyTestsInWindow(
                    newSessionStart, newSessionStart.plusSeconds(60), Duration.ofDays(14))
                .tests())
        .extracting(NewlyFlakyTest::historyKey)
        .containsExactly(CHECKOUT + "#refund()", CHECKOUT + "#ship()");
  }

  @Test
  void givenASessionWithNoRecordedAttempts_whenAskingWhatEmerged_thenItIsAnUnknownSession() {
    assertThatExceptionOfType(UnknownSessionException.class)
        .isThrownBy(() -> store.findNewlyFlakyTestsInSession("never-seen", Duration.ofDays(14)));
  }

  @Test
  void
      givenFlakySessionsOnDaysOneAndThreeAndACleanOneOnDayThree_whenTrendingByDay_thenEachDayCountsSessionsAndFlakySessionsAndDayTwoIsZero() {
    record("d1", 1, T0.plusSeconds(60), failedAttempt(PAY, 100, TIMEOUT));
    record("d1", 2, T0.plusSeconds(61), passedAttempt(PAY, 100));
    Instant dayThree = T0.plus(Duration.ofDays(2));
    record("d3a", 1, dayThree, failedAttempt(PAY, 100, TIMEOUT));
    record("d3a", 2, dayThree.plusSeconds(1), passedAttempt(PAY, 100));
    record("d3b", 1, dayThree, passedAttempt(REFUND, 100));

    List<TrendBucket> dailyTrend =
        store.findFlakinessPerBucket(T0, T0.plus(Duration.ofDays(3)), Duration.ofDays(1), null);

    assertThat(dailyTrend)
        .extracting(TrendBucket::from, TrendBucket::sessions, TrendBucket::flakySessions)
        .containsExactly(tuple(T0, 1, 1), tuple(DAY_LATER, 0, 0), tuple(dayThree, 2, 1));
  }

  @Test
  void
      givenSessionsOfTwoTestsOnDayThree_whenTrendingOneTestByDay_thenOnlySessionsThatRanItAreCounted() {
    Instant dayThree = T0.plus(Duration.ofDays(2));
    record("d3a", 1, dayThree, passedAttempt(PAY, 100));
    record("d3b", 1, dayThree, passedAttempt(REFUND, 100));

    assertThat(
            store.findFlakinessPerBucket(
                T0, T0.plus(Duration.ofDays(3)), Duration.ofDays(1), CHECKOUT + "#refund()"))
        .extracting(TrendBucket::sessions)
        .containsExactly(0, 0, 1);
  }

  @Test
  void
      givenAFailedAttemptPastRetentionAndAPassedAttemptWithinIt_whenPruning_thenOnlyTheAttemptWithinRetentionRemains() {
    record("old", 1, T0, failedAttempt(PAY, 100, TIMEOUT));
    record("new", 1, T0.plus(Duration.ofDays(40)), passedAttempt(PAY, 100));

    store.prune(Duration.ofDays(30), T0.plus(Duration.ofDays(40)));

    Instant from = T0.minus(Duration.ofDays(1));
    Instant to = T0.plus(Duration.ofDays(41));
    assertThat(store.findLatestFailedAttempts(from, to, null, 10).failedAttempts()).isEmpty();
    assertThat(store.findFlakinessPerBucket(from, to, Duration.ofDays(42), null))
        .singleElement()
        .extracting(TrendBucket::attempts)
        .isEqualTo(1);
  }

  @Test
  void
      givenADirectoryAsTheDatabasePath_whenOpening_thenStoreIsDisabledAndRecordingDoesNotThrowButReadsThrowStatsUnavailable() {
    StatsStore unavailableStore = StatsStore.openOrCreateElseDisabled(dir);

    assertThat(unavailableStore).isInstanceOf(DisabledStatsStore.class);
    unavailableStore.recordAttemptsQuietly("s1", 0, 1, List.of(passedAttempt(PAY, 1)), T0);
    assertThatExceptionOfType(StatsUnavailableException.class)
        .isThrownBy(() -> unavailableStore.findLatestFailedAttempts(T0, DAY_LATER, null, 1));
  }

  @Test
  void
      givenAMigratedDatabaseHoldingARecordedFailure_whenReopened_thenTheStoreStaysEnabledAndStillListsTheFailure() {
    record("s1", 1, T0, failedAttempt(PAY, 100, TIMEOUT));
    store.close();

    store = StatsStore.openOrCreateElseDisabled(dir.resolve("stats.db"));

    assertThat(store).isInstanceOf(SqliteStatsStore.class);
    assertThat(store.findLatestFailedAttempts(T0, DAY_LATER, null, 10).failedAttempts()).hasSize(1);
  }

  @Test
  void givenARunawaySessionPastRetention_whenPruning_thenTheDatabaseFileShrinks()
      throws IOException {
    Path databaseFile = dir.resolve("stats.db");
    for (int position = 1; position <= 5_000; position++) {
      Failure distinct =
          new Failure(
              "java.lang.ExceptionInInitializerError",
              "row " + position,
              "java.lang.ExceptionInInitializerError: row " + position + "\n"
                  + "\tat x\n".repeat(50));
      record("runaway", 1, T0, failedAttempt(buildRowsInvocationTestId(position), 0, distinct));
    }
    store.prune(Duration.ofDays(1), T0);
    long grown = Files.size(databaseFile);

    store.prune(Duration.ofDays(1), DAY_LATER.plus(Duration.ofDays(1)));

    assertThat(Files.size(databaseFile)).isLessThan(grown / 10);
  }

  @Test
  void givenADatabaseCreatedWithoutAutoVacuum_whenOpened_thenItIsConvertedAndKeepsItsRows()
      throws SQLException {
    store.close();
    Path legacyFile = dir.resolve("legacy.db");
    try (Connection legacy = DriverManager.getConnection("jdbc:sqlite:" + legacyFile)) {
      StatsSchema.migrateSchemaToLatest(legacy, legacyFile);
    }
    store = StatsStore.openOrCreateElseDisabled(legacyFile);
    record("s1", 1, T0, failedAttempt(PAY, 100, TIMEOUT));
    store.close();

    store = StatsStore.openOrCreateElseDisabled(legacyFile);

    assertThat(store.findLatestFailedAttempts(T0, DAY_LATER, null, 10).failedAttempts()).hasSize(1);
    try (Connection reopened = DriverManager.getConnection("jdbc:sqlite:" + legacyFile);
        ResultSet mode = reopened.createStatement().executeQuery("PRAGMA auto_vacuum")) {
      assertThat(mode.getInt(1)).isEqualTo(1);
    }
  }

  private void record(
      String sessionId, int attemptNumber, Instant acceptedAt, Attempt... attempts) {
    store.recordAttemptsQuietly(sessionId, 0, attemptNumber, List.of(attempts), acceptedAt);
  }

  private static Attempt failedAttempt(String testId, long durationMs, Failure failure) {
    return new Attempt(testId, AttemptKind.UNIT, Outcome.FAILED, durationMs, "boom", failure);
  }

  private static Attempt passedAttempt(String testId, long durationMs) {
    return new Attempt(testId, AttemptKind.UNIT, Outcome.PASSED, durationMs, null, null);
  }

  private static String buildCheckoutMethodId(String methodName) {
    return "[engine:junit-jupiter]/[class:" + CHECKOUT + "]/[method:" + methodName + "()]";
  }

  private static String buildRowsInvocationTestId(int index) {
    return ROWS + "/[test-template-invocation:#" + index + "]";
  }
}
