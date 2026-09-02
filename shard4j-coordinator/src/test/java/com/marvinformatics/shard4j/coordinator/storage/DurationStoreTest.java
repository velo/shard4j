package com.marvinformatics.shard4j.coordinator.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.HistoryKey;
import com.marvinformatics.shard4j.protocol.Outcome;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurationStoreTest {

  private static final HistoryKey KEY = new HistoryKey("com.example.orders.OrderIT#slow()");

  @TempDir Path dir;

  private DurationStore store() {
    return new DurationStore(dir.resolve("current.json"), 3_600_000L);
  }

  @Test
  void estimateIsTheMedianOverTheLastFiveDistinctSessions() {
    DurationStore store = store();
    long[] durations = {10, 20, 30, 40, 50, 900};
    for (int i = 0; i < durations.length; i++) {
      store.recordMeasured(KEY, "session-" + i, durations[i], false);
    }
    // The oldest session fell out of the window; median of 20,30,40,50,900 is 40.
    assertThat(store.estimate(KEY)).hasValue(40L);
  }

  @Test
  void aSecondRecordFromTheSameSessionDoesNotShrinkTheWindow() {
    DurationStore store = store();
    store.recordMeasured(KEY, "session-a", 100, false);
    store.recordMeasured(KEY, "session-a", 999, false);
    store.recordMeasured(KEY, "session-b", 200, false);
    assertThat(store.estimate(KEY)).hasValue(150L);
  }

  @Test
  void firstOnShardRowsAreIgnoredUnlessTheyAreAllThereIs() {
    DurationStore store = store();
    store.recordMeasured(KEY, "session-a", 17_600, true);
    assertThat(store.estimate(KEY)).hasValue(17_600L);
    store.recordMeasured(KEY, "session-b", 1_300, false);
    assertThat(store.estimate(KEY)).hasValue(1_300L);
  }

  @Test
  void valuesAboveTheClampAreDiscardedNotStored() {
    DurationStore store = store();
    store.recordMeasured(KEY, "session-a", 3_600_001L, false);
    assertThat(store.estimate(KEY)).isEmpty();
  }

  @Test
  void unknownIsAbsenceOfTheKeyNotASentinel() {
    assertThat(store().estimate(new HistoryKey("com.example.Never#ran()"))).isEmpty();
  }

  @Test
  void snapshotRoundTripsThroughDisk() {
    DurationStore store = store();
    store.recordMeasured(KEY, "session-a", 500, false);
    store.saveSnapshot();

    DurationStore reloaded = store();
    assertThat(reloaded.loadSnapshot()).isTrue();
    assertThat(reloaded.estimate(KEY)).hasValue(500L);
  }

  @Test
  void missingSnapshotReportsColdSoTheSeedFoldCanRun() {
    assertThat(store().loadSnapshot()).isFalse();
  }

  @Test
  void invocationPlanComesOnlyFromACompleteBreakdown() {
    DurationStore store = store();
    store.recordInvocation(KEY, "session-a", 1, 40);
    store.recordInvocation(KEY, "session-a", 2, 50);
    assertThat(store.invocationPlan(KEY)).isEmpty();

    store.markInvocationsComplete(KEY, "session-a");
    assertThat(store.invocationPlan(KEY)).containsExactly(1, 2);
  }

  @Test
  void planPositionsSortNumericallyNotLexicographically() {
    DurationStore store = store();
    for (int position = 1; position <= 11; position++) {
      store.recordInvocation(KEY, "session-a", position, 10);
    }
    store.markInvocationsComplete(KEY, "session-a");
    assertThat(store.invocationPlan(KEY).get(1)).isEqualTo(2);
    assertThat(store.invocationPlan(KEY).get(10)).isEqualTo(11);
  }

  @Test
  void newestCompleteBreakdownWinsAndAnIncompleteNewerOneDoesNotHideIt() {
    DurationStore store = store();
    store.recordInvocation(KEY, "session-a", 1, 40);
    store.recordInvocation(KEY, "session-a", 2, 50);
    store.markInvocationsComplete(KEY, "session-a");
    // The newer session saw only one row finish; trusting it would drop #2 silently.
    store.recordInvocation(KEY, "session-b", 1, 41);
    assertThat(store.invocationPlan(KEY)).containsExactly(1, 2);
  }

  @Test
  void invocationEstimateIsPerPositionAndTheMethodEstimateIsTheirSum() {
    DurationStore store = store();
    store.recordInvocation(KEY, "session-a", 1, 40);
    store.recordInvocation(KEY, "session-a", 2, 60);
    store.recordInvocation(KEY, "session-b", 1, 50);
    assertThat(store.invocationEstimate(KEY, 1)).hasValue(45L);
    assertThat(store.invocationEstimate(KEY, 2)).hasValue(60L);
    assertThat(store.invocationEstimate(KEY, 9)).isEmpty();
    // An entry accreted from invocations carries their sum as the method-level figure.
    assertThat(store.estimate(KEY)).hasValue(75L);
  }

  @Test
  void attachingRowsToAWholeMethodEntryNeverOverwritesItsMeasuredTotal() {
    DurationStore store = store();
    store.recordMeasured(KEY, "session-a", 220, false);
    store.recordInvocation(KEY, "session-a", 1, 40);
    store.recordInvocation(KEY, "session-a", 2, 50);
    assertThat(store.estimate(KEY)).hasValue(220L);
    store.markInvocationsComplete(KEY, "session-a");
    assertThat(store.invocationPlan(KEY)).containsExactly(1, 2);
  }

  @Test
  void droppingAVanishedPositionRemovesItFromEveryWindowEntry() {
    DurationStore store = store();
    store.recordInvocation(KEY, "session-a", 1, 40);
    store.recordInvocation(KEY, "session-a", 2, 50);
    store.markInvocationsComplete(KEY, "session-a");
    store.dropInvocation(KEY, 2);
    assertThat(store.invocationPlan(KEY)).containsExactly(1);
    assertThat(store.invocationEstimate(KEY, 2)).isEmpty();
    assertThat(store.estimate(KEY)).hasValue(40L);
  }

  @Test
  void breakdownSurvivesTheSnapshotRoundTrip() {
    DurationStore store = store();
    store.recordInvocation(KEY, "session-a", 1, 40);
    store.recordInvocation(KEY, "session-a", 2, 50);
    store.markInvocationsComplete(KEY, "session-a");
    store.saveSnapshot();

    DurationStore reloaded = store();
    assertThat(reloaded.loadSnapshot()).isTrue();
    assertThat(reloaded.invocationPlan(KEY)).containsExactly(1, 2);
    assertThat(reloaded.invocationEstimate(KEY, 2)).hasValue(50L);
  }

  @Test
  void coldLoadRebuildsAPassedTemplatesBreakdownAsComplete() {
    String template =
        "[engine:junit-jupiter]/[class:com.example.orders.OrderIT]"
            + "/[test-template:rows(java.lang.String)]";
    HistoryKey key = new HistoryKey("com.example.orders.OrderIT#rows(java.lang.String)");
    DurationStore store = store();
    store.coldLoad(
        List.of(
            unitRow(template, Outcome.PASSED, 90),
            invocationRow(template + "/[test-template-invocation:#1]", Outcome.PASSED, 40),
            invocationRow(template + "/[test-template-invocation:#2]", Outcome.SKIPPED, 0),
            invocationRow(template + "/[test-template-invocation:#3]", Outcome.PASSED, 50)));
    assertThat(store.invocationPlan(key)).containsExactly(1, 2, 3);
    assertThat(store.estimate(key)).hasValue(90L);
  }

  @Test
  void coldLoadOfADistributedSessionKeepsDurationsButTrustsNoPlan() {
    String template =
        "[engine:junit-jupiter]/[class:com.example.orders.OrderIT]"
            + "/[test-template:rows(java.lang.String)]";
    HistoryKey key = new HistoryKey("com.example.orders.OrderIT#rows(java.lang.String)");
    DurationStore store = store();
    // Individually-leased invocations complete as unit rows carrying the invocation id;
    // whether the session saw every position finish is not reconstructable from rows.
    store.coldLoad(
        List.of(
            unitRow(template + "/[test-template-invocation:#1]", Outcome.PASSED, 40),
            unitRow(template + "/[test-template-invocation:#2]", Outcome.PASSED, 50)));
    assertThat(store.invocationPlan(key)).isEmpty();
    assertThat(store.invocationEstimate(key, 1)).hasValue(40L);
    assertThat(store.estimate(key)).hasValue(90L);
  }

  private static LogRecord unitRow(String testId, Outcome outcome, long durationMs) {
    return LogRecord.unitCompletion(
        "example/orders-service",
        "seeded-elsewhere",
        1,
        testId,
        0,
        1,
        outcome,
        durationMs,
        false,
        null,
        Instant.parse("2026-08-20T10:00:00Z"));
  }

  private static LogRecord invocationRow(String testId, Outcome outcome, long durationMs) {
    return LogRecord.invocationCompletion(
        "example/orders-service",
        "seeded-elsewhere",
        1,
        testId,
        0,
        1,
        outcome,
        durationMs,
        null,
        Instant.parse("2026-08-20T10:00:00Z"));
  }
}
