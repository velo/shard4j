package com.marvinformatics.shard4j.coordinator.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.HistoryKey;
import java.nio.file.Path;
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
      store.recordPassed(KEY, "session-" + i, durations[i], false);
    }
    // The oldest session fell out of the window; median of 20,30,40,50,900 is 40.
    assertThat(store.estimate(KEY)).hasValue(40L);
  }

  @Test
  void aSecondRecordFromTheSameSessionDoesNotShrinkTheWindow() {
    DurationStore store = store();
    store.recordPassed(KEY, "session-a", 100, false);
    store.recordPassed(KEY, "session-a", 999, false);
    store.recordPassed(KEY, "session-b", 200, false);
    assertThat(store.estimate(KEY)).hasValue(150L);
  }

  @Test
  void firstOnShardRowsAreIgnoredUnlessTheyAreAllThereIs() {
    DurationStore store = store();
    store.recordPassed(KEY, "session-a", 17_600, true);
    assertThat(store.estimate(KEY)).hasValue(17_600L);
    store.recordPassed(KEY, "session-b", 1_300, false);
    assertThat(store.estimate(KEY)).hasValue(1_300L);
  }

  @Test
  void valuesAboveTheClampAreDiscardedNotStored() {
    DurationStore store = store();
    store.recordPassed(KEY, "session-a", 3_600_001L, false);
    assertThat(store.estimate(KEY)).isEmpty();
  }

  @Test
  void unknownIsAbsenceOfTheKeyNotASentinel() {
    assertThat(store().estimate(new HistoryKey("com.example.Never#ran()"))).isEmpty();
  }

  @Test
  void snapshotRoundTripsThroughDisk() {
    DurationStore store = store();
    store.recordPassed(KEY, "session-a", 500, false);
    store.saveSnapshot();

    DurationStore reloaded = store();
    assertThat(reloaded.loadSnapshot()).isTrue();
    assertThat(reloaded.estimate(KEY)).hasValue(500L);
  }

  @Test
  void missingSnapshotReportsColdSoTheSeedFoldCanRun() {
    assertThat(store().loadSnapshot()).isFalse();
  }
}
