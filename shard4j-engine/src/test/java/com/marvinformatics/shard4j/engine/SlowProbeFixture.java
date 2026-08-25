package com.marvinformatics.shard4j.engine;

import org.junit.jupiter.api.Test;

/** Ordering probe with the longest seeded durations; alphabetically last on purpose. */
class SlowProbeFixture {

  @Test
  void slowest() {
    OrderProbeRecorder.record("SlowProbeFixture#slowest");
  }

  @Test
  void slower() {
    OrderProbeRecorder.record("SlowProbeFixture#slower");
  }
}
