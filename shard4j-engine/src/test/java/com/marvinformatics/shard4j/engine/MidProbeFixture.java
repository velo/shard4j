package com.marvinformatics.shard4j.engine;

import org.junit.jupiter.api.Test;

/** Ordering probe with mid-range seeded durations; alphabetically in the middle. */
class MidProbeFixture {

  @Test
  void mid() {
    OrderProbeRecorder.record("MidProbeFixture#mid");
  }

  @Test
  void milder() {
    OrderProbeRecorder.record("MidProbeFixture#milder");
  }
}
