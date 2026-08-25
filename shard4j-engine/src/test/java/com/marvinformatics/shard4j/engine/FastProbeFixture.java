package com.marvinformatics.shard4j.engine;

import org.junit.jupiter.api.Test;

/** Ordering probe with the shortest seeded durations; alphabetically first on purpose. */
class FastProbeFixture {

  @Test
  void fast() {
    OrderProbeRecorder.record("FastProbeFixture#fast");
  }

  @Test
  void faster() {
    OrderProbeRecorder.record("FastProbeFixture#faster");
  }
}
