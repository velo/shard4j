package com.marvinformatics.shard4j.engine;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The other abort shape: the class container aborts in {@code @BeforeAll} and Jupiter
 * emits no events at all for either leaf -- and the abort does not propagate upward, so
 * only a per-unit accounting can explain them.
 */
class AbortedSetupFixture {

  @BeforeAll
  static void requiresAService() {
    assumeTrue(false, "local service is not running");
  }

  @Test
  void first() {}

  @Test
  void second() {}
}
