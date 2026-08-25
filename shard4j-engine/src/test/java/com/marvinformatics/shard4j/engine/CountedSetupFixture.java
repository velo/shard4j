package com.marvinformatics.shard4j.engine;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Counts its {@code @BeforeAll} executions: the drain test pins that everything a shard
 * runs in one class shares one nested execution, and so pays the class setup once.
 */
class CountedSetupFixture {

  static final AtomicInteger SETUPS = new AtomicInteger();

  @BeforeAll
  static void countSetup() {
    SETUPS.incrementAndGet();
  }

  @Test
  void one() {}

  @Test
  void two() {}

  @Test
  void three() {}
}
