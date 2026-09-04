package com.example.orders;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Ordering fixture, slowest of the three. The sleeps exist so the coordinator has real
 * durations to rank: slowest-first is unobservable when every class costs the same, and a
 * test that cannot observe the ordering cannot catch it regressing.
 */
@Tag("shard4j-fixture")
class SlowBrewIT {

  @Test
  void steeps() {
    Sleeps.forMillis(900);
  }
}
