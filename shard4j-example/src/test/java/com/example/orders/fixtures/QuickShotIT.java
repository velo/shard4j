package com.example.orders.fixtures;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Ordering fixture, fastest of the three. See {@link SlowBrewIT}. */
@Tag("shard4j-fixture")
public class QuickShotIT {

  @Test
  void pulls() {
    Sleeps.forMillis(80);
  }
}
