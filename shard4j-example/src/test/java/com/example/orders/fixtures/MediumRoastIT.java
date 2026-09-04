package com.example.orders.fixtures;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Ordering fixture, middle of the three. See {@link SlowBrewIT}. */
@Tag("shard4j-fixture")
public class MediumRoastIT {

  @Test
  void pours() {
    Sleeps.forMillis(450);
  }
}
