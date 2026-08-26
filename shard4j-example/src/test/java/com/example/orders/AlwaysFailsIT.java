package com.example.orders;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Sharding fixture: fails on every attempt, so the budget is spent rather than recovered.
 * The counterpart to {@link FlakyGatewayIT}, which fails once and then passes -- between
 * them they cover both exits from the retry model, and only this one proves that an
 * exhausted budget still ends FAILED instead of quietly draining into a green session.
 */
@Tag("shard4j-fixture")
class AlwaysFailsIT {

  @Test
  void neverPasses() {
    throw new AssertionError("this fixture fails every attempt, by design");
  }
}
