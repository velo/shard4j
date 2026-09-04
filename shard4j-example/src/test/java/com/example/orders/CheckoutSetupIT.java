package com.example.orders;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Sharding fixture: the class-level abort shape. {@code @BeforeAll} aborts, so no event is
 * ever emitted for either test below -- the harness asserts both still reach ABORTED.
 */
@Tag("shard4j-fixture")
class CheckoutSetupIT {

  @BeforeAll
  static void requiresPaymentSandbox() {
    assumeTrue(false, "payment sandbox is unreachable");
  }

  @Test
  void authorisesCard() {}

  @Test
  void capturesFunds() {}
}
