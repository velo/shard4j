package com.example.orders;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Sharding fixture: fails its first execution and passes afterwards, so a run only goes
 * green if the failure is re-handed through the barrier into a retry pass. The static
 * counter is only correct in the in-process harness; under real per-pass forks each JVM
 * starts it fresh and the fixture fails terminally, so do not promote it into the
 * coordinated profile as-is.
 */
@Tag("shard4j-fixture")
class FlakyGatewayIT {

  private static final AtomicInteger ATTEMPTS = new AtomicInteger();

  @Test
  void retriesAgainstTheGateway() {
    if (ATTEMPTS.incrementAndGet() == 1) {
      fail("gateway timed out on the first attempt");
    }
  }
}
