package com.example.orders.fixtures;

import static org.assertj.core.api.Assertions.fail;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Sharding fixture: fails its first execution and passes afterwards, so a run only goes
 * green if the failure is requeued and taken again. The static counter is only correct in
 * the in-process harness; under real forks each JVM starts it fresh and the fixture fails
 * terminally, so do not promote it into the coordinated profile as-is.
 *
 * <p>Because the counter is JVM-wide it is also shared by every harness test that drives
 * this fixture, and a second one silently spends the first one's failure -- leaving the
 * next test to observe a unit that passed first time and no retry at all. Any test using
 * this fixture must call {@link #resetAttempts()} in its own setup.
 */
@Tag("shard4j-fixture")
public class FlakyGatewayIT {

  private static final AtomicInteger ATTEMPTS = new AtomicInteger();

  /** Re-arms the fixture so the next execution fails once more. */
  public static void resetAttempts() {
    ATTEMPTS.set(0);
  }

  @Test
  void retriesAgainstTheGateway() {
    if (ATTEMPTS.incrementAndGet() == 1) {
      fail("gateway timed out on the first attempt");
    }
  }
}
