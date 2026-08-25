package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The quiescence contract: {@code stop()} returns only once no ping can ever fire again.
 * The dangerous shape is a ping blocked in a socket read -- interrupts do not reach it --
 * firing after {@code depart()} and rejoining an explicitly departed shard into the
 * quorum, so the fake ping below blocks exactly like a socket read: uninterruptibly,
 * preserving the interrupt flag for when the read finally returns.
 */
class LivenessKeepaliveTest {

  @Test
  void givenAPingBlockedLikeASocketRead_whenStopping_thenStopReturnsOnlyAfterThePingCompletes()
      throws InterruptedException {
    CountDownLatch pingStarted = new CountDownLatch(1);
    CountDownLatch releasePing = new CountDownLatch(1);
    AtomicInteger pings = new AtomicInteger();
    Runnable ping =
        () -> {
          pings.incrementAndGet();
          pingStarted.countDown();
          boolean interrupted = false;
          while (true) {
            try {
              releasePing.await();
              break;
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        };
    LivenessKeepalive keepalive = LivenessKeepalive.start(ping, Duration.ofMillis(10));
    assertThat(pingStarted.await(5, TimeUnit.SECONDS)).isTrue();

    Thread stopper = new Thread(keepalive::stop, "keepalive-stopper");
    stopper.start();
    stopper.join(300);
    assertThat(stopper.isAlive())
        .as("stop() must not return while a ping is still in flight")
        .isTrue();

    releasePing.countDown();
    stopper.join(5_000);
    assertThat(stopper.isAlive())
        .as("stop() must return once the in-flight ping completes")
        .isFalse();

    int settled = pings.get();
    Thread.sleep(100);
    assertThat(pings.get())
        .as("no ping may fire after stop() has returned")
        .isEqualTo(settled);
  }
}
