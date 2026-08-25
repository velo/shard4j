package com.marvinformatics.shard4j.engine;

import java.time.Duration;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * The engine's half of the liveness contract: the coordinator presumes dead any
 * unreleased shard that holds no lease and has been silent for three barrier-poll
 * intervals, in every phase -- including phases where nothing mandates a cadence, such as
 * a slow {@code @AfterAll} between classes with every lease already reported. This thread
 * pings an empty claim well inside that tolerance so a live shard is never presumed dead
 * and released out of a session that still has work only it will claim.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
final class LivenessKeepalive {

  private static final System.Logger log = System.getLogger(LivenessKeepalive.class.getName());

  private static final Duration INTERVAL = Duration.ofSeconds(5);

  private final Thread thread;

  static LivenessKeepalive start(Runnable ping) {
    return start(ping, INTERVAL);
  }

  static LivenessKeepalive start(Runnable ping, Duration interval) {
    Thread thread =
        new Thread(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  Thread.sleep(interval.toMillis());
                  ping.run();
                } catch (InterruptedException e) {
                  return;
                } catch (RuntimeException e) {
                  // Best effort by design: the main loop's own calls also count as life,
                  // and a dead coordinator fails those loudly where failing here would
                  // only kill the messenger.
                  log.log(
                      System.Logger.Level.DEBUG,
                      "Keepalive ping failed; the next one will try again",
                      e);
                }
              }
            },
            "shard4j-keepalive");
    thread.setDaemon(true);
    thread.start();
    return new LivenessKeepalive(thread);
  }

  /**
   * Quiesces, not merely signals: an interrupt cannot reach a ping blocked in a socket
   * read, and a claim packet that lands after {@code depart()} rejoins the shard into the
   * quorum -- a claim is proof of life -- undoing an explicit departure that barrier
   * packets are carefully fenced against reviving. So stop() returns only once the
   * keepalive thread is dead, which is what lets the caller order it before departing.
   */
  void stop() {
    thread.interrupt();
    try {
      thread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
