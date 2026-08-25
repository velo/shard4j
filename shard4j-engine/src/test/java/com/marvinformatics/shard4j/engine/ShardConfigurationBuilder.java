package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.Pass;
import java.time.Duration;
import java.util.Map;

/**
 * Test-side builder: a 12-argument positional constructor call hides a transposed pair of
 * ints -- attempt and concurrency sit side by side -- behind a clean compile, silently
 * turning a two-slot test serial. Tests name what they mean instead, and the one
 * positional call site left lives here.
 */
final class ShardConfigurationBuilder {

  private final String coordinatorUrl;
  private final String sessionId;
  private int concurrency = 1;

  private ShardConfigurationBuilder(String coordinatorUrl, String sessionId) {
    this.coordinatorUrl = coordinatorUrl;
    this.sessionId = sessionId;
  }

  static ShardConfigurationBuilder coordinatedShard(String coordinatorUrl, String sessionId) {
    return new ShardConfigurationBuilder(coordinatorUrl, sessionId);
  }

  ShardConfigurationBuilder concurrency(int concurrency) {
    this.concurrency = concurrency;
    return this;
  }

  ShardConfiguration build() {
    return new ShardConfiguration(
        true,
        coordinatorUrl,
        CoordinatorContainer.SECRET,
        sessionId,
        0,
        Pass.MAIN,
        1,
        concurrency,
        Map.of(),
        Duration.ofSeconds(30),
        null,
        true);
  }
}
