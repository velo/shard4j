package com.marvinformatics.shard4j.engine;

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
  private int shardIndex;
  private int concurrency = 1;
  private Integer shardCount;

  private ShardConfigurationBuilder(String coordinatorUrl, String sessionId) {
    this.coordinatorUrl = coordinatorUrl;
    this.sessionId = sessionId;
  }

  static ShardConfigurationBuilder coordinatedShard(String coordinatorUrl, String sessionId) {
    return new ShardConfigurationBuilder(coordinatorUrl, sessionId);
  }

  ShardConfigurationBuilder shardIndex(int shardIndex) {
    this.shardIndex = shardIndex;
    return this;
  }

  ShardConfigurationBuilder concurrency(int concurrency) {
    this.concurrency = concurrency;
    return this;
  }

  ShardConfigurationBuilder shardCount(int shardCount) {
    this.shardCount = shardCount;
    return this;
  }

  ShardConfiguration build() {
    return new ShardConfiguration(
        true,
        coordinatorUrl,
        CoordinatorContainer.SECRET,
        sessionId,
        shardIndex,
        1,
        concurrency,
        shardCount,
        Map.of(),
        Duration.ofSeconds(30),
        null,
        true);
  }
}
