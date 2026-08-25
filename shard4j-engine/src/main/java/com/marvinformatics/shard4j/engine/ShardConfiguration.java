package com.marvinformatics.shard4j.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * The consumer-facing configuration surface: system properties first, then environment
 * variables ({@code shard.foo.bar} maps to {@code SHARD_FOO_BAR}), with one exception.
 *
 * <p>Absent or false {@code enabled} means the engine is completely inert: no network
 * call, nothing claimed. With {@code enabled} true, any missing required key is a hard
 * failure naming the key -- never a silent fall-through to running everything.
 *
 * <p>There is no default for {@code coordinatorUrl}, and there never will be.
 *
 * @param coordinatorSecret read from the environment only. A value supplied as a system
 *     property is refused with an explanation: properties appear in {@code ps} output, in
 *     failsafe's argLine echo, and in crash dumps.
 * @param metadata forwarded verbatim as the registration metadata map
 */
public record ShardConfiguration(
    boolean enabled,
    String coordinatorUrl,
    String coordinatorSecret,
    String sessionId,
    int shardIndex,
    String pass,
    int attempt,
    Map<String, String> metadata,
    Duration retryBudget,
    Instant deadline,
    boolean allLeasedAbortedIsFailure) {

  public static ShardConfiguration fromEnvironment() {
    throw new UnsupportedOperationException("not implemented");
  }
}
