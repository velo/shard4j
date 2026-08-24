package com.marvinformatics.shard4j.protocol;

import java.util.Comparator;

/**
 * Lease fencing token, compared lexicographically.
 *
 * @param epoch bumped by a registration carrying a higher attempt
 * @param incarnation coordinator process counter, persisted and fsynced before the
 *     first request is served
 * @param seq in-memory monotonic counter
 */
public record Fence(long epoch, long incarnation, long seq) implements Comparable<Fence> {

  private static final Comparator<Fence> LEXICOGRAPHIC =
      Comparator.comparingLong(Fence::epoch)
          .thenComparingLong(Fence::incarnation)
          .thenComparingLong(Fence::seq);

  @Override
  public int compareTo(Fence other) {
    return LEXICOGRAPHIC.compare(this, other);
  }
}
