package com.marvinformatics.shard4j.protocol;

import java.time.Instant;

/**
 * One leased unit. {@code probe} marks a cardinality probe: an invocation id one position
 * past a template's recorded parameter count, handed out to discover growth. A probe that
 * does not materialise is expected and is returned quietly; a non-probe invocation that
 * does not materialise means the parameter set shrank since it was measured, which is a
 * loud failure.
 */
public record Grant(
    String testId, Fence fence, Instant expiresAt, boolean probe, int attemptsRemaining) {

  /**
   * True when a failure of this attempt would be requeued rather than made terminal. The
   * engine uses it to decide what to tell the *launcher* -- an aborted leaf keeps failsafe
   * green while the retry is still owed -- and nothing else. What the coordinator is told
   * is always the real outcome: downgrading toward the coordinator would turn a genuine
   * failure into passing coverage, because the verdict counts ABORTED as terminal-OK.
   */
  public boolean retryable() {
    return attemptsRemaining > 1;
  }
}
