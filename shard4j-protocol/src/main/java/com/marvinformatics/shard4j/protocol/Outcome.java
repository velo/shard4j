package com.marvinformatics.shard4j.protocol;

/** What a shard reports for a lease unit or for a single invocation of one. */
public enum Outcome {
  PASSED,
  FAILED,
  /** The execution never started. Requires a reason. */
  SKIPPED,
  /** The execution started and an assumption failed. Requires a reason. */
  ABORTED;

  /**
   * Whether this result measured real work, and so may teach the duration store.
   *
   * <p>{@code PASSED} and {@code ABORTED} both ran: an assumption that fails after three
   * minutes of setup spent those three minutes, and the coverage verdict already counts
   * the outcome as terminal-OK. {@code SKIPPED} never started, and {@code FAILED} timed a
   * failure rather than the work -- neither may set an estimate the scheduler orders by,
   * nor stand in for a row in a distribution plan.
   */
  public boolean measuredWork() {
    return this == PASSED || this == ABORTED;
  }
}
