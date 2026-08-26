package com.marvinformatics.shard4j.protocol;

/**
 * Coordinator-side state of one lease unit.
 *
 * <p>{@code PASSED}, {@code SKIPPED} and {@code ABORTED} are absorbing and all three
 * satisfy the coverage verdict. {@code FAILED} is terminal: it means the
 * attempt budget is spent. A failure with budget left is put back to {@code PENDING}.
 */
public enum TestState {
  PENDING,
  LEASED,
  PASSED,
  FAILED,
  SKIPPED,
  ABORTED;

  /** Absorbing states never re-enter any claimable pool, not even across an epoch bump. */
  public boolean isAbsorbing() {
    return this == PASSED || this == SKIPPED || this == ABORTED;
  }
}
