package com.marvinformatics.shard4j.protocol;

/**
 * Coordinator-side state of one lease unit.
 *
 * <p>{@code PASSED}, {@code SKIPPED} and {@code ABORTED} are absorbing and all three
 * satisfy the coverage verdict. {@code FAILED} is claimable again in the next pass.
 */
public enum TestState {
  PENDING,
  LEASED,
  PASSED,
  FAILED,
  SKIPPED,
  ABORTED
}
