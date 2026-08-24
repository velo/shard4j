package com.marvinformatics.shard4j.protocol;

/**
 * Overall success is coverage -- every registered lease unit reached a terminal
 * non-failing state -- never shard exit codes and never queue emptiness.
 */
public enum SessionVerdict {
  PASSED,
  FAILED,
  /** Every shard departed while tests remained non-terminal. */
  INCOMPLETE
}
