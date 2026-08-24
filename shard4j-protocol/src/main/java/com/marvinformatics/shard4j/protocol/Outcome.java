package com.marvinformatics.shard4j.protocol;

/** What a shard reports for a lease unit or for a single invocation of one. */
public enum Outcome {
  PASSED,
  FAILED,
  /** The execution never started. Requires a reason. */
  SKIPPED,
  /** The execution started and an assumption failed. Requires a reason. */
  ABORTED
}
