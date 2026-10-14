package com.marvinformatics.shard4j.coordinator.stats;

/** The statistics database could not be opened; scheduling carries on without it. */
public final class StatsUnavailableException extends RuntimeException {

  public StatsUnavailableException(String reason) {
    super("Failure statistics are unavailable on this instance: " + reason);
  }
}
