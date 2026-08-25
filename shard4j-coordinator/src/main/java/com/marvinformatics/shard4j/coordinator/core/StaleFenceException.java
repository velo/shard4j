package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.Fence;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * A write under a fence that no longer holds the lease: the lease was reclaimed, the epoch
 * was bumped, or an older incarnation wrote. The payload is kept aside with zero effect on
 * state, and the answer carries the fence that beat it so the shard's log says why.
 */
public class StaleFenceException extends RuntimeException {

  @Getter
  @Accessors(fluent = true)
  private final transient Fence currentFence;

  public StaleFenceException(Fence currentFence) {
    super("Stale fence; current is " + currentFence);
    this.currentFence = currentFence;
  }
}
