package com.marvinformatics.shard4j.coordinator.core;

/**
 * A barrier arrival or departure carrying an epoch the session has moved past. Results
 * are fenced per lease; these two calls hold no lease, so the session epoch is their
 * fence. The call has zero effect on state: above all it must not resurrect the sender
 * into the roster, the waiter tally or a quorum of an attempt it is not part of.
 */
public class StaleEpochException extends RuntimeException {

  public StaleEpochException(long requestEpoch, long currentEpoch) {
    super("Stale epoch " + requestEpoch + "; the session is at epoch " + currentEpoch);
  }
}
