package com.marvinformatics.shard4j.protocol;

import java.time.Instant;

/** {@code DONE} means "stop pulling", not "the session passed". */
public record BarrierResponse(Action action, Integer retryAfterSeconds, Instant earliestLeaseExpiry) {

  public enum Action {
    RUN,
    WAIT,
    DONE
  }
}
