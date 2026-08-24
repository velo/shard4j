package com.marvinformatics.shard4j.protocol;

/** Arrival is the pass-completion report; polled while waiting. */
public record BarrierRequest(int shard, Pass completedPass) {}
