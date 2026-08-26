package com.marvinformatics.shard4j.protocol;

/**
 * Arrival is the pass-completion report; polled while waiting. {@code epoch} is the fence,
 * exactly as a result is fenced by its lease: an arrival mutates the shard roster, and
 * after an epoch bump a zombie of the previous attempt must not be able to resurrect
 * itself into the waiter tally or the quorum of an attempt it is not part of.
 */
public record BarrierRequest(int shard, long epoch) {}
