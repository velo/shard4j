package com.marvinformatics.shard4j.protocol;

/**
 * The open ask: the shard has capacity and the coordinator decides what it runs next.
 * Candidates would be redundant here -- registration already gave the coordinator the
 * whole census, and the choice being the coordinator's is the entire point.
 */
public record NextClassRequest(int shard) {}
