package com.marvinformatics.shard4j.protocol;

/** Acknowledges that the shard is out of every barrier quorum and live-shard count. */
public record DepartResponse(int shard, boolean departed) {}
