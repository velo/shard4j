package com.marvinformatics.shard4j.protocol;

/**
 * Removes the shard from barrier quorums and live-shard counts. Free to send, and it
 * converts "someone will poll soon" into "no one will ever take this". Epoch-fenced like
 * a barrier arrival: a departure announced under a previous attempt's epoch says nothing
 * about this one.
 */
public record DepartRequest(int shard, long epoch) {}
