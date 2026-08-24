package com.marvinformatics.shard4j.protocol;

/**
 * Removes the shard from barrier quorums and live-shard counts. Free to send, and it
 * converts "someone will poll soon" into "no one will ever take this".
 */
public record DepartRequest(int shard) {}
