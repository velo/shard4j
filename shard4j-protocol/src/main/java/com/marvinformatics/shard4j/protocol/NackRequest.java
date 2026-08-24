package com.marvinformatics.shard4j.protocol;

import java.util.List;

/**
 * Explicit release of leases a shard cannot report. Fencing gates the NACK exactly as it
 * gates a result write: a stale NACK would requeue a test a reclaiming shard is
 * legitimately running.
 */
public record NackRequest(int shard, List<NackedLease> leases) {

  public record NackedLease(String testId, Fence fence, String reason) {}
}
