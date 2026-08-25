package com.marvinformatics.shard4j.protocol;

import java.util.List;

/**
 * Explicit release of leases a shard cannot report. Fencing gates the NACK exactly as it
 * gates a result write: a stale NACK would requeue a test a reclaiming shard is
 * legitimately running.
 */
public record NackRequest(int shard, List<NackedLease> leases) {

  /**
   * {@code vanished} means the shard proved the id does not exist in the current
   * discovery: the nested execution selected it and JUnit materialised nothing. For a
   * probe the coordinator absorbs that as confirmation of the recorded parameter count;
   * for a measured invocation it means the parameter set changed since it was last
   * measured, and the stale position is dropped from duration history.
   */
  public record NackedLease(String testId, Fence fence, String reason, boolean vanished) {}
}
