package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.CensusUnit;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.NackRequest;
import java.util.ArrayList;
import java.util.List;

/**
 * The pass epilogue's classification of leases the engine could not explain with a
 * terminal outcome, as a pure function over the drained grants -- so the three-way
 * decision and every NACK's wording are unit-testable without a container.
 *
 * <p>What an unexplained lease means depends on what was granted. A cardinality probe
 * that never materialised is the expected answer: NACKed as vanished so the coordinator
 * strikes it from the census, and nothing fails. A measured invocation that never
 * materialised is parameter-set drift: the {@code @MethodSource} changed since the
 * coordinator last measured the method, so the position was handed out optimistically
 * and no longer exists -- NACKed as vanished so history drops the stale position, and
 * the shard fails naming exactly that cause, because a run that silently skipped a
 * once-real invocation must never look green. Anything else is a lease this engine
 * cannot explain -- a bug in the engine, never a property of the suite -- NACKed back to
 * the pool and failed as such.
 *
 * <p>{@code failure} is null when every unexplained lease was a probe; otherwise it is
 * the composed message the shard fails with.
 */
record Reconciliation(List<NackRequest.NackedLease> nacks, String failure) {

  static Reconciliation classify(List<Grant> unexplained, int shardIndex) {
    List<NackRequest.NackedLease> nacks = new ArrayList<>();
    List<String> driftedInvocations = new ArrayList<>();
    List<String> unexplainable = new ArrayList<>();
    for (Grant grant : unexplained) {
      boolean invocation = invocationShaped(grant.testId());
      if (invocation && grant.probe()) {
        nacks.add(
            new NackRequest.NackedLease(
                grant.testId(),
                grant.fence(),
                "Cardinality probe past recorded history did not materialise on shard "
                    + shardIndex
                    + "); the recorded parameter count still stands",
                true));
      } else if (invocation) {
        nacks.add(
            new NackRequest.NackedLease(
                grant.testId(),
                grant.fence(),
                "Invocation no longer exists on shard "
                    + shardIndex
                    + "): the parameter set changed since this invocation was last"
                    + " measured; dropped from the plan and returned to the pool",
                true));
        driftedInvocations.add(grant.testId());
      } else {
        nacks.add(
            new NackRequest.NackedLease(
                grant.testId(),
                grant.fence(),
                "Leased but never produced a terminal outcome on shard "
                    + shardIndex
                    + "); returned to the pool",
                false));
        unexplainable.add(grant.testId());
      }
    }
    return new Reconciliation(nacks, failureOf(shardIndex, driftedInvocations, unexplainable));
  }

  /** The wording for leases abandoned wholesale -- a mid-pass failure or a SIGTERM. */
  static List<NackRequest.NackedLease> abandoned(
      List<Grant> outstanding, int shardIndex, String cause) {
    return outstanding.stream()
        .map(
            grant ->
                new NackRequest.NackedLease(
                    grant.testId(),
                    grant.fence(),
                    "Abandoned on shard "
                        + shardIndex
                        + "): "
                        + cause
                        + "; returned to the pool",
                    false))
        .toList();
  }

  private static String failureOf(
      int shardIndex, List<String> driftedInvocations, List<String> unexplainable) {
    if (driftedInvocations.isEmpty() && unexplainable.isEmpty()) {
      return null;
    }
    StringBuilder message = new StringBuilder("Shard ").append(shardIndex);
    if (!driftedInvocations.isEmpty()) {
      message
          .append(" was granted ")
          .append(driftedInvocations.size())
          .append(" invocation(s) that no longer exist -- the parameter set changed since")
          .append(" they were last measured -- ")
          .append(String.join(", ", driftedInvocations))
          .append("; history has dropped them and the next run expands from the corrected")
          .append(" plan");
    }
    if (!unexplainable.isEmpty()) {
      if (!driftedInvocations.isEmpty()) {
        message.append(". It also");
      }
      message
          .append(" could not reconcile ")
          .append(unexplainable.size())
          .append(" leased unit(s) to a terminal outcome; they were NACKed back to the")
          .append(" pool: ")
          .append(String.join(", ", unexplainable));
    }
    return message.toString();
  }

  /** The one grammar decides; a shape it cannot parse falls to the unexplained arm. */
  private static boolean invocationShaped(String unitId) {
    try {
      return CensusUnit.parse(unitId).invocation() != null;
    } catch (IllegalArgumentException notClaimable) {
      return false;
    }
  }
}
