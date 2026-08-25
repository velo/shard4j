package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Grant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The reconciliation ledger: every lease this shard holds and has not yet explained with
 * a terminal outcome. Both halves of the fence invariant live here:
 *
 * <p>{@link #track} lets the newest fence win per unit -- a unit re-pooled mid-run
 * (coordinator restart, lease TTL expiry, both supported events) can be re-granted to a
 * sibling slot under a newer fence while the first slot is still running it, and the
 * re-grant overwrites the stale entry. {@link #explain} then removes only on a fence
 * match, so the stale batch's report explains away its own grant alone. Removing blindly
 * would strip the live lease from the ledger: a later failure could not NACK it, and a
 * silent drop would reconcile to a clean exit while the unit sits leased and unrun.
 */
final class LeaseLedger {

  private final Map<String, Grant> outstanding = new LinkedHashMap<>();

  /**
   * Every grant enters the ledger the moment it arrives, not when its batch runs: a
   * failure between claiming and running -- a transport death mid-drain, a malformed
   * grant -- must NACK what was already leased instead of abandoning it to the TTL.
   */
  void track(List<Grant> grants) {
    synchronized (outstanding) {
      grants.forEach(grant -> outstanding.put(grant.testId(), grant));
    }
  }

  /** Removes the unit only while the ledger still holds it under the reporting fence. */
  void explain(String unitId, Fence fence) {
    synchronized (outstanding) {
      Grant tracked = outstanding.get(unitId);
      if (tracked != null && tracked.fence().equals(fence)) {
        outstanding.remove(unitId);
      }
    }
  }

  /**
   * Snapshot-and-clear: every still-outstanding grant, live fence included, in grant
   * order, and the ledger is emptied -- so a second drain is a no-op and nothing is ever
   * NACKed twice. The grants themselves come back rather than pre-built NACKs, because
   * what an unexplained lease means depends on what was granted: a cardinality probe
   * vanishing is expected, a measured invocation vanishing is parameter-set drift, and
   * anything else is an engine bug -- and only the caller can word those apart.
   */
  List<Grant> drainAll() {
    synchronized (outstanding) {
      List<Grant> drained = new ArrayList<>(outstanding.values());
      outstanding.clear();
      return drained;
    }
  }
}
