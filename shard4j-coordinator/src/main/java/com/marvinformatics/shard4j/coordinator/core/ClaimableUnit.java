package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.CensusUnit;
import com.marvinformatics.shard4j.protocol.HistoryKey;

/**
 * One claimable unit as the scheduler passes it around: the parsed id, the method-level
 * census entry it expanded from, and whether it is a cardinality probe -- an invocation
 * one position past the method's recorded parameter count, handed out to discover growth
 * rather than to run measured work.
 *
 * <p>This is the currency of census expansion end to end: produced by expansion, held by
 * the session's per-unit state, ranked by {@link ClaimOrdering} and granted by the
 * scheduler -- so none of them re-asks the session what it already carries.
 */
public record ClaimableUnit(String censusId, CensusUnit unit, boolean probe) {

  public String id() {
    return unit.id();
  }

  public String className() {
    return unit.className();
  }

  public HistoryKey historyKey() {
    return unit.historyKey();
  }

  public Integer invocation() {
    return unit.invocation();
  }
}
