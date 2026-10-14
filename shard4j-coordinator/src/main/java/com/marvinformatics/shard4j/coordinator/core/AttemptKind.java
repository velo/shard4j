package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.CensusUnit;

/**
 * What one recorded attempt row stands for: a leaf, a template leased whole, or one of its rows.
 */
public enum AttemptKind {
  UNIT,
  TEMPLATE,
  INVOCATION;

  public static AttemptKind ofLease(String leaseId) {
    CensusUnit unit = CensusUnit.parse(leaseId);
    return unit.template() && unit.invocation() == null ? TEMPLATE : UNIT;
  }
}
