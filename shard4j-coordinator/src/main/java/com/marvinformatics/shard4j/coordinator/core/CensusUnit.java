package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.HistoryKey;

/**
 * A census unit as the coordinator holds it: the wire id, plus the two facts derived from
 * it once, when the census is registered.
 *
 * <p>The protocol calls an execution id opaque, and it stays that way -- the single
 * derivation lives in {@link HistoryKeys}, and everything downstream reads these fields
 * instead of parsing the id again. That is what lets the scheduler both rank the pool and
 * group it by class without a second grammar to keep in step with the first.
 */
public record CensusUnit(String id, String className, HistoryKey historyKey) {}
