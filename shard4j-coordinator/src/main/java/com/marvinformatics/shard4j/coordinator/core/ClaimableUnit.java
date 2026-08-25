package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.CensusUnit;

/**
 * One claimable unit as census expansion produced it: the parsed id, plus whether it is a
 * cardinality probe -- an invocation one position past the method's recorded parameter
 * count, handed out to discover growth rather than to run measured work.
 */
record ClaimableUnit(CensusUnit unit, boolean probe) {}
