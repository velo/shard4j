package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.Outcome;
import java.util.List;

/**
 * One lease unit's terminal outcome, as the engine measured it. For a template unit the
 * outcome is the aggregate over its invocations -- computed here, on the shard, because
 * the coordinator never learns a template's invocation count in advance -- and the
 * duration is the whole method's elapsed time, which is what the scheduler hands out.
 *
 * @param invocations non-null only for a template unit, one record per materialised
 *     invocation with its own duration
 */
record UnitResult(
    ExecutionId unitId,
    Outcome outcome,
    long durationMs,
    String reason,
    List<InvocationRecord> invocations) {}
