package com.marvinformatics.shard4j.protocol;

import java.util.List;

/**
 * One call per lease unit as it completes -- never batched at shard exit, never one call
 * per invocation.
 *
 * <p>For a template unit the outcome is the shard's aggregate over its invocations: the
 * shard computes it, because the coordinator never learns a template's invocation count in
 * advance and so can never tell "all invocations reported" from "some are still coming".
 * {@code durationMs} is the whole unit's elapsed time and is the only figure the ordering
 * aggregate consumes.
 *
 * @param reason required and non-empty for SKIPPED and ABORTED
 * @param invocations present only for a template unit
 */
public record ResultRequest(
    int shard,
    String testId,
    Fence fence,
    Outcome outcome,
    long durationMs,
    boolean firstOnShard,
    String reason,
    List<InvocationRecord> invocations) {}
