package com.marvinformatics.shard4j.protocol;

import java.time.Instant;

/**
 * One leased unit. {@code probe} marks a cardinality probe: an invocation id one position
 * past a template's recorded parameter count, handed out to discover growth. A probe that
 * does not materialise is expected and is returned quietly; a non-probe invocation that
 * does not materialise means the parameter set shrank since it was measured, which is a
 * loud failure.
 */
public record Grant(String testId, Fence fence, Instant expiresAt, boolean probe) {}
