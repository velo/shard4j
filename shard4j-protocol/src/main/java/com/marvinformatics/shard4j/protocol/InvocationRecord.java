package com.marvinformatics.shard4j.protocol;

/**
 * One invocation of a template unit, recorded individually under its own shape-C id.
 * Written to history for humans; never an ordering input.
 */
public record InvocationRecord(String testId, Outcome outcome, long durationMs, String reason) {}
