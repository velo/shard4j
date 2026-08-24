package com.marvinformatics.shard4j.protocol;

/**
 * The answer to a result write. A rejected write is a stale fence: the lease was reclaimed,
 * the epoch was bumped, or an older incarnation wrote. It carries the fence that beat it so
 * the shard's log says why, and it is non-fatal to the shard -- per-job exit codes are not
 * the gate.
 *
 * @param currentFence null when the write was accepted
 */
public record ResultResponse(boolean accepted, Fence currentFence) {}
