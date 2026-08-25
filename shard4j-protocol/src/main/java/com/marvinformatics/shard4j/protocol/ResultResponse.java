package com.marvinformatics.shard4j.protocol;

/**
 * The answer to a result write. A rejected write is a stale fence: the lease was reclaimed,
 * the epoch was bumped, or an older incarnation wrote. It carries the fence that beat it so
 * the shard's log says why, and it is non-fatal to the shard -- per-job exit codes are not
 * the gate.
 *
 * @param currentFence null when the write was accepted -- and also null on a rejection when
 *     no lease is outstanding at all, because the unit already reached a terminal state or
 *     returned to the pool: there is no competing holder to name, only the absence of one
 */
public record ResultResponse(boolean accepted, Fence currentFence) {}
