package com.marvinformatics.shard4j.protocol;

import java.time.Instant;

/**
 * One leased unit. {@code probe} marks a cardinality probe -- a position past everything
 * the coordinator has recorded for a template -- for which not materialising is the
 * expected answer and reported as a vanished NACK rather than a failure; a whole unit that
 * does not materialise means the parameter set shrank since it was measured, which is a
 * loud failure.
 *
 * <p>{@code retryable} is the coordinator's promise that a failure of <em>this</em> attempt
 * would be requeued rather than made terminal. The engine uses it to decide what to tell
 * the <em>launcher</em> -- an aborted leaf keeps failsafe green while the retry is still
 * owed -- and nothing else. What the coordinator is told is always the real outcome:
 * downgrading toward the coordinator would turn a genuine failure into passing coverage,
 * because the verdict counts ABORTED as terminal-OK. The attempt budget itself is the
 * coordinator's, and never travels: {@code SessionView.RecordView.attempt} is where the
 * ordinal is observable.
 */
public record Grant(
    String testId, Fence fence, Instant expiresAt, boolean probe, boolean retryable) {}
