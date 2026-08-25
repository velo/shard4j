package com.marvinformatics.shard4j.protocol;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * The read-only observability surface and the verdict's entire input.
 *
 * <p>It returns ids, states, durations, outcomes, reasons and shard labels. Never source
 * paths, never logs, never stack traces, and never the shared secret.
 *
 * <p>{@code nacks} and {@code staleResults} are capped diagnostic channels; the two dropped
 * counters say how many entries a flood pushed past the cap, so the signal that it happened
 * survives even when the entries do not.
 */
public record SessionView(
    String session,
    int attempt,
    long epoch,
    Map<String, String> metadata,
    int registeredCount,
    String registeredHash,
    List<ShardView> shards,
    List<TestView> tests,
    List<NackRequest.NackedLease> nacks,
    List<ResultRequest> staleResults,
    int nacksDropped,
    int staleResultsDropped) {

  public record ShardView(int shard, boolean departed, int completed) {}

  /** {@code lease} is present exactly while the unit is LEASED, and null otherwise. */
  public record TestView(
      String testId, TestState state, String reason, LeaseView lease, List<RecordView> records) {}

  /** Who holds a live lease, under which fence, and until when -- the stranded-lease detail. */
  public record LeaseView(int shard, Fence fence, Instant expiresAt) {}

  public record RecordView(Pass pass, int shard, Outcome outcome, long durationMs, Instant timestamp) {}
}
