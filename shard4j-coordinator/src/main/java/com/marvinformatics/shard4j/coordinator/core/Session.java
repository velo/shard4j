package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * One live session: the census, the per-unit state machine, the shard roster and the
 * diagnostic side channels. All mutation happens under the coordinator's single write lock.
 */
@Accessors(fluent = true)
final class Session {

  // A malfunctioning shard can emit NACKs or stale results without bound; the lists are
  // diagnostic, so beyond the cap only the count survives -- enough to show the flood
  // happened without letting it eat the heap.
  private static final int DIAGNOSTIC_CAP = 100;

  @Getter private final String id;
  @Getter private final String testSetHash;
  private final Map<String, String> metadata;
  private final Map<String, UnitState> units = new LinkedHashMap<>();
  private final Map<Integer, ShardInfo> shards = new TreeMap<>();
  private final List<NackRequest.NackedLease> nacks = new ArrayList<>();
  private final List<ResultRequest> staleResults = new ArrayList<>();
  private int nacksDropped;
  private int staleResultsDropped;
  @Getter private int attempt;
  @Getter private long epoch;
  @Getter private Instant lastActivity;

  Session(
      String id,
      int attempt,
      long epoch,
      Map<String, String> metadata,
      String testSetHash,
      List<String> tests,
      Instant now) {
    this.id = id;
    this.attempt = attempt;
    this.epoch = epoch;
    this.metadata = Map.copyOf(metadata == null ? Map.of() : metadata);
    this.testSetHash = testSetHash;
    tests.forEach(testId -> units.put(testId, new UnitState()));
    this.lastActivity = now;
  }

  int registeredCount() {
    return units.size();
  }

  void touch(Instant now) {
    lastActivity = now;
  }

  boolean isRegistered(String testId) {
    return units.containsKey(testId);
  }

  void join(int shard, Instant now) {
    shards.computeIfAbsent(shard, index -> new ShardInfo()).departed = false;
    touch(now);
  }

  void depart(int shard) {
    shards.computeIfAbsent(shard, index -> new ShardInfo()).departed = true;
  }

  /**
   * A registration carrying a higher attempt: the previous attempt's shards are known-dead,
   * so every lease is invalidated immediately and everything not yet absorbed -- including
   * FAILED, which the new attempt must re-hand -- returns to PENDING.
   */
  void bumpEpoch(int newAttempt, long newEpoch) {
    this.attempt = newAttempt;
    this.epoch = newEpoch;
    for (UnitState unit : units.values()) {
      if (!unit.state.isAbsorbing()) {
        unit.state = TestState.PENDING;
        unit.failedIn = null;
        unit.lease = null;
      }
    }
  }

  boolean claimableIn(String testId, Pass pass) {
    UnitState unit = units.get(testId);
    return switch (pass) {
      case MAIN -> unit.state == TestState.PENDING;
      case RETRY1 -> unit.state == TestState.FAILED && unit.failedIn == Pass.MAIN;
      case RETRY2 -> unit.state == TestState.FAILED && unit.failedIn == Pass.RETRY1;
    };
  }

  void lease(
      String testId, int shard, Pass pass, Fence fence, Instant grantedAt, Instant expiresAt) {
    UnitState unit = units.get(testId);
    unit.lease = new Lease(shard, pass, fence, grantedAt, expiresAt, unit.state, unit.failedIn);
    unit.state = TestState.LEASED;
  }

  /**
   * Expiry is the backstop for a genuinely silent death; the unit returns to its pool. The
   * holder is treated as departed at the same moment -- a shard that stops reporting can
   * never announce departure itself, and without this the roster keeps a ghost alive and a
   * stranded session can never be diagnosed as INCOMPLETE. A holder that was merely slow
   * rejoins on its next claim.
   */
  void releaseExpiredLeases(Instant now) {
    for (UnitState unit : units.values()) {
      if (unit.state == TestState.LEASED && !unit.lease.expiresAt().isAfter(now)) {
        shards.computeIfAbsent(unit.lease.shard(), index -> new ShardInfo()).departed = true;
        restore(unit);
      }
    }
  }

  Fence currentFence(String testId) {
    Lease lease = currentLease(testId);
    return lease != null ? lease.fence() : null;
  }

  Lease currentLease(String testId) {
    UnitState unit = units.get(testId);
    return unit != null ? unit.lease : null;
  }

  void releaseLease(String testId) {
    restore(units.get(testId));
  }

  private void restore(UnitState unit) {
    unit.state = unit.lease.origin();
    unit.failedIn = unit.lease.originFailedIn();
    unit.lease = null;
  }

  void applyResult(
      int shard, Pass pass, String testId, Outcome outcome, long durationMs, String reason, Instant now) {
    UnitState unit = units.get(testId);
    unit.records.add(new SessionView.RecordView(pass, shard, outcome, durationMs, now));
    switch (outcome) {
      case PASSED -> unit.state = TestState.PASSED;
      case FAILED -> {
        unit.state = TestState.FAILED;
        unit.failedIn = pass;
      }
      case SKIPPED -> {
        unit.state = TestState.SKIPPED;
        unit.reason = reason;
      }
      case ABORTED -> {
        unit.state = TestState.ABORTED;
        unit.reason = reason;
      }
    }
    unit.lease = null;
    shards.computeIfAbsent(shard, index -> new ShardInfo()).completed++;
    touch(now);
  }

  void recordNack(NackRequest.NackedLease lease, Instant now) {
    if (nacks.size() < DIAGNOSTIC_CAP) {
      nacks.add(lease);
    } else {
      nacksDropped++;
    }
    touch(now);
  }

  void recordStale(ResultRequest request) {
    if (staleResults.size() < DIAGNOSTIC_CAP) {
      staleResults.add(request);
    } else {
      staleResultsDropped++;
    }
  }

  SessionView view() {
    List<SessionView.ShardView> shardViews =
        shards.entrySet().stream()
            .map(
                entry ->
                    new SessionView.ShardView(
                        entry.getKey(), entry.getValue().departed, entry.getValue().completed))
            .toList();
    List<SessionView.TestView> testViews =
        units.entrySet().stream()
            .map(
                entry ->
                    new SessionView.TestView(
                        entry.getKey(),
                        entry.getValue().state,
                        entry.getValue().reason,
                        leaseViewOf(entry.getValue()),
                        List.copyOf(entry.getValue().records)))
            .toList();
    return new SessionView(
        id,
        attempt,
        epoch,
        metadata,
        units.size(),
        testSetHash,
        shardViews,
        testViews,
        List.copyOf(nacks),
        List.copyOf(staleResults),
        nacksDropped,
        staleResultsDropped);
  }

  private static SessionView.LeaseView leaseViewOf(UnitState unit) {
    return unit.lease == null
        ? null
        : new SessionView.LeaseView(unit.lease.shard(), unit.lease.fence(), unit.lease.expiresAt());
  }

  private static final class UnitState {
    private TestState state = TestState.PENDING;
    private Pass failedIn;
    private Lease lease;
    private String reason;
    private final List<SessionView.RecordView> records = new ArrayList<>();
  }

  record Lease(
      int shard,
      Pass pass,
      Fence fence,
      Instant grantedAt,
      Instant expiresAt,
      TestState origin,
      Pass originFailedIn) {}

  private static final class ShardInfo {
    private boolean departed;
    private int completed;
  }
}
