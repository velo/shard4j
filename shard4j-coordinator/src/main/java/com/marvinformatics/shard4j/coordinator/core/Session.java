package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.CensusUnit;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

/**
 * One live session: the census, the per-unit state machine, the shard roster and the
 * diagnostic side channels. All mutation happens under the coordinator's single write lock.
 *
 * <p>The census lives here at two granularities on purpose. What a shard registers -- and
 * what registration equality is judged over -- is the method-level enumeration, because
 * invocations do not exist at the shard's discovery time. What the scheduler hands out is
 * the expanded unit set: a template method with a trusted invocation plan becomes one
 * claimable unit per recorded position (plus one cardinality probe past the last), and
 * everything else stays a single whole unit. Each expanded unit runs the full per-unit
 * state machine independently, so a failed invocation retries alone and the coverage
 * verdict counts every position individually.
 */
@Accessors(fluent = true)
final class Session {

  // A malfunctioning shard can emit NACKs or stale results without bound; the lists are
  // diagnostic, so beyond the cap only the count survives -- enough to show the flood
  // happened without letting it eat the heap.
  private static final int DIAGNOSTIC_CAP = 100;

  // The poll cadence the design fixes for a shard waiting at a barrier.
  private static final int RETRY_AFTER_SECONDS = 5;

  // The cadence is mandated, so silence is measurable: a shard holding no lease that has
  // missed three consecutive polls is presumed dead. Generous against jitter, and cheap to
  // be wrong about -- a merely slow or partitioned shard rejoins on its next call.
  private static final Duration PRESUMED_DEAD_AFTER =
      Duration.ofSeconds(3L * RETRY_AFTER_SECONDS);

  // How long after session creation a declared-but-unseen shard still reserves its fair
  // share of a template's invocations. Bounded so a shard that never boots costs at most
  // this much held-back spreading, not a stranded pass: once the window closes, whoever
  // is still asking takes the remainder.
  private static final Duration FLEET_ARRIVAL_WINDOW = Duration.ofSeconds(60);

  @Getter private final String id;
  private final Map<String, String> metadata;
  private final Set<String> registered = new LinkedHashSet<>();
  private final Map<String, List<String>> unitsByCensusId = new LinkedHashMap<>();
  private final Map<String, UnitState> units = new LinkedHashMap<>();
  private final Map<Integer, ShardInfo> shards = new TreeMap<>();
  private final List<NackRequest.NackedLease> nacks = new ArrayList<>();
  private final List<ResultRequest> staleResults = new ArrayList<>();
  private final Instant createdAt;
  private int nacksDropped;
  private int staleResultsDropped;
  private int declaredShardCount;
  @Getter private int attempt;
  @Getter private long epoch;
  @Getter private Instant lastActivity;

  Session(
      String id,
      int attempt,
      long epoch,
      Map<String, String> metadata,
      List<String> censusIds,
      Map<String, List<ClaimableUnit>> expansion,
      Instant now) {
    this.id = id;
    this.attempt = attempt;
    this.epoch = epoch;
    this.metadata = Map.copyOf(metadata == null ? Map.of() : metadata);
    for (String censusId : censusIds) {
      registered.add(censusId);
      List<String> unitIds = new ArrayList<>();
      for (ClaimableUnit expanded : expansion.get(censusId)) {
        unitIds.add(expanded.unit().id());
        units.put(expanded.unit().id(), new UnitState(censusId, expanded.unit(), expanded.probe()));
      }
      unitsByCensusId.put(censusId, unitIds);
    }
    this.createdAt = now;
    this.lastActivity = now;
  }

  /** Claimable units, expansion included -- the verdict's denominator. */
  int registeredCount() {
    return units.size();
  }

  /** Registered method-level census entries -- stable across history, unlike expansion. */
  int censusSize() {
    return registered.size();
  }

  /** The registration contract: method-level ids, the granularity census equality is judged at. */
  Set<String> censusIds() {
    return Set.copyOf(registered);
  }

  void touch(Instant now) {
    lastActivity = now;
  }

  boolean inCensus(String censusId) {
    return registered.contains(censusId);
  }

  boolean isRegistered(String unitId) {
    return units.containsKey(unitId);
  }

  boolean isProbe(String unitId) {
    UnitState unit = units.get(unitId);
    return unit != null && unit.probe;
  }

  String censusIdOf(String unitId) {
    return units.get(unitId).censusId;
  }

  /** The consumer-declared fleet size, kept as the maximum any registration reported. */
  void declareFleet(Integer shardCount) {
    if (shardCount != null && shardCount > declaredShardCount) {
      declaredShardCount = shardCount;
    }
  }

  void join(int shard, Instant now) {
    ShardInfo info = shards.computeIfAbsent(shard, index -> new ShardInfo());
    info.departed = false;
    info.explicitlyDeparted = false;
    info.lastSeenAt = now;
    touch(now);
  }

  /** The shard's own goodbye -- unlike a presumed death, a barrier packet cannot undo it. */
  void depart(int shard) {
    ShardInfo info = shards.computeIfAbsent(shard, index -> new ShardInfo());
    info.departed = true;
    info.explicitlyDeparted = true;
  }

  boolean hasJoined(int shard) {
    ShardInfo info = shards.get(shard);
    return info != null && !info.departed;
  }

  boolean hasDeparted(int shard) {
    ShardInfo info = shards.get(shard);
    return info != null && info.departed;
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
    // The previous attempt's shards are known-dead and its barriers never resolve; the new
    // attempt's shards re-join with a clean watermark and no early release.
    for (ShardInfo info : shards.values()) {
      info.departed = true;
      info.explicitlyDeparted = false;
      info.completedPass = null;
      info.released = false;
      info.exhaustedIn = null;
    }
  }

  private static boolean claimableIn(UnitState unit, Pass pass) {
    return switch (pass) {
      case MAIN -> unit.state == TestState.PENDING;
      case RETRY1 -> unit.state == TestState.FAILED && unit.failedIn == Pass.MAIN;
      case RETRY2 -> unit.state == TestState.FAILED && unit.failedIn == Pass.RETRY1;
    };
  }

  /** Every claimable unit the given pass could grant right now, in registration order. */
  List<CensusUnit> claimable(Pass pass) {
    return units.values().stream()
        .filter(unit -> claimableIn(unit, pass))
        .map(unit -> unit.unit)
        .toList();
  }

  /** The parsed form of a unit already known to be held by this session. */
  CensusUnit unitOf(String unitId) {
    return units.get(unitId).unit;
  }

  /** The claimable units behind one method-level candidate: itself, or its expansion. */
  List<CensusUnit> claimableUnitsOf(String censusId, Pass pass) {
    return unitsByCensusId.getOrDefault(censusId, List.of()).stream()
        .map(units::get)
        .filter(unit -> claimableIn(unit, pass))
        .map(unit -> unit.unit)
        .toList();
  }

  /**
   * A probe passed, so the parameter set grew past everything recorded: the next position
   * becomes a probe in turn, and the hand-out walks the growth one position at a time
   * within the same session. Added only once, and only while the method still exists in
   * this census.
   */
  void addProbe(String censusId, CensusUnit unit) {
    if (units.containsKey(unit.id()) || !registered.contains(censusId)) {
      return;
    }
    units.put(unit.id(), new UnitState(censusId, unit, true));
    unitsByCensusId.get(censusId).add(unit.id());
  }

  /**
   * A vanished probe: the shard proved the position does not exist, so the unit leaves the
   * census entirely -- it was never a test, and leaving it PENDING would turn every clean
   * run into an INCOMPLETE verdict.
   */
  void removeVanishedProbe(String unitId) {
    UnitState unit = units.remove(unitId);
    if (unit != null) {
      unitsByCensusId.get(unit.censusId).remove(unitId);
    }
  }

  /** True when every measured (non-probe) unit of the method absorbed without failing. */
  boolean measuredUnitsAllNonFailing(String censusId) {
    return unitsByCensusId.getOrDefault(censusId, List.of()).stream()
        .map(units::get)
        .filter(unit -> !unit.probe)
        .allMatch(unit -> unit.state == TestState.PASSED || unit.state == TestState.SKIPPED);
  }

  /** The open ask came back empty for this shard: it will not ask again in this pass. */
  void markExhausted(int shard, Pass pass) {
    shards.computeIfAbsent(shard, index -> new ShardInfo()).exhaustedIn = pass;
  }

  /**
   * How many more of the method's invocations this shard may lease right now. The cap is
   * a fair share -- ceil of the pass's eligible invocations over the expected fleet --
   * and it only binds while some other shard may still ask: another live shard is still
   * working the pass, or a declared shard has not arrived and the arrival window is open.
   * The last shard still asking is never capped, which is what makes the hold-back safe:
   * spreading degrades to today's behaviour rather than stranding a unit.
   */
  int invocationAllowance(String censusId, int shard, Pass pass, Instant now) {
    if (!othersMayStillClaim(shard, pass, now)) {
      return Integer.MAX_VALUE;
    }
    int eligible = 0;
    int mine = 0;
    for (String unitId : unitsByCensusId.getOrDefault(censusId, List.of())) {
      UnitState unit = units.get(unitId);
      if (unit.state == TestState.LEASED) {
        eligible++;
        if (unit.lease.shard() == shard) {
          mine++;
        }
        continue;
      }
      if (claimableIn(unit, pass)) {
        eligible++;
        continue;
      }
      SessionView.RecordView latest =
          unit.records.isEmpty() ? null : unit.records.get(unit.records.size() - 1);
      if (latest != null && latest.pass() == pass) {
        eligible++;
        if (latest.shard() == shard) {
          mine++;
        }
      }
    }
    int share = Math.ceilDiv(eligible, expectedFleet(pass, now));
    return Math.max(0, share - mine);
  }

  private int expectedFleet(Pass pass, Instant now) {
    int active =
        (int) shards.values().stream().filter(info -> !info.departed && !info.released).count();
    int fleet = Math.max(1, active);
    if (pass == Pass.MAIN && withinArrivalWindow(now)) {
      fleet = Math.max(fleet, declaredShardCount);
    }
    return fleet;
  }

  private boolean othersMayStillClaim(int shard, Pass pass, Instant now) {
    boolean otherStillWorking =
        shards.entrySet().stream()
            .anyMatch(
                entry ->
                    entry.getKey() != shard
                        && !entry.getValue().departed
                        && !entry.getValue().released
                        && entry.getValue().exhaustedIn != pass
                        && (entry.getValue().completedPass == null
                            || entry.getValue().completedPass.ordinal() < pass.ordinal()));
    if (otherStillWorking) {
      return true;
    }
    return pass == Pass.MAIN && withinArrivalWindow(now) && declaredShardCount > shards.size();
  }

  private boolean withinArrivalWindow(Instant now) {
    return now.isBefore(createdAt.plus(FLEET_ARRIVAL_WINDOW));
  }

  Pass completedPassOf(int shard) {
    ShardInfo info = shards.get(shard);
    return info == null ? null : info.completedPass;
  }

  /**
   * Arrival at a barrier is proof of life, so it reverses a presumed death -- and that
   * revival is load-bearing: were a fleet's leases all to expire at once, the pools would
   * be resolved against zero live shards and every still-working shard would be released
   * with units pending. An explicit departure is different: the shard said goodbye, so the
   * only arrival that can follow is a delayed or duplicated packet, and reviving on that
   * would resurrect a shard that will never poll again into every future quorum.
   */
  void completePass(int shard, Pass pass, Instant now) {
    ShardInfo info = shards.computeIfAbsent(shard, index -> new ShardInfo());
    if (!info.explicitlyDeparted) {
      info.departed = false;
      info.lastSeenAt = now;
    }
    if (info.completedPass == null || info.completedPass.ordinal() < pass.ordinal()) {
      info.completedPass = pass;
    }
    touch(now);
  }

  boolean isReleased(int shard) {
    ShardInfo info = shards.get(shard);
    return info != null && info.released;
  }

  void release(int shard) {
    shards.computeIfAbsent(shard, index -> new ShardInfo()).released = true;
  }

  /**
   * The barrier answer for a shard that finished {@code completedPass}. Three counts drive
   * it: units already failed into the asker's next pool, units that may yet land there
   * (leased, or claimable in a pass some live shard has not finished), and shards waiting
   * at this same barrier. The next pass runs only once every live undeparted shard has
   * finished the current one -- the first finisher must not drain an empty failure pool and
   * leave the straggler retrying its own failures alone -- and a shard is released the
   * moment it cannot be needed, so a barrier never costs the fleet the slowest shard's
   * wall time for nothing. Pure decision: the caller persists and applies a release.
   */
  BarrierResponse barrierDecision(int shard, Pass completedPass) {
    if (isReleased(shard)) {
      return done();
    }
    Pass nextPass = nextOf(completedPass);
    if (nextPass == null) {
      return done();
    }
    int retryPool = 0;
    int mayStillFail = 0;
    Instant earliestLeaseExpiry = null;
    for (UnitState unit : units.values()) {
      if (unit.state == TestState.LEASED) {
        mayStillFail++;
        Instant expiresAt = unit.lease.expiresAt();
        if (earliestLeaseExpiry == null || expiresAt.isBefore(earliestLeaseExpiry)) {
          earliestLeaseExpiry = expiresAt;
        }
        continue;
      }
      Pass pool = claimablePoolOf(unit);
      if (pool == nextPass) {
        retryPool++;
      } else if (pool != null && someLiveShardStillReaches(pool)) {
        mayStillFail++;
      }
    }
    int waiting =
        (int)
            shards.values().stream()
                .filter(info -> !info.departed && !info.released && info.completedPass == completedPass)
                .count();
    if (retryPool + mayStillFail == 0 || waiting > retryPool + mayStillFail) {
      return done();
    }
    // The same liveness filter as the waiter count and someLiveShardStillReaches, on
    // purpose: a released shard was told to stop pulling, can claim nothing and holds no
    // lease, so no signal it could ever emit -- not even lease expiry -- would advance its
    // watermark. Counting it here would park the fleet behind a shard that cannot move.
    boolean quorumMet =
        shards.values().stream()
            .filter(info -> !info.departed && !info.released)
            .allMatch(
                info ->
                    info.completedPass != null
                        && info.completedPass.ordinal() >= completedPass.ordinal());
    if (quorumMet && retryPool > 0) {
      return new BarrierResponse(BarrierResponse.Action.RUN, null, null);
    }
    return new BarrierResponse(BarrierResponse.Action.WAIT, RETRY_AFTER_SECONDS, earliestLeaseExpiry);
  }

  /** The pass that could still claim this unit, or null when no pool will ever hold it. */
  private static Pass claimablePoolOf(UnitState unit) {
    if (unit.state == TestState.PENDING) {
      return Pass.MAIN;
    }
    if (unit.state == TestState.FAILED) {
      return nextOf(unit.failedIn);
    }
    return null;
  }

  /**
   * A unit claimable in {@code pool} may yet be run -- and may yet fail -- only while some
   * live, unreleased shard has not moved past that pass. Once every live shard is beyond
   * it, the unit is stranded: it can never enter a retry pool and must not hold a barrier.
   */
  private boolean someLiveShardStillReaches(Pass pool) {
    return shards.values().stream()
        .filter(info -> !info.departed && !info.released)
        .anyMatch(
            info -> {
              Pass current = info.completedPass == null ? Pass.MAIN : nextOf(info.completedPass);
              return current != null && current.ordinal() <= pool.ordinal();
            });
  }

  private static Pass nextOf(Pass pass) {
    return switch (pass) {
      case MAIN -> Pass.RETRY1;
      case RETRY1 -> Pass.RETRY2;
      case RETRY2 -> null;
    };
  }

  private static BarrierResponse done() {
    return new BarrierResponse(BarrierResponse.Action.DONE, null, null);
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
  List<Integer> releaseExpiredLeases(Instant now) {
    List<Integer> newlyDeparted = new ArrayList<>();
    for (UnitState unit : units.values()) {
      if (unit.state == TestState.LEASED && !unit.lease.expiresAt().isAfter(now)) {
        ShardInfo info = shards.computeIfAbsent(unit.lease.shard(), index -> new ShardInfo());
        if (!info.departed) {
          info.departed = true;
          newlyDeparted.add(unit.lease.shard());
        }
        restore(unit);
      }
    }
    return newlyDeparted;
  }

  /**
   * The waiter-side twin of lease expiry: a shard waiting at a barrier holds no lease, so
   * expiry can never notice its death, yet its stale watermark would keep counting in the
   * waiter tally and the quorum forever. The mandated poll cadence makes its silence
   * measurable instead -- no lease held and nothing heard for {@link #PRESUMED_DEAD_AFTER}
   * means presumed dead, and it departs exactly as an expired holder does. Released shards
   * are exempt: DONE told them to stop polling, so silence is their normal state, and they
   * already count in no quorum and no tally.
   */
  List<Integer> departSilentShards(Instant now) {
    Set<Integer> leaseHolders = new HashSet<>();
    for (UnitState unit : units.values()) {
      if (unit.state == TestState.LEASED) {
        leaseHolders.add(unit.lease.shard());
      }
    }
    List<Integer> newlyDeparted = new ArrayList<>();
    for (Map.Entry<Integer, ShardInfo> entry : shards.entrySet()) {
      ShardInfo info = entry.getValue();
      boolean silentTooLong =
          info.lastSeenAt == null || !info.lastSeenAt.plus(PRESUMED_DEAD_AFTER).isAfter(now);
      if (!info.departed
          && !info.released
          && silentTooLong
          && !leaseHolders.contains(entry.getKey())) {
        info.departed = true;
        newlyDeparted.add(entry.getKey());
      }
    }
    return newlyDeparted;
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
    ShardInfo info = shards.computeIfAbsent(shard, index -> new ShardInfo());
    info.completed++;
    info.lastSeenAt = now;
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
                        entry.getKey(),
                        entry.getValue().departed,
                        entry.getValue().completed,
                        entry.getValue().completedPass,
                        entry.getValue().released))
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

  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  private static final class UnitState {
    private final String censusId;
    private final CensusUnit unit;
    private final boolean probe;
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
    private boolean explicitlyDeparted;
    private int completed;
    private Pass completedPass;
    private boolean released;
    private Instant lastSeenAt;
    private Pass exhaustedIn;
  }
}
