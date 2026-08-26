package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.Outcome;
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

  @Getter private final String id;
  private final Map<String, String> metadata;
  private final Set<String> registered = new LinkedHashSet<>();
  private final Map<String, List<String>> unitsByCensusId = new LinkedHashMap<>();
  private final Map<String, UnitState> units = new LinkedHashMap<>();
  private final Map<Integer, ShardInfo> shards = new TreeMap<>();
  private final List<NackRequest.NackedLease> nacks = new ArrayList<>();
  private final List<ResultRequest> staleResults = new ArrayList<>();
  private final Instant createdAt;
  private final FairShare fairShare;
  private int nacksDropped;
  private int staleResultsDropped;
  @Getter private int attempt;
  private final int maxAttempts;
  @Getter private long epoch;
  @Getter private Instant lastActivity;

  /** {@code expansion} carries the census order and content in one structure: each
   * method-level census id, in registration order, to its expanded claimable units. */
  Session(
      String id,
      int attempt,
      long epoch,
      int maxAttempts,
      Map<String, String> metadata,
      Map<String, List<ClaimableUnit>> expansion,
      Instant now) {
    this.id = id;
    this.attempt = attempt;
    this.maxAttempts = maxAttempts;
    this.epoch = epoch;
    this.metadata = Map.copyOf(metadata == null ? Map.of() : metadata);
    expansion.forEach(
        (censusId, expanded) -> {
          registered.add(censusId);
          List<String> unitIds = new ArrayList<>();
          for (ClaimableUnit unit : expanded) {
            unitIds.add(unit.id());
            units.put(unit.id(), new UnitState(unit));
          }
          unitsByCensusId.put(censusId, unitIds);
        });
    this.createdAt = now;
    this.fairShare = new FairShare(shards, now);
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

  /** The consumer-declared fleet size; the fair-share policy is its only reader. */
  void declareFleet(Integer shardCount) {
    fairShare.declareFleet(shardCount);
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
        unit.attempts = 0;
        unit.lease = null;
      }
    }
    // The previous attempt's shards are known-dead and its barriers never resolve; the new
    // attempt's shards re-join with a clean watermark and no early release.
    for (ShardInfo info : shards.values()) {
      info.departed = true;
      info.explicitlyDeparted = false;
      info.idleSince = null;
      info.released = false;
    }
    fairShare.epochBumped();
  }

  /**
   * One condition, because a failure that still has budget is put straight back to PENDING
   * rather than parked in a pass-specific pool. There is no second question to ask.
   */
  static boolean isClaimable(UnitState unit) {
    return unit.state == TestState.PENDING;
  }

  /** Every unit claimable right now, in registration order. */
  List<ClaimableUnit> claimable() {
    return units.values().stream()
        .filter(Session::isClaimable)
        .map(unit -> unit.unit)
        .toList();
  }

  /** The expanded form of a unit already known to be held by this session. */
  ClaimableUnit unitOf(String unitId) {
    return units.get(unitId).unit;
  }

  /** The claimable units behind one method-level candidate: itself, or its expansion. */
  List<ClaimableUnit> claimableUnitsOf(String censusId) {
    return unitsByCensusId.getOrDefault(censusId, List.of()).stream()
        .map(units::get)
        .filter(Session::isClaimable)
        .map(unit -> unit.unit)
        .toList();
  }

  /**
   * A probe materialised, so the parameter set grew past everything recorded: the next
   * position becomes a probe in turn, and the hand-out walks the growth one position at a
   * time within the same session. Added only once, and only while the method still exists
   * in this census; returns whether this call was the one that added it.
   */
  boolean addProbe(ClaimableUnit unit) {
    if (units.containsKey(unit.id()) || !registered.contains(unit.censusId())) {
      return false;
    }
    units.put(unit.id(), new UnitState(unit));
    unitsByCensusId.get(unit.censusId()).add(unit.id());
    return true;
  }

  /**
   * A vanished probe: the shard proved the position does not exist, so the unit leaves the
   * census entirely -- it was never a test, and leaving it PENDING would turn every clean
   * run into an INCOMPLETE verdict.
   */
  void removeVanishedProbe(String unitId) {
    UnitState unit = units.remove(unitId);
    if (unit != null) {
      unitsByCensusId.get(unit.unit.censusId()).remove(unitId);
    }
  }

  /** True when every measured (non-probe) unit of the method absorbed without failing. */
  boolean measuredUnitsAllNonFailing(String censusId) {
    return unitsByCensusId.getOrDefault(censusId, List.of()).stream()
        .map(units::get)
        .filter(unit -> !unit.unit.probe())
        .allMatch(unit -> unit.state == TestState.PASSED || unit.state == TestState.SKIPPED);
  }

  /** The open ask came back empty for this shard: it has stopped pulling. */
  void markExhausted(int shard) {
    fairShare.markExhausted(shard);
  }

  /** How many more of the method's invocations this shard may lease: {@link FairShare}. */
  int invocationAllowance(String censusId, int shard, Instant now) {
    List<UnitState> unitsOfMethod =
        unitsByCensusId.getOrDefault(censusId, List.of()).stream().map(units::get).toList();
    return fairShare.invocationAllowance(unitsOfMethod, shard, now);
  }

  boolean isIdle(int shard) {
    ShardInfo info = shards.get(shard);
    return info != null && info.idleSince != null;
  }

  /**
   * Arrival at a barrier is proof of life, so it reverses a presumed death -- and that
   * revival is load-bearing: were a fleet's leases all to expire at once, the pools would
   * be resolved against zero live shards and every still-working shard would be released
   * with units pending. An explicit departure is different: the shard said goodbye, so the
   * only arrival that can follow is a delayed or duplicated packet, and reviving on that
   * would resurrect a shard that will never poll again into every future quorum.
   */
  void markIdle(int shard, Instant now) {
    ShardInfo info = shards.computeIfAbsent(shard, index -> new ShardInfo());
    if (!info.explicitlyDeparted) {
      info.departed = false;
      info.lastSeenAt = now;
    }
    // First arrival wins: the clock measures how long this shard has been starved, and
    // restarting it on every poll would make a patient shard look freshly idle forever.
    if (info.idleSince == null) {
      info.idleSince = now;
    }
    touch(now);
  }

  /** Work still in play: a unit claimable now, or one leased that could yet requeue. */
  boolean hasOutstandingWork() {
    return units.values().stream()
        .anyMatch(unit -> unit.state == TestState.PENDING || unit.state == TestState.LEASED);
  }

  boolean isReleased(int shard) {
    ShardInfo info = shards.get(shard);
    return info != null && info.released;
  }

  void release(int shard) {
    shards.computeIfAbsent(shard, index -> new ShardInfo()).released = true;
  }

  /**
   * The barrier answer for a shard that has found nothing to claim, decided by where the
   * shard stands in the hunger queue against how much work exists or could yet exist.
   *
   * <p>Two counts bound how many shards are worth keeping: units claimable right now, and
   * leases still outstanding. A shard ranked inside the first is told to RUN; one ranked
   * inside the sum is held as spare capacity; anything past that is surplus and released.
   *
   * <p>That reconciles two rules that look opposed. A finished shard should not cost the
   * fleet the slowest shard's wall time -- but with two shards and one test, releasing the
   * idle one strands the next attempt on the shard that just failed it, because there is
   * nowhere else for it to go. Sizing the hold to the work keeps a spare for every lease
   * that might return, and no more.
   *
   * <p><strong>Every</strong> outstanding lease counts, including one on its final attempt.
   * It is tempting to exclude those on the grounds that a final attempt cannot requeue and
   * so can create no work -- but a lease has three exits, not two. Besides PASSED and
   * FAILED it can be <em>restored</em>: expiry and NACK both return the unit to PENDING
   * with the attempt un-spent. Excluding final-attempt leases releases the last spare
   * exactly when the holder is about to die, which strands the unit with nobody left to
   * poll and loses the run -- the failure mode this barrier exists to prevent. Holding
   * costs at most one test's duration; the alternative costs the whole suite.
   */
  BarrierResponse barrierDecision(int shard, Instant now) {
    if (isReleased(shard)) {
      return done();
    }
    int claimable = 0;
    int outstanding = 0;
    Instant earliestLeaseExpiry = null;
    for (UnitState unit : units.values()) {
      if (unit.state == TestState.PENDING) {
        claimable++;
      } else if (unit.state == TestState.LEASED) {
        outstanding++;
        Instant expiresAt = unit.lease.expiresAt();
        if (earliestLeaseExpiry == null || expiresAt.isBefore(earliestLeaseExpiry)) {
          earliestLeaseExpiry = expiresAt;
        }
      }
    }
    if (claimable + outstanding == 0) {
      return done();
    }
    int rank = hungerRank(shard, now);
    if (rank < claimable) {
      return new BarrierResponse(BarrierResponse.Action.RUN, null, null);
    }
    if (rank < claimable + outstanding) {
      return new BarrierResponse(
          BarrierResponse.Action.WAIT, RETRY_AFTER_SECONDS, earliestLeaseExpiry);
    }
    return done();
  }

  /**
   * How many live shards have been starved longer than this one. Polling is pull-based, so
   * without an explicit order a requeued unit goes to whoever happens to ask next -- which
   * on a fleet of equal pollers is arbitrary, and reliably starves a shard that has been
   * waiting since long before the others arrived. Ties break on shard index so the order is
   * total and no two shards ever read the same rank.
   */
  private int hungerRank(int shard, Instant now) {
    Instant mine =
        shards.containsKey(shard) && shards.get(shard).idleSince != null
            ? shards.get(shard).idleSince
            : now;
    return (int)
        shards.entrySet().stream()
            .filter(entry -> entry.getKey() != shard)
            .filter(entry -> !entry.getValue().departed && !entry.getValue().released)
            .filter(entry -> entry.getValue().idleSince != null)
            .filter(
                entry -> {
                  int byTime = entry.getValue().idleSince.compareTo(mine);
                  return byTime < 0 || (byTime == 0 && entry.getKey() < shard);
                })
            .count();
  }

  private static BarrierResponse done() {
    return new BarrierResponse(BarrierResponse.Action.DONE, null, null);
  }

  void lease(
      String testId, int shard, Fence fence, Instant grantedAt, Instant expiresAt) {
    UnitState unit = units.get(testId);
    unit.lease = new Lease(shard, fence, grantedAt, expiresAt, unit.state, unit.attempts);
    unit.state = TestState.LEASED;
    // Taking work ends the wait. Without this the flag set at the barrier never cleared,
    // so a shard that went back to work still counted as starved and the fleet released
    // shards that were needed.
    ShardInfo holder = shards.computeIfAbsent(shard, index -> new ShardInfo());
    holder.idleSince = null;
    fairShare.resumed(shard);
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

  /** Attempts still available to this unit, the one about to run included. */
  int attemptsRemaining(String testId) {
    return Math.max(0, maxAttempts - attemptsOf(testId));
  }

  /** Attempts already spent on this unit; the one being recorded now is this plus one. */
  int attemptsOf(String testId) {
    UnitState unit = units.get(testId);
    return unit == null ? 0 : unit.attempts;
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
    unit.attempts = unit.lease.originAttempts();
    unit.lease = null;
  }

  void applyResult(
      int shard,
      String testId,
      int attempt,
      Outcome outcome,
      long durationMs,
      String reason,
      Instant now) {
    UnitState unit = units.get(testId);
    // The attempt is handed in rather than recomputed: the caller already derived it to
    // write the completion log, and two independent derivations of the same ordinal drift.
    unit.records.add(new SessionView.RecordView(attempt, shard, outcome, durationMs, now));
    switch (outcome) {
      case PASSED -> unit.state = TestState.PASSED;
      case FAILED -> {
        // The whole retry model, in two lines: spend an attempt, and go straight back to the
        // claimable queue if any remain. No pass-specific pool and no barrier to wait on --
        // whichever shard asks next takes it, which is usually a different one.
        unit.attempts++;
        unit.state = unit.attempts < maxAttempts ? TestState.PENDING : TestState.FAILED;
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
    // Part of the replay-safe half of the idle lifecycle. Leases are deliberately not
    // logged, so the clear in lease() exists only in memory: fold the log after a restart
    // and a shard that idled once and then worked for an hour comes back
    // idle-since-its-first-barrier, outranking every genuine waiter and getting them
    // released while work is outstanding. Every logged proof that a shard was working
    // therefore clears it -- here, and on the NACK and stale-result paths below.
    //
    // This narrows the phantom rather than eliminating it: a shard that idles, leases, and
    // dies before producing any of those three leaves nothing in the log to clear it, and
    // replays as idle until its next lease. The residual window is "restart to that shard's
    // next lease" instead of unbounded, and the cost is a fairness skew in the rank rather
    // than lost work -- ranks among live idle shards remain a permutation, so the sizes of
    // the RUN/WAIT/DONE bands do not change. Closing it properly needs leases in the log,
    // which is a deliberate non-goal. On the live path this is a no-op -- lease() cleared it.
    info.idleSince = null;
    info.completed++;
    info.lastSeenAt = now;
    touch(now);
  }

  /** A NACK is proof the shard was holding work, so it also ends the idle clock. */
  void clearIdle(int shard) {
    ShardInfo info = shards.get(shard);
    if (info != null) {
      info.idleSince = null;
    }
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
    clearIdle(request.shard());
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

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  static final class UnitState {
    final ClaimableUnit unit;
    TestState state = TestState.PENDING;
    int attempts;
    Lease lease;
    String reason;
    final List<SessionView.RecordView> records = new ArrayList<>();
  }

  record Lease(
      int shard,
      Fence fence,
      Instant grantedAt,
      Instant expiresAt,
      TestState origin,
      int originAttempts) {}

  static final class ShardInfo {
    boolean departed;
    boolean explicitlyDeparted;
    int completed;
    Instant idleSince;
    boolean released;
    Instant lastSeenAt;
  }
}
