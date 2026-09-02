package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

/**
 * One live session: the census, the per-unit state machine and the diagnostic side
 * channels. The shard roster and every policy that reads only it live in
 * {@link ShardRoster}. All mutation happens under the coordinator's single write lock.
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

  @Getter private final String id;
  private final Map<String, String> metadata;
  private final Set<String> registered = new LinkedHashSet<>();
  private final Map<String, List<String>> unitsByCensusId = new LinkedHashMap<>();
  private final Map<String, UnitState> units = new LinkedHashMap<>();
  private final ShardRoster roster = new ShardRoster();
  private final List<NackRequest.NackedLease> nacks = new ArrayList<>();
  private final List<ResultRequest> staleResults = new ArrayList<>();
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
    this.fairShare = new FairShare(roster, now);
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
    roster.join(shard, now);
    touch(now);
  }

  void depart(int shard) {
    roster.depart(shard);
  }

  boolean hasJoined(int shard) {
    return roster.hasJoined(shard);
  }

  boolean hasDeparted(int shard) {
    return roster.hasDeparted(shard);
  }

  boolean isIdle(int shard) {
    return roster.isIdle(shard);
  }

  boolean isReleased(int shard) {
    return roster.isReleased(shard);
  }

  void release(int shard) {
    roster.release(shard);
  }

  void markIdle(int shard, Instant now) {
    roster.markIdle(shard, now);
    touch(now);
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
    roster.epochBumped();
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

  /** True when every measured (non-probe) unit of the method has reached an absorbing state. */
  boolean measuredUnitsAllAbsorbed(String censusId) {
    return unitsByCensusId.getOrDefault(censusId, List.of()).stream()
        .map(units::get)
        .filter(unit -> !unit.unit.probe())
        .allMatch(unit -> unit.state.isAbsorbing());
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

  /** Work still in play: a unit claimable now, or one leased that could yet requeue. */
  boolean hasOutstandingWork() {
    return units.values().stream()
        .anyMatch(unit -> unit.state == TestState.PENDING || unit.state == TestState.LEASED);
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
  BarrierResponse barrierDecision(int shard) {
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
    int rank = roster.hungerRank(shard);
    if (rank < claimable) {
      return new BarrierResponse(BarrierResponse.Action.RUN, null, null);
    }
    if (rank < claimable + outstanding) {
      return new BarrierResponse(
          BarrierResponse.Action.WAIT, ShardRoster.RETRY_AFTER_SECONDS, earliestLeaseExpiry);
    }
    return done();
  }

  private static BarrierResponse done() {
    return new BarrierResponse(BarrierResponse.Action.DONE, null, null);
  }

  void lease(String testId, int shard, Fence fence, Instant grantedAt, Instant expiresAt) {
    UnitState unit = units.get(testId);
    unit.lease = new Lease(shard, fence, grantedAt, expiresAt, unit.state, unit.attempts);
    unit.state = TestState.LEASED;
    roster.proofOfWork(shard);
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
        if (roster.presumeDead(unit.lease.shard())) {
          newlyDeparted.add(unit.lease.shard());
        }
        restore(unit);
      }
    }
    return newlyDeparted;
  }

  /** The roster's silence sweep, told which shards are excused by holding a lease. */
  List<Integer> departSilentShards(Instant now) {
    Set<Integer> leaseHolders = new HashSet<>();
    for (UnitState unit : units.values()) {
      if (unit.state == TestState.LEASED) {
        leaseHolders.add(unit.lease.shard());
      }
    }
    return roster.departSilent(now, leaseHolders);
  }

  Fence currentFence(String testId) {
    Lease lease = currentLease(testId);
    return lease != null ? lease.fence() : null;
  }

  /**
   * True when a failure of the attempt about to run would be requeued rather than made
   * terminal -- the same expression {@link #applyResult} evaluates once the attempt is
   * spent, so the promise the grant carries and the decision that honours it cannot say
   * different things.
   */
  boolean retryableAfterFailure(String testId) {
    return attemptsOf(testId) + 1 < maxAttempts;
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
    roster.recordCompletion(shard, now);
    touch(now);
  }

  void recordNack(int shard, NackRequest.NackedLease lease, Instant now) {
    roster.proofOfWork(shard);
    if (nacks.size() < DIAGNOSTIC_CAP) {
      nacks.add(lease);
    } else {
      nacksDropped++;
    }
    touch(now);
  }

  void recordStale(ResultRequest request) {
    roster.proofOfWork(request.shard());
    if (staleResults.size() < DIAGNOSTIC_CAP) {
      staleResults.add(request);
    } else {
      staleResultsDropped++;
    }
  }

  SessionView view() {
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
        roster.views(),
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
}
