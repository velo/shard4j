package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.coordinator.storage.DurationStore;
import com.marvinformatics.shard4j.coordinator.storage.HistoryLog;
import com.marvinformatics.shard4j.coordinator.storage.LogRecord;
import com.marvinformatics.shard4j.coordinator.storage.SessionLog;
import com.marvinformatics.shard4j.protocol.BarrierRequest;
import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.CensusUnit;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.DepartResponse;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.HistoryKey;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NackResponse;
import com.marvinformatics.shard4j.protocol.NextClassRequest;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.RegisterResponse;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.ResultResponse;
import com.marvinformatics.shard4j.protocol.SessionView;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * The single writer. All authoritative state lives in memory behind one lock, and every
 * mutation is made durable in the session log before memory changes -- so a crash between
 * the two is recovered by replay, and memory is never ahead of disk.
 *
 * <p>This is deliberately not a distributed system: one instance, one data directory, one
 * serialised write path. The scale it serves is a burst of shards at run start and well
 * under one request per second after that.
 */
@Slf4j
public final class CoordinatorCore {

  private static final String REQUIRED_ID_PREFIX = "[engine:junit-jupiter]/[class:";
  private static final int REASON_LIMIT = 500;
  private static final long SPAN_SLACK_MS = 1_000;

  private final Object writeLock = new Object();
  private final Map<String, Session> sessions = new HashMap<>();
  private final SessionLog sessionLog;
  private final HistoryLog historyLog;
  private final DurationStore durations;
  private final Clock clock;
  private final String tenantKey;
  private final long incarnation;
  private final Duration leaseTtl;
  private final int maxClaimBatch;
  private final Duration gcIdle;
  private long seq;

  // A builder rather than a nine-field positional constructor: two adjacent Durations make
  // a silent transposition possible, and a field reorder would reorder the constructor and
  // still compile.
  @Builder
  private CoordinatorCore(
      SessionLog sessionLog,
      HistoryLog historyLog,
      DurationStore durations,
      Clock clock,
      String tenantKey,
      long incarnation,
      Duration leaseTtl,
      int maxClaimBatch,
      Duration gcIdle) {
    this.sessionLog = sessionLog;
    this.historyLog = historyLog;
    this.durations = durations;
    this.clock = clock;
    this.tenantKey = tenantKey;
    this.incarnation = incarnation;
    this.leaseTtl = leaseTtl;
    this.maxClaimBatch = maxClaimBatch;
    this.gcIdle = gcIdle;
  }

  public RegisterResponse register(String sessionId, RegisterRequest request) {
    validateCensus(request);
    synchronized (writeLock) {
      Instant now = clock.instant();
      Session session = liveSessionOrNull(sessionId, now);
      if (session == null) {
        sessionLog.append(
            LogRecord.registered(
                tenantKey,
                sessionId,
                request.attempt(),
                1,
                request.metadata(),
                request.tests(),
                now));
        session =
            new Session(
                sessionId,
                request.attempt(),
                1,
                request.metadata(),
                request.tests(),
                expandCensus(request.tests()),
                now);
        sessions.put(sessionId, session);
        log.info(
            "Session {} created: {} lease units, attempt {}",
            sessionId,
            session.registeredCount(),
            request.attempt());
      } else {
        requireMatchingCensus(session, request.tests());
        if (request.attempt() > session.attempt()) {
          long newEpoch = session.epoch() + 1;
          sessionLog.append(
              LogRecord.registered(
                  tenantKey,
                  sessionId,
                  request.attempt(),
                  newEpoch,
                  request.metadata(),
                  request.tests(),
                  now));
          session.bumpEpoch(request.attempt(), newEpoch);
          log.info(
              "Session {} rejoined at attempt {}; epoch is now {} and prior leases are void",
              sessionId,
              request.attempt(),
              newEpoch);
        }
      }
      session.declareFleet(request.shardCount());
      joinLogged(sessionId, session, request.shard(), now);
      // The census size, not the expanded unit count: expansion varies with duration
      // history, and a figure printed every run to be noticed must only move when the
      // suite itself does.
      return new RegisterResponse(session.epoch(), session.censusSize());
    }
  }

  public ClaimResponse claim(String sessionId, ClaimRequest request) {
    synchronized (writeLock) {
      Instant now = clock.instant();
      Session session = requireSession(sessionId, now);
      sweepSilentShards(sessionId, session, now);
      for (String candidate : request.candidates()) {
        if (!session.inCensus(candidate)) {
          throw new UnregisteredTestException(candidate);
        }
      }
      // An early-released shard always receives an empty grant: released means the
      // coordinator decided it is not needed, and a grant would un-decide that.
      if (session.isReleased(request.shard())) {
        session.touch(now);
        return new ClaimResponse(List.of());
      }
      List<CensusUnit> claimable =
          request.candidates().stream()
              .flatMap(candidate -> session.claimableUnitsOf(candidate, request.pass()).stream())
              .toList();
      List<CensusUnit> ordered = orderFor(session, claimable);
      List<Grant> granted = grantCapped(session, request.shard(), request.pass(), ordered, now);
      joinLogged(sessionId, session, request.shard(), now);
      return new ClaimResponse(granted);
    }
  }

  /**
   * The open ask -- "what do I run next?" -- which is where cross-class slowest-first
   * actually lives: the whole claimable pool is ranked by {@link ClaimOrdering}, the class
   * of the top-ranked unit is the answer, and that class's first capped batch is leased in
   * the same locked breath so a named class is never an empty promise. The pool arrives
   * already parsed, so ranking it costs no id surgery. Ranking whole units
   * rather than class aggregates means the fixed unit rules extend across classes for
   * free: a class holding a no-history unit outranks every fully-measured class, in the
   * pinned hash order of its unknowns, and known classes follow by their slowest remaining
   * unit. The shard drains the named class through the per-class claim before asking
   * again, which is what keeps {@code @BeforeAll} a once-per-class cost.
   */
  public NextClassResponse nextClass(String sessionId, NextClassRequest request) {
    synchronized (writeLock) {
      Instant now = clock.instant();
      Session session = requireSession(sessionId, now);
      sweepSilentShards(sessionId, session, now);
      // An early-released shard was told it is not needed; naming a class would un-decide
      // that.
      if (session.isReleased(request.shard())) {
        session.touch(now);
        return new NextClassResponse(null, List.of());
      }
      List<CensusUnit> ordered = orderFor(session, session.claimable(request.pass()));
      Set<String> triedClasses = new LinkedHashSet<>();
      for (CensusUnit top : ordered) {
        String className = top.className();
        if (!triedClasses.add(className)) {
          continue;
        }
        List<CensusUnit> inChosenClass =
            ordered.stream().filter(unit -> className.equals(unit.className())).toList();
        List<Grant> granted =
            grantCapped(session, request.shard(), request.pass(), inChosenClass, now);
        if (!granted.isEmpty()) {
          joinLogged(sessionId, session, request.shard(), now);
          return new NextClassResponse(className, granted);
        }
        // Everything in this class is capped for this shard right now: its share of the
        // class's invocations is taken and the rest is held for shards still working or
        // still arriving. The next class in the schedule may still have work for it.
      }
      // The empty answer is a commitment: the shard's pull loop stops on it, so it is
      // remembered -- the fair-share cap must never again hold anything back for a shard
      // that will not ask.
      session.markExhausted(request.shard(), request.pass());
      joinLogged(sessionId, session, request.shard(), now);
      return new NextClassResponse(null, List.of());
    }
  }

  /**
   * Leases the capped prefix of an already-ordered claimable list. Whole units lease
   * freely, exactly as before distribution existed; expanded invocation units are
   * additionally held to the method's fair-share allowance, so one fast asker cannot take
   * a template whose spreading is the entire point of expanding it.
   */
  private List<Grant> grantCapped(
      Session session, int shard, Pass pass, List<CensusUnit> ordered, Instant now) {
    List<Grant> granted = new ArrayList<>();
    Map<String, Integer> allowanceLeft = new HashMap<>();
    for (CensusUnit unit : ordered) {
      if (granted.size() >= maxClaimBatch) {
        break;
      }
      if (unit.invocation() != null) {
        String censusId = session.censusIdOf(unit.id());
        int left =
            allowanceLeft.computeIfAbsent(
                censusId, id -> session.invocationAllowance(id, shard, pass, now));
        if (left <= 0) {
          continue;
        }
        allowanceLeft.put(censusId, left - 1);
      }
      Fence fence = new Fence(session.epoch(), incarnation, ++seq);
      Instant expiresAt = now.plus(leaseTtl);
      boolean probe = session.isProbe(unit.id());
      session.lease(unit.id(), shard, pass, fence, now, expiresAt);
      granted.add(new Grant(unit.id(), fence, expiresAt, probe));
    }
    return granted;
  }

  /** The scheduler's view of one unit's estimate: per-position for an invocation unit. */
  private List<CensusUnit> orderFor(Session session, List<CensusUnit> claimable) {
    return ClaimOrdering.order(claimable, this::estimateOf, unit -> session.isProbe(unit.id()));
  }

  private OptionalLong estimateOf(CensusUnit unit) {
    return unit.invocation() == null
        ? durations.estimate(unit.historyKey())
        : durations.invocationEstimate(unit.historyKey(), unit.invocation());
  }

  /**
   * The census a shard registers is method-level; what the scheduler hands out is this
   * expansion of it. A template method whose duration history carries a complete
   * invocation breakdown becomes one claimable unit per recorded position -- handed out
   * optimistically, reconciled by the shard if a position no longer exists -- plus one
   * cardinality probe past the last, which is how growth of the parameter set is noticed
   * at all: JUnit materialises nothing for a nonexistent selection, so only handing out a
   * position past the plan can prove there is not one. Everything else -- plain methods,
   * templates never seen or never seen completing -- stays one whole unit, exactly the
   * unknowns-first fallback the ordering rule already uses, one level down.
   */
  private Map<String, List<ClaimableUnit>> expandCensus(List<String> censusIds) {
    Map<String, List<ClaimableUnit>> expansion = new LinkedHashMap<>();
    for (String censusId : censusIds) {
      expansion.put(censusId, expand(censusId));
    }
    return expansion;
  }

  private List<ClaimableUnit> expand(String censusId) {
    CensusUnit whole = CensusUnit.parse(censusId);
    if (!whole.template()) {
      return List.of(new ClaimableUnit(whole, false));
    }
    List<Integer> plan = durations.invocationPlan(whole.historyKey());
    if (plan.isEmpty()) {
      return List.of(new ClaimableUnit(whole, false));
    }
    List<ClaimableUnit> expanded = new ArrayList<>();
    for (int position : plan) {
      expanded.add(new ClaimableUnit(whole.atPosition(position), false));
    }
    int pastThePlan = plan.get(plan.size() - 1) + 1;
    expanded.add(new ClaimableUnit(whole.atPosition(pastThePlan), true));
    log.info(
        "Expanding {} into {} invocation unit(s) plus a cardinality probe at #{}",
        censusId,
        plan.size(),
        pastThePlan);
    return expanded;
  }

  public ResultResponse result(String sessionId, ResultRequest request) {
    validateResult(request);
    synchronized (writeLock) {
      Instant now = clock.instant();
      Session session = requireSession(sessionId, now);
      sweepSilentShards(sessionId, session, now);
      if (!session.isRegistered(request.testId())) {
        throw new UnregisteredTestException(request.testId());
      }
      Session.Lease lease = session.currentLease(request.testId());
      if (lease == null || !lease.fence().equals(request.fence())) {
        session.recordStale(request);
        session.touch(now);
        log.warn(
            "Stale result for {} in session {} rejected; the payload is kept aside untouched",
            request.testId(),
            sessionId);
        throw new StaleFenceException(lease == null ? null : lease.fence());
      }
      // The fence proves the caller holds the lease, so a shard or pass that disagrees with
      // it is a client bug -- and a mislabelled pass would corrupt the failedIn bookkeeping
      // that decides which retry pool a failure lands in.
      if (lease.shard() != request.shard()) {
        throw new ProtocolViolationException(
            "Result for "
                + request.testId()
                + " reports shard "
                + request.shard()
                + " but the lease is held by shard "
                + lease.shard());
      }
      if (lease.pass() != request.pass()) {
        throw new ProtocolViolationException(
            "Result for "
                + request.testId()
                + " reports pass "
                + request.pass()
                + " but the lease was granted for pass "
                + lease.pass());
      }
      // The lease-to-result span is a sanity bound on the engine-measured duration, not the
      // measurement: under batched claims the span legitimately exceeds the duration, so
      // only the impossible direction -- a duration longer than the lease was even held --
      // is worth a line in the log.
      long observedSpanMs = Duration.between(lease.grantedAt(), now).toMillis();
      if (request.durationMs() > observedSpanMs + SPAN_SLACK_MS) {
        log.warn(
            "Result for {} reports {} ms but its lease was held for only {} ms;"
                + " the engine-measured duration stays primary",
            request.testId(),
            request.durationMs(),
            observedSpanMs);
      }
      String reason = truncate(request.reason());
      sessionLog.append(
          LogRecord.unitCompletion(
              tenantKey,
              sessionId,
              session.epoch(),
              request.testId(),
              request.shard(),
              request.pass(),
              request.outcome(),
              request.durationMs(),
              request.firstOnShard(),
              reason,
              now));
      session.applyResult(
          request.shard(),
          request.pass(),
          request.testId(),
          request.outcome(),
          request.durationMs(),
          reason,
          now);
      appendHistory(sessionId, session.epoch(), request, reason, now);
      recordDurations(sessionId, session, request);
      return new ResultResponse(true, null);
    }
  }

  public NackResponse nack(String sessionId, NackRequest request) {
    synchronized (writeLock) {
      Instant now = clock.instant();
      Session session = requireSession(sessionId, now);
      sweepSilentShards(sessionId, session, now);
      List<String> released = new ArrayList<>();
      List<String> rejected = new ArrayList<>();
      for (NackRequest.NackedLease lease : request.leases()) {
        Fence current =
            session.isRegistered(lease.testId()) ? session.currentFence(lease.testId()) : null;
        if (current != null && current.equals(lease.fence())) {
          session.releaseLease(lease.testId());
          session.recordNack(lease, now);
          sessionLog.appendQuietly(
              LogRecord.nack(
                  tenantKey,
                  sessionId,
                  request.shard(),
                  lease.testId(),
                  truncate(lease.reason()),
                  lease.vanished(),
                  now));
          if (lease.vanished()) {
            applyVanished(sessionId, session, lease.testId());
          }
          released.add(lease.testId());
        } else {
          rejected.add(lease.testId());
        }
      }
      return new NackResponse(released, rejected);
    }
  }

  public DepartResponse depart(String sessionId, DepartRequest request) {
    synchronized (writeLock) {
      Instant now = clock.instant();
      Session session = requireSession(sessionId, now);
      if (request.epoch() != session.epoch()) {
        throw new StaleEpochException(request.epoch(), session.epoch());
      }
      // Durable before memory, like every mutation: a crash between the two must find the
      // departure on disk, never only in memory.
      if (!session.hasDeparted(request.shard())) {
        sessionLog.append(LogRecord.departed(tenantKey, sessionId, request.shard(), now));
      }
      session.depart(request.shard());
      session.touch(now);
      return new DepartResponse(request.shard(), true);
    }
  }

  /**
   * The barrier: arrival is the pass-completion report, polled while waiting. The decision
   * itself lives in the session; this method makes what it decides durable -- the pass
   * watermark and any early release go through the completion log, because a restart that
   * forgot either would re-grant work to a released shard or hold a quorum open for a
   * shard that already finished.
   */
  public BarrierResponse barrier(String sessionId, BarrierRequest request) {
    if (request.completedPass() == null) {
      throw new ProtocolViolationException(
          "completedPass is required; a barrier arrival is the pass-completion report");
    }
    synchronized (writeLock) {
      Instant now = clock.instant();
      Session session = requireSession(sessionId, now);
      if (request.epoch() != session.epoch()) {
        throw new StaleEpochException(request.epoch(), session.epoch());
      }
      sweepSilentShards(sessionId, session, now);
      Pass completedSoFar = session.completedPassOf(request.shard());
      if (completedSoFar == null || completedSoFar.ordinal() < request.completedPass().ordinal()) {
        sessionLog.append(
            LogRecord.passComplete(
                tenantKey,
                sessionId,
                session.epoch(),
                request.shard(),
                request.completedPass(),
                now));
      }
      session.completePass(request.shard(), request.completedPass(), now);
      BarrierResponse decision = session.barrierDecision(request.shard(), request.completedPass());
      // DONE after the final pass just means nothing is left; recording it as RELEASED
      // would claim an early-release decision that was never made.
      if (decision.action() == BarrierResponse.Action.DONE
          && request.completedPass() != Pass.RETRY2
          && !session.isReleased(request.shard())) {
        sessionLog.append(
            LogRecord.released(tenantKey, sessionId, session.epoch(), request.shard(), now));
        session.release(request.shard());
        log.info(
            "Session {}: shard {} released after {}; it will claim nothing further",
            sessionId,
            request.shard(),
            request.completedPass());
      }
      return decision;
    }
  }

  public SessionView view(String sessionId) {
    synchronized (writeLock) {
      Instant now = clock.instant();
      Session session = requireSession(sessionId, now);
      sweepSilentShards(sessionId, session, now);
      return session.view();
    }
  }

  /**
   * Boot replay: fold the surviving log window back into memory. Leases are deliberately
   * not part of the log -- a lease is a liveness claim and after a restart the coordinator
   * knows nothing about liveness, so every non-terminal unit returns as PENDING and
   * re-handing is the at-least-once behaviour already permitted.
   */
  public void replay(List<LogRecord> records) {
    synchronized (writeLock) {
      for (LogRecord record : records) {
        switch (record.type()) {
          case REGISTERED -> replayRegistered(record);
          case JOINED -> replayJoined(record);
          case COMPLETION -> replayCompletion(record);
          case NACK -> replayNack(record);
          case PASS_COMPLETE -> replayPassComplete(record);
          case DEPARTED -> replayDeparted(record);
          case RELEASED -> replayReleased(record);
        }
      }
      Instant now = clock.instant();
      sessions.values().removeIf(session -> session.lastActivity().plus(gcIdle).isBefore(now));
      if (!sessions.isEmpty()) {
        log.info("Replay recovered {} live session(s)", sessions.size());
      }
    }
  }

  private void replayRegistered(LogRecord record) {
    Session session = sessions.get(record.session());
    if (session == null) {
      sessions.put(
          record.session(),
          new Session(
              record.session(),
              record.attempt(),
              record.epoch(),
              record.metadata(),
              record.tests(),
              expandCensus(record.tests()),
              record.ts()));
      return;
    }
    // The live path rejects a conflicting census before it is ever appended, so two
    // REGISTERED records for one session with different test sets mean the log is
    // contradictory; folding them silently would replay completions into the wrong census.
    if (!session.censusIds().equals(new HashSet<>(record.tests()))) {
      throw new IllegalStateException(
          "Refusing replay: session "
              + record.session()
              + " has REGISTERED records with conflicting test sets; repair the"
              + " session log before starting");
    }
    if (record.attempt() > session.attempt()) {
      session.bumpEpoch(record.attempt(), record.epoch());
      session.touch(record.ts());
    }
  }

  private void replayJoined(LogRecord record) {
    Session session = sessions.get(record.session());
    if (session != null) {
      session.join(record.shard(), record.ts());
    }
  }

  private void replayCompletion(LogRecord record) {
    Session session = sessions.get(record.session());
    if (session == null || !Boolean.TRUE.equals(record.unit())) {
      return;
    }
    if (!session.isRegistered(record.testId())) {
      log.warn("Replay: completion for unregistered {} ignored", record.testId());
      return;
    }
    session.applyResult(
        record.shard(),
        record.pass(),
        record.testId(),
        record.outcome(),
        record.durationMs(),
        record.reason(),
        record.ts());
  }

  private void replayNack(LogRecord record) {
    Session session = sessions.get(record.session());
    if (session == null) {
      return;
    }
    boolean vanished = Boolean.TRUE.equals(record.vanished());
    session.recordNack(
        new NackRequest.NackedLease(record.testId(), null, record.reason(), vanished), record.ts());
    // Only the census correction is replayed; the duration-store drop lives in the
    // snapshot. A re-expansion that resurrects the probe merely re-probes and re-vanishes.
    if (vanished && session.isRegistered(record.testId()) && session.isProbe(record.testId())) {
      session.removeVanishedProbe(record.testId());
    }
  }

  private void replayPassComplete(LogRecord record) {
    Session session = sessions.get(record.session());
    if (session != null) {
      session.completePass(record.shard(), record.pass(), record.ts());
    }
  }

  private void replayDeparted(LogRecord record) {
    Session session = sessions.get(record.session());
    if (session != null) {
      session.depart(record.shard());
    }
  }

  private void replayReleased(LogRecord record) {
    Session session = sessions.get(record.session());
    if (session != null) {
      session.release(record.shard());
    }
  }

  /**
   * The silent-death detector, run before every decision. Lease expiry catches a shard
   * that died mid-unit; the poll-silence sweep catches one that died holding nothing -- a
   * waiter at a barrier has no lease for expiry to notice. Both mark the shard departed
   * and log the departure so a restart does not resurrect the ghost into a barrier quorum.
   * Quietly, because the sweep fires on read paths too -- and a lost record is not fatal:
   * replay would revive the ghost from its COMPLETION records with no lease left to
   * re-expire, but this same sweep re-departs it as soon as its silence exceeds the
   * tolerance, so the cost is a slower INCOMPLETE, never a wedged fleet.
   */
  private void sweepSilentShards(String sessionId, Session session, Instant now) {
    for (int shard : session.releaseExpiredLeases(now)) {
      sessionLog.appendQuietly(LogRecord.departed(tenantKey, sessionId, shard, now));
    }
    for (int shard : session.departSilentShards(now)) {
      sessionLog.appendQuietly(LogRecord.departed(tenantKey, sessionId, shard, now));
    }

  }

  /**
   * Joins are durable like departures, and for the same reason: a shard that registered
   * but completed nothing before a restart would otherwise vanish from the replayed
   * roster, and every quorum would resolve without it -- a premature RUN and zero
   * rebalance for whatever it was still running. Logged only on the transition into the
   * roster, since every claim re-joins.
   */
  private void joinLogged(String sessionId, Session session, int shard, Instant now) {
    if (!session.hasJoined(shard)) {
      sessionLog.append(LogRecord.joined(tenantKey, sessionId, session.epoch(), shard, now));
    }
    session.join(shard, now);
  }

  /** The same idle rule the lazy path applies per lookup, in bulk, for the scheduler. */
  public void gcIdleSessions() {
    synchronized (writeLock) {
      Instant now = clock.instant();
      sessions.values().removeIf(session -> session.lastActivity().plus(gcIdle).isBefore(now));
    }
  }

  private Session requireSession(String sessionId, Instant now) {
    Session session = liveSessionOrNull(sessionId, now);
    if (session == null) {
      throw new UnknownSessionException(sessionId);
    }
    return session;
  }

  /** Idle GC applies the same rule lazily that boot replay applies in bulk. */
  private Session liveSessionOrNull(String sessionId, Instant now) {
    Session session = sessions.get(sessionId);
    if (session != null && session.lastActivity().plus(gcIdle).isBefore(now)) {
      sessions.remove(sessionId);
      return null;
    }
    return session;
  }

  private void appendHistory(
      String sessionId, long epoch, ResultRequest request, String reason, Instant now) {
    historyLog.append(
        LogRecord.unitCompletion(
            tenantKey,
            sessionId,
            epoch,
            request.testId(),
            request.shard(),
            request.pass(),
            request.outcome(),
            request.durationMs(),
            request.firstOnShard(),
            reason,
            now));
    if (request.invocations() != null) {
      for (InvocationRecord invocation : request.invocations()) {
        historyLog.append(
            LogRecord.invocationCompletion(
                tenantKey,
                sessionId,
                epoch,
                invocation.testId(),
                request.shard(),
                request.pass(),
                invocation.outcome(),
                invocation.durationMs(),
                truncate(invocation.reason()),
                now));
      }
    }
  }

  /**
   * What a completed unit teaches the scheduler. A whole unit feeds the method aggregate
   * as ever, and a PASSED template additionally contributes its per-row breakdown, marked
   * complete because a passing aggregate enumerated every row it materialised. An
   * individually-leased invocation contributes its own position -- a skipped row at
   * duration zero, so a conditionally-skipped position stays in the plan instead of
   * silently leaving the hand-out -- and the breakdown is marked complete only once every
   * measured position of the method has absorbed without failing. A probe that passes in
   * MAIN is real growth: the next position is probed in the same session, so the plan
   * walks the growth instead of discovering one row per run.
   */
  private void recordDurations(String sessionId, Session session, ResultRequest request) {
    CensusUnit unit = session.unitOf(request.testId());
    HistoryKey key = unit.historyKey();
    if (unit.invocation() == null) {
      if (request.outcome() != Outcome.PASSED) {
        return;
      }
      durations.recordPassed(key, sessionId, request.durationMs(), request.firstOnShard());
      if (request.invocations() == null || request.invocations().isEmpty()) {
        return;
      }
      boolean everyRowUsable = true;
      for (InvocationRecord row : request.invocations()) {
        Integer position = positionOfRecordId(row.testId());
        if (position == null) {
          everyRowUsable = false;
          continue;
        }
        long rowDuration = row.outcome() == Outcome.SKIPPED ? 0 : row.durationMs();
        durations.recordInvocation(key, sessionId, position, rowDuration);
      }
      if (everyRowUsable) {
        durations.markInvocationsComplete(key, sessionId);
      }
      return;
    }
    if (request.outcome() != Outcome.PASSED && request.outcome() != Outcome.SKIPPED) {
      return;
    }
    long duration = request.outcome() == Outcome.SKIPPED ? 0 : request.durationMs();
    durations.recordInvocation(key, sessionId, unit.invocation(), duration);
    String censusId = session.censusIdOf(request.testId());
    if (session.measuredUnitsAllNonFailing(censusId)) {
      durations.markInvocationsComplete(key, sessionId);
    }
    if (request.outcome() == Outcome.PASSED
        && request.pass() == Pass.MAIN
        && session.isProbe(request.testId())) {
      int next = unit.invocation() + 1;
      session.addProbe(censusId, CensusUnit.parse(censusId).atPosition(next));
      log.info(
          "Session {}: probe {} materialised -- the parameter set grew; probing #{} next",
          sessionId,
          request.testId(),
          next);
    }
  }

  private static Integer positionOfRecordId(String recordId) {
    try {
      return CensusUnit.parse(recordId).invocation();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * The shard proved this id resolves to nothing in the current commit. For a probe that
   * is the expected answer: the unit leaves the census, confirming the recorded parameter
   * count. For a measured invocation it means the parameter set shrank since it was
   * measured, so the stale position is dropped from history -- the current session stays
   * loud (the unit stays PENDING and the shard that discovered the drift has already
   * failed naming it), but the next session expands from the corrected plan.
   */
  private void applyVanished(String sessionId, Session session, String testId) {
    CensusUnit unit = session.unitOf(testId);
    if (unit.invocation() == null) {
      return;
    }
    if (session.isProbe(testId)) {
      session.removeVanishedProbe(testId);
      log.info(
          "Session {}: cardinality probe {} does not exist; the recorded parameter count"
              + " stands",
          sessionId,
          testId);
      return;
    }
    durations.dropInvocation(unit.historyKey(), unit.invocation());
    log.warn(
        "Session {}: invocation {} no longer exists -- the parameter set changed since it"
            + " was last measured; dropped from duration history",
        sessionId,
        testId);
  }

  private static void validateCensus(RegisterRequest request) {
    if (request.attempt() < 1) {
      throw new ProtocolViolationException("attempt must be a positive integer");
    }
    if (request.shardCount() != null && request.shardCount() < 1) {
      throw new ProtocolViolationException("shardCount must be a positive integer when present");
    }
    if (request.tests() == null || request.tests().isEmpty()) {
      throw new ProtocolViolationException(
          "A census must enumerate at least one lease unit; an empty enumeration is the"
              + " silently-running-nothing failure mode and is refused");
    }
    Set<String> seen = new HashSet<>();
    for (String testId : request.tests()) {
      if (testId == null || !testId.startsWith(REQUIRED_ID_PREFIX)) {
        throw new ProtocolViolationException(
            "Execution ids must be rooted at " + REQUIRED_ID_PREFIX + "...]: " + testId);
      }
      CensusUnit unit;
      try {
        unit = CensusUnit.parse(testId);
      } catch (IllegalArgumentException e) {
        throw new ProtocolViolationException(e.getMessage());
      }
      if (unit.invocation() != null) {
        throw new ProtocolViolationException(
            "An invocation id is a record id, never a lease unit: " + testId);
      }
      if (!seen.add(testId)) {
        throw new ProtocolViolationException("Duplicate lease unit in census: " + testId);
      }
    }
  }

  /**
   * Registration carries the whole census, so the comparison is over sets directly and a
   * mismatch names exactly which ids diverged -- no digest, so no separator, collation or
   * charset for the two sides to agree on.
   */
  private static void requireMatchingCensus(Session session, List<String> offeredTests) {
    Set<String> stored = session.censusIds();
    Set<String> offered = new LinkedHashSet<>(offeredTests);
    if (stored.equals(offered)) {
      return;
    }
    List<String> onlyStored = stored.stream().filter(id -> !offered.contains(id)).sorted().toList();
    List<String> onlyOffered =
        offered.stream().filter(id -> !stored.contains(id)).sorted().toList();
    throw new RegistrationMismatchException(onlyStored, onlyOffered);
  }

  private static void validateResult(ResultRequest request) {
    if (request.durationMs() < 0) {
      throw new ProtocolViolationException("durationMs must not be negative");
    }
    boolean needsReason =
        request.outcome() == Outcome.SKIPPED || request.outcome() == Outcome.ABORTED;
    if (needsReason && (request.reason() == null || request.reason().isBlank())) {
      throw new ProtocolViolationException(
          request.outcome() + " requires a non-empty reason; the printed lists are the safety"
              + " net that keeps admitted non-passes honest");
    }
    if (request.outcome() == Outcome.PASSED && request.invocations() != null) {
      // SKIPPED rows are the one admissible mix: a per-invocation disabling condition
      // skipping a row of an otherwise-passing template still means the unit ran and
      // passed everything it ran. FAILED or ABORTED rows contradict the aggregate.
      boolean inconsistent =
          request.invocations().stream()
              .anyMatch(
                  invocation ->
                      invocation.outcome() == Outcome.FAILED
                          || invocation.outcome() == Outcome.ABORTED);
      if (inconsistent) {
        throw new ProtocolViolationException(
            "A PASSED unit cannot carry a FAILED or ABORTED invocation; the aggregate must"
                + " be consistent with what it aggregates");
      }
    }
  }

  private static String truncate(String reason) {
    if (reason == null || reason.length() <= REASON_LIMIT) {
      return reason;
    }
    return reason.substring(0, REASON_LIMIT);
  }
}
