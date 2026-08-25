package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.coordinator.storage.DurationStore;
import com.marvinformatics.shard4j.coordinator.storage.HistoryLog;
import com.marvinformatics.shard4j.coordinator.storage.LogRecord;
import com.marvinformatics.shard4j.coordinator.storage.SessionLog;
import com.marvinformatics.shard4j.protocol.BarrierRequest;
import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.DepartResponse;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NackResponse;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
      joinLogged(sessionId, session, request.shard(), now);
      return new RegisterResponse(session.epoch(), session.registeredCount());
    }
  }

  public ClaimResponse claim(String sessionId, ClaimRequest request) {
    synchronized (writeLock) {
      Instant now = clock.instant();
      Session session = requireSession(sessionId, now);
      sweepSilentShards(sessionId, session, now);
      for (String candidate : request.candidates()) {
        if (!session.isRegistered(candidate)) {
          throw new UnregisteredTestException(candidate);
        }
      }
      // An early-released shard always receives an empty grant: released means the
      // coordinator decided it is not needed, and a grant would un-decide that.
      if (session.isReleased(request.shard())) {
        session.touch(now);
        return new ClaimResponse(List.of());
      }
      List<String> claimable =
          request.candidates().stream()
              .filter(candidate -> session.claimableIn(candidate, request.pass()))
              .toList();
      List<String> ordered = ClaimOrdering.order(claimable, durations::estimate);
      List<Grant> granted = new ArrayList<>();
      for (String testId : ordered.subList(0, Math.min(maxClaimBatch, ordered.size()))) {
        Fence fence = new Fence(session.epoch(), incarnation, ++seq);
        Instant expiresAt = now.plus(leaseTtl);
        session.lease(testId, request.shard(), request.pass(), fence, now, expiresAt);
        granted.add(new Grant(testId, fence, expiresAt));
      }
      joinLogged(sessionId, session, request.shard(), now);
      return new ClaimResponse(granted);
    }
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
      if (request.outcome() == Outcome.PASSED) {
        durations.recordPassed(
            HistoryKeys.of(request.testId()), sessionId, request.durationMs(), request.firstOnShard());
      }
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
                  tenantKey, sessionId, request.shard(), lease.testId(), truncate(lease.reason()), now));
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
    if (session != null) {
      session.recordNack(
          new NackRequest.NackedLease(record.testId(), null, record.reason()), record.ts());
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

  private static void validateCensus(RegisterRequest request) {
    if (request.attempt() < 1) {
      throw new ProtocolViolationException("attempt must be a positive integer");
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
      if (testId.contains("[test-template-invocation:")) {
        throw new ProtocolViolationException(
            "An invocation id is a record id, never a lease unit: " + testId);
      }
      try {
        HistoryKeys.of(testId);
      } catch (IllegalArgumentException e) {
        throw new ProtocolViolationException(e.getMessage());
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
      boolean inconsistent =
          request.invocations().stream()
              .anyMatch(invocation -> invocation.outcome() != Outcome.PASSED);
      if (inconsistent) {
        throw new ProtocolViolationException(
            "A PASSED unit cannot carry a non-PASSED invocation; the aggregate must be"
                + " consistent with what it aggregates");
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
