package com.marvinformatics.shard4j.coordinator.core;

import com.marvinformatics.shard4j.coordinator.storage.DurationStore;
import com.marvinformatics.shard4j.protocol.CensusUnit;
import com.marvinformatics.shard4j.protocol.HistoryKey;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Everything the coordinator knows about distributing a template method's invocations:
 * how a method-level census expands into per-position units plus a cardinality probe,
 * what a completed unit teaches the duration store, how the probe walk discovers growth,
 * and what a shard-proven vanished position corrects. It touches only the duration store
 * and the session's per-unit metadata; the state machine, roster and durability stay with
 * {@link CoordinatorCore}.
 */
@Slf4j
@RequiredArgsConstructor
final class InvocationDistribution {

  private final DurationStore durations;

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
  Map<String, List<ClaimableUnit>> expandCensus(List<String> censusIds) {
    Map<String, List<ClaimableUnit>> expansion = new LinkedHashMap<>();
    for (String censusId : censusIds) {
      expansion.put(censusId, expand(censusId));
    }
    return expansion;
  }

  private List<ClaimableUnit> expand(String censusId) {
    CensusUnit whole = CensusUnit.parse(censusId);
    if (!whole.template()) {
      return List.of(new ClaimableUnit(censusId, whole, false));
    }
    List<Integer> plan = durations.invocationPlan(whole.historyKey());
    if (plan.isEmpty()) {
      return List.of(new ClaimableUnit(censusId, whole, false));
    }
    List<ClaimableUnit> expanded = new ArrayList<>();
    for (int position : plan) {
      expanded.add(new ClaimableUnit(censusId, whole.atPosition(position), false));
    }
    int pastThePlan = plan.get(plan.size() - 1) + 1;
    expanded.add(new ClaimableUnit(censusId, whole.atPosition(pastThePlan), true));
    log.info(
        "Expanding {} into {} invocation unit(s) plus a cardinality probe at #{}",
        censusId,
        plan.size(),
        pastThePlan);
    return expanded;
  }

  /** The scheduler's view of one unit's estimate: per-position for an invocation unit. */
  OptionalLong estimateOf(ClaimableUnit unit) {
    return unit.invocation() == null
        ? durations.estimate(unit.historyKey())
        : durations.invocationEstimate(unit.historyKey(), unit.invocation());
  }

  /**
   * What a completed unit teaches the scheduler. A whole unit feeds the method aggregate,
   * and a template that measured work additionally contributes its per-row breakdown,
   * marked complete because such an aggregate enumerated every row it materialised. An
   * individually-leased invocation contributes its own position -- a skipped row at
   * duration zero, so a conditionally-skipped position stays in the plan instead of
   * silently leaving the hand-out -- and the breakdown is marked complete only once every
   * measured position of the method has reached an absorbing state. A probe that
   * materialises at all is real growth -- passing, failing, skipping or aborting, it
   * proved the position exists -- so the next position is probed in the same session and
   * the plan walks the growth instead of discovering one row per run.
   *
   * <p>{@link Outcome#measuredWork()} is the gate, not {@code == PASSED}: a template
   * whose rows include an assumption-skipped one aggregates to ABORTED, and gating on
   * PASSED left such a method with no history at all -- so it leased whole in every
   * session and one shard ran the lot. A suite that skips by assumption is the common
   * case, not the corner.
   */
  void recordDurations(String sessionId, Session session, ResultRequest request) {
    ClaimableUnit unit = session.unitOf(request.testId());
    HistoryKey key = unit.historyKey();
    if (unit.invocation() == null) {
      if (!request.outcome().measuredWork()) {
        return;
      }
      durations.recordMeasured(key, sessionId, request.durationMs(), request.firstOnShard());
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
    String censusId = unit.censusId();
    // A probe that materialised is proof the set grew past the plan, whatever its outcome
    // and whichever shard ran it -- a truly nonexistent position produces no result at all,
    // only a vanished NACK. Chained before any outcome gate on purpose: an ABORTED probe
    // absorbs green, and gating the walk on PASSED would halt discovery there, leaving
    // every row past it silently unrun in every session.
    if (unit.probe()) {
      int next = unit.invocation() + 1;
      boolean added =
          session.addProbe(
              new ClaimableUnit(censusId, CensusUnit.parse(censusId).atPosition(next), true));
      if (added) {
        log.info(
            "Session {}: probe {} materialised -- the parameter set grew; probing #{} next",
            sessionId,
            request.testId(),
            next);
      }
    }
    if (request.outcome() == Outcome.FAILED) {
      return;
    }
    long duration = request.outcome() == Outcome.SKIPPED ? 0 : request.durationMs();
    durations.recordInvocation(key, sessionId, unit.invocation(), duration);
    if (session.measuredUnitsAllNonFailing(censusId)) {
      durations.markInvocationsComplete(key, sessionId);
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
  void applyVanished(String sessionId, Session session, String testId) {
    ClaimableUnit unit = session.unitOf(testId);
    if (unit.invocation() == null) {
      return;
    }
    if (unit.probe()) {
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

  private static Integer positionOfRecordId(String recordId) {
    try {
      return CensusUnit.parse(recordId).invocation();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
