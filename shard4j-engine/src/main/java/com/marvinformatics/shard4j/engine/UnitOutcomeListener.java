package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.Outcome;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.reporting.ReportEntry;

/**
 * Rides one nested Jupiter execution: forwards every event to the build tool's listener --
 * which is what keeps the three report writers producing real per-class files -- and maps
 * events onto terminal outcomes per lease unit.
 *
 * <p>The mapping is per unit, never inferred from the engine-level result, because neither
 * abort shape reaches the root: a {@code @BeforeAll} abort marks the class container
 * aborted and emits <em>nothing at all</em> for its leaves, while the container's own
 * parent still finishes successful; an in-body assumption starts the leaf and then aborts
 * it, sailing past any "did execution begin" check. Both must land as {@code ABORTED}
 * with a reason, or the unit sits leased for the full TTL and the session ends
 * INCOMPLETE naming no cause.
 *
 * <p>Each nested execution gets its own instance, so concurrent class drains never share
 * one -- but a consumer who turns on Jupiter's own parallel execution makes a single
 * nested execution deliver events from many threads at once. The engine tolerates that:
 * the stateful callbacks are synchronised, which keeps finalisation exactly-once per unit
 * where a racing check-then-put could report a unit twice or lose it entirely -- and
 * under a coverage verdict a lost outcome is the exact failure class this engine exists
 * to delete.
 */
final class UnitOutcomeListener implements EngineExecutionListener {

  private final EngineExecutionListener downstream;
  private final UniqueId nestedRootId;
  private final boolean forwardEngineNode;
  private final Set<String> leasedUnits;
  private final Predicate<String> retryable;
  private final Consumer<UnitResult> onUnitComplete;

  private final Map<String, UnitResult> finalized = new HashMap<>();
  private final Map<UniqueId, Long> startedAtNanos = new HashMap<>();
  private final Map<String, List<InvocationRecord>> invocationsByUnit = new HashMap<>();

  UnitOutcomeListener(
      EngineExecutionListener downstream,
      UniqueId nestedRootId,
      boolean forwardEngineNode,
      Set<ExecutionId> leasedUnits,
      Predicate<String> retryable,
      Consumer<UnitResult> onUnitComplete) {
    this.downstream = downstream;
    this.nestedRootId = nestedRootId;
    this.forwardEngineNode = forwardEngineNode;
    this.retryable = retryable;
    this.leasedUnits = new HashSet<>();
    leasedUnits.forEach(unit -> this.leasedUnits.add(unit.value()));
    this.onUnitComplete = onUnitComplete;
  }

  // Deliberately not synchronized, here and in reportingEntryPublished: both are
  // stateless pass-throughs touching none of the maps above, and serialising them would
  // throttle a consumer's parallel nested execution for nothing.
  @Override
  public void dynamicTestRegistered(TestDescriptor descriptor) {
    downstream.dynamicTestRegistered(descriptor);
  }

  @Override
  public synchronized void executionStarted(TestDescriptor descriptor) {
    startedAtNanos.put(descriptor.getUniqueId(), System.nanoTime());
    if (forward(descriptor)) {
      downstream.executionStarted(descriptor);
    }
  }

  @Override
  public synchronized void executionSkipped(TestDescriptor descriptor, String reason) {
    String effective = orUnexplained(reason, "skipped");
    String wireId = wireIdOf(descriptor);
    if (leasedUnits.contains(wireId)) {
      finalize(wireId, Outcome.SKIPPED, 0, effective, invocationsByUnit.get(wireId));
    } else if (isInvocation(descriptor)) {
      recordInvocation(descriptor, Outcome.SKIPPED, 0, effective);
    } else {
      // A skipped container emits no events for its children at all; the units beneath it
      // must still leave LEASED, so the container's reason lands on each of them.
      fillUnitsBeneath(wireId, Outcome.SKIPPED, prefixed(descriptor, effective));
    }
    if (forward(descriptor)) {
      downstream.executionSkipped(descriptor, reason);
    }
  }

  @Override
  public synchronized void executionFinished(
      TestDescriptor descriptor, TestExecutionResult result) {
    long durationMs = elapsedMs(descriptor);
    String wireId = wireIdOf(descriptor);
    if (leasedUnits.contains(wireId)) {
      finalizeUnit(descriptor, wireId, result, durationMs);
    } else if (isInvocation(descriptor)) {
      recordInvocation(descriptor, outcomeOf(result), durationMs, messageOf(result, null));
    } else if (result.getStatus() != TestExecutionResult.Status.SUCCESSFUL) {
      // The @BeforeAll shapes: the container ends aborted or failed having emitted nothing
      // for its children, and the abort does not propagate upward -- so this is the only
      // event from which those units can ever be explained.
      Outcome outcome =
          result.getStatus() == TestExecutionResult.Status.ABORTED ? Outcome.ABORTED : Outcome.FAILED;
      fillUnitsBeneath(
          wireId, outcome, prefixed(descriptor, messageOf(result, outcome.name().toLowerCase(Locale.ROOT))));
    }
    if (forward(descriptor)) {
      downstream.executionFinished(descriptor, downgradeIfRetryable(descriptor, result));
    }
  }

  /**
   * A failure the coordinator will requeue is reported to the *launcher* as aborted, so a
   * shard stays green while a retry is still owed and only an exhausted budget reddens it.
   * This rewrite is one-directional on purpose: {@link #finalize} above has already handed
   * the coordinator the real FAILED, and it must keep doing so -- the coverage verdict
   * counts ABORTED as terminal-OK, so downgrading toward the coordinator would convert a
   * genuine failure into passing coverage.
   */
  private TestExecutionResult downgradeIfRetryable(
      TestDescriptor descriptor, TestExecutionResult result) {
    if (result.getStatus() != TestExecutionResult.Status.FAILED
        || !retryable.test(owningUnitOf(descriptor))) {
      return result;
    }
    return TestExecutionResult.aborted(new RetryPending(result.getThrowable().orElse(null)));
  }

  /**
   * The unit this descriptor's outcome belongs to, by the same rule {@link
   * #executionFinished} applies: a leaf that was itself leased owns its outcome, and
   * anything below a leased container reports under that container.
   *
   * <p>Stripping to the template unconditionally is wrong and silently so. When the
   * coordinator distributes a parameterized method it leases each invocation separately,
   * and the grant keeps the {@code [test-template-invocation:#n]} segment -- so a lookup
   * by the stripped template id misses, every distributed invocation looks non-retryable,
   * and the downgrade quietly does nothing for the one mode that needs it most. Both
   * resolutions live here so they cannot drift apart again.
   */
  private String owningUnitOf(TestDescriptor descriptor) {
    String wireId = wireIdOf(descriptor);
    return leasedUnits.contains(wireId) ? wireId : ExecutionIdentity.leaseId(descriptor).value();
  }

  /** Marks a failure the coordinator still owes a retry; never escapes the launcher report. */
  static final class RetryPending extends RuntimeException {
    RetryPending(Throwable cause) {
      super("failed; the coordinator will requeue this unit for another attempt", cause);
    }
  }

  @Override
  public void reportingEntryPublished(TestDescriptor descriptor, ReportEntry entry) {
    downstream.reportingEntryPublished(descriptor, entry);
  }

  private void finalizeUnit(
      TestDescriptor descriptor, String wireId, TestExecutionResult result, long durationMs) {
    List<InvocationRecord> invocations = invocationsByUnit.get(wireId);
    boolean template =
        "test-template".equals(descriptor.getUniqueId().getLastSegment().getType());
    if (!template || result.getStatus() != TestExecutionResult.Status.SUCCESSFUL) {
      Outcome outcome = outcomeOf(result);
      finalize(
          wireId,
          outcome,
          durationMs,
          messageOf(result, outcome == Outcome.ABORTED ? "assumption failed" : null),
          invocations);
      return;
    }
    // A successful template container says nothing about its rows: Jupiter does not fail
    // the container when an invocation fails, so the aggregate is computed from the
    // invocation records -- the one place the shard knows something the coordinator cannot.
    finalize(wireId, aggregateOf(invocations), durationMs, firstNonPassedReason(invocations), invocations);
  }

  private static Outcome aggregateOf(List<InvocationRecord> invocations) {
    if (invocations == null || invocations.isEmpty()) {
      return Outcome.PASSED;
    }
    boolean anyFailed = invocations.stream().anyMatch(row -> row.outcome() == Outcome.FAILED);
    if (anyFailed) {
      return Outcome.FAILED;
    }
    boolean anyAborted = invocations.stream().anyMatch(row -> row.outcome() == Outcome.ABORTED);
    if (anyAborted) {
      return Outcome.ABORTED;
    }
    boolean allSkipped = invocations.stream().allMatch(row -> row.outcome() == Outcome.SKIPPED);
    return allSkipped ? Outcome.SKIPPED : Outcome.PASSED;
  }

  private static String firstNonPassedReason(List<InvocationRecord> invocations) {
    if (invocations == null) {
      return null;
    }
    return invocations.stream()
        .filter(row -> row.outcome() != Outcome.PASSED)
        .map(row -> orUnexplained(row.reason(), row.outcome().name().toLowerCase(Locale.ROOT)))
        .findFirst()
        .orElse(null);
  }

  private void recordInvocation(
      TestDescriptor descriptor, Outcome outcome, long durationMs, String reason) {
    ExecutionId recordId = ExecutionIdentity.executionId(descriptor);
    String unitId = ExecutionIdentity.leaseId(descriptor).value();
    invocationsByUnit
        .computeIfAbsent(unitId, key -> new ArrayList<>())
        .add(new InvocationRecord(recordId.value(), outcome, durationMs, reason));
  }

  private void fillUnitsBeneath(String containerWireId, Outcome outcome, String reason) {
    String prefix = containerWireId + "/";
    for (String unit : leasedUnits) {
      if (!finalized.containsKey(unit) && unit.startsWith(prefix)) {
        finalize(unit, outcome, 0, reason, invocationsByUnit.get(unit));
      }
    }
  }

  private void finalize(
      String wireId,
      Outcome outcome,
      long durationMs,
      String reason,
      List<InvocationRecord> invocations) {
    if (finalized.containsKey(wireId)) {
      return;
    }
    UnitResult result =
        new UnitResult(
            new ExecutionId(wireId),
            outcome,
            durationMs,
            orUnexplained(reason, needsReason(outcome) ? outcome.name().toLowerCase(Locale.ROOT) : null),
            invocations == null ? null : List.copyOf(invocations));
    finalized.put(wireId, result);
    onUnitComplete.accept(result);
  }

  private static Outcome outcomeOf(TestExecutionResult result) {
    return switch (result.getStatus()) {
      case SUCCESSFUL -> Outcome.PASSED;
      case FAILED -> Outcome.FAILED;
      case ABORTED -> Outcome.ABORTED;
    };
  }

  private static boolean needsReason(Outcome outcome) {
    return outcome == Outcome.SKIPPED || outcome == Outcome.ABORTED;
  }

  private boolean forward(TestDescriptor descriptor) {
    return forwardEngineNode || !descriptor.getUniqueId().equals(nestedRootId);
  }

  private long elapsedMs(TestDescriptor descriptor) {
    Long startedAt = startedAtNanos.get(descriptor.getUniqueId());
    return startedAt == null ? 0 : Math.max(0, (System.nanoTime() - startedAt) / 1_000_000);
  }

  private static String wireIdOf(TestDescriptor descriptor) {
    return ExecutionIdentity.executionId(descriptor).value();
  }

  private static boolean isInvocation(TestDescriptor descriptor) {
    return "test-template-invocation"
        .equals(descriptor.getUniqueId().getLastSegment().getType());
  }

  private static String prefixed(TestDescriptor descriptor, String reason) {
    return descriptor.getDisplayName() + ": " + reason;
  }

  private static String messageOf(TestExecutionResult result, String absentMeans) {
    return result
        .getThrowable()
        .map(Throwable::getMessage)
        .filter(message -> message != null && !message.isBlank())
        .or(() -> Optional.ofNullable(absentMeans))
        .orElse(null);
  }

  private static String orUnexplained(String reason, String absentMeans) {
    return reason == null || reason.isBlank() ? absentMeans : reason;
  }
}
