package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.Outcome;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;

/**
 * Every scenario here executes real fixture classes through the real nested Jupiter
 * delegation, so what is asserted is what a consumer's suite will actually produce --
 * including the two abort shapes whose events a naive listener never sees.
 */
class UnitOutcomeListenerTest {

  private static final String PLAIN = PlainShapesFixture.class.getName();
  private static final String SETUP = AbortedSetupFixture.class.getName();
  private static final String ROWS = RowsFixture.class.getName();
  private static final String SKIPPED_ROW = SkippedRowFixture.class.getName();
  private static final String BROKEN_SETUP = FailedSetupFixture.class.getName();

  /**
   * The listener takes the leases the nested execution is running -- one map, not a set of
   * ids alongside a predicate over the same ids. Only {@code retryable} varies here; the
   * rest of a grant is what the coordinator would have sent.
   */
  private static Map<ExecutionId, Grant> leases(
      Set<ExecutionId> units, Predicate<String> retryable) {
    Map<ExecutionId, Grant> leases = new LinkedHashMap<>();
    units.forEach(
        unit ->
            leases.put(
                unit,
                new Grant(
                    unit.value(),
                    new Fence(1, 1, 1),
                    Instant.parse("2026-08-20T10:00:00Z"),
                    false,
                    retryable.test(unit.value()))));
    return leases;
  }

  private static String method(String className, String method) {
    return "[engine:junit-jupiter]/[class:" + className + "]/[method:" + method + "]";
  }

  private static Map<String, UnitResult> execute(String... unitIds) {
    JupiterDelegate jupiter =
        new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID));
    Set<ExecutionId> leased = new HashSet<>();
    for (String unitId : unitIds) {
      leased.add(new ExecutionId(unitId));
    }
    TestDescriptor batch =
        jupiter.discoverIds(
            leased.stream().toList(),
            new MapConfigurationParameters(Map.of()),
            EngineTestHarness.outputDirectoryCreator());
    Map<String, UnitResult> results = new HashMap<>();
    UnitOutcomeListener listener =
        new UnitOutcomeListener(
            EngineExecutionListener.NOOP,
            jupiter.nestedRootId(),
            false,
            leases(leased, unitId -> false),
            result -> results.put(result.unitId().value(), result));
    jupiter.execute(batch, EngineTestHarness.outerRequest(EngineExecutionListener.NOOP), listener);
    return results;
  }

  @Test
  void givenEveryLeafOutcome_whenExecuting_thenEachUnitLeavesWithExactlyOneTerminalState() {
    Map<String, UnitResult> results =
        execute(
            method(PLAIN, "passes()"),
            method(PLAIN, "fails()"),
            method(PLAIN, "abortsInBody()"),
            method(PLAIN, "disabled()"));

    assertThat(results).hasSize(4);
    assertThat(results.get(method(PLAIN, "passes()")).outcome()).isEqualTo(Outcome.PASSED);
    assertThat(results.get(method(PLAIN, "fails()")).outcome()).isEqualTo(Outcome.FAILED);
    UnitResult aborted = results.get(method(PLAIN, "abortsInBody()"));
    assertThat(aborted.outcome()).isEqualTo(Outcome.ABORTED);
    assertThat(aborted.reason()).contains("feature flag off");
    UnitResult disabled = results.get(method(PLAIN, "disabled()"));
    assertThat(disabled.outcome()).isEqualTo(Outcome.SKIPPED);
    assertThat(disabled.reason()).contains("not in this environment");
  }

  @Test
  void givenABeforeAllAbort_whenNoLeafEventsAreEmitted_thenEveryUnitBeneathIsAborted() {
    Map<String, UnitResult> results =
        execute(method(SETUP, "first()"), method(SETUP, "second()"));

    assertThat(results).hasSize(2);
    for (UnitResult result : results.values()) {
      assertThat(result.outcome()).isEqualTo(Outcome.ABORTED);
      assertThat(result.reason()).contains("local service is not running");
    }
  }

  private static String rowsTemplate() {
    return "[engine:junit-jupiter]/[class:" + ROWS + "]/[test-template:rows(java.lang.String)]";
  }

  private static String rowsInvocation(int position) {
    return rowsTemplate() + "/[test-template-invocation:#" + position + "]";
  }

  @Test
  void givenLeasedInvocations_whenExecuting_thenEachFinalizesItselfAndUnselectedRowsNeverRun() {
    Map<String, UnitResult> results = execute(rowsInvocation(1), rowsInvocation(3));

    // Each leased invocation is its own unit with its own terminal outcome and no
    // aggregate row list; the unselected middle row (which would fail) never executed.
    assertThat(results).containsOnlyKeys(rowsInvocation(1), rowsInvocation(3));
    assertThat(results.get(rowsInvocation(1)).outcome()).isEqualTo(Outcome.PASSED);
    assertThat(results.get(rowsInvocation(3)).outcome()).isEqualTo(Outcome.PASSED);
    assertThat(results.get(rowsInvocation(1)).invocations()).isNull();
  }

  @Test
  void givenALeasedFailingInvocation_whenExecuting_thenItFailsAloneUnderItsOwnId() {
    Map<String, UnitResult> results = execute(rowsInvocation(2));

    assertThat(results).containsOnlyKeys(rowsInvocation(2));
    assertThat(results.get(rowsInvocation(2)).outcome()).isEqualTo(Outcome.FAILED);
    assertThat(results.get(rowsInvocation(2)).reason()).contains("row rejected");
  }

  @Test
  void givenAStaleInvocationPastTheParameterSet_whenExecuting_thenItVanishesInSilence() {
    // The property reconciliation exists for: JUnit materialises nothing for a selected
    // position that does not exist -- no event, no error, a clean exit.
    Map<String, UnitResult> results = execute(rowsInvocation(1), rowsInvocation(4));

    assertThat(results).containsOnlyKeys(rowsInvocation(1));
  }

  @Test
  void givenAFailingRow_whenATemplateUnitCompletes_thenTheAggregateFailsAndRowsAreIndividual() {
    String unit = "[engine:junit-jupiter]/[class:" + ROWS + "]/[test-template:rows(java.lang.String)]";
    Map<String, UnitResult> results = execute(unit);

    UnitResult result = results.get(unit);
    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.invocations()).hasSize(3);
    assertThat(result.invocations())
        .extracting(InvocationRecord::outcome)
        .containsExactly(Outcome.PASSED, Outcome.FAILED, Outcome.PASSED);
    assertThat(result.invocations().get(1).testId())
        .isEqualTo(unit + "/[test-template-invocation:#2]");
    assertThat(result.invocations().get(1).reason()).contains("row rejected: broken");
  }

  /**
   * The one admissible mixed aggregate: a per-invocation disabling condition skips a row
   * of an otherwise-passing template. The unit ran and passed everything it ran, so the
   * aggregate stays PASSED -- and the coordinator accepts SKIPPED rows under it, which
   * SessionLoopIT pins from the other side of the wire.
   */
  @Test
  void givenASkippedRow_whenATemplateUnitCompletes_thenTheAggregateStaysPassed() {
    String unit =
        "[engine:junit-jupiter]/[class:" + SKIPPED_ROW + "]/[test-template:rows(java.lang.String)]";
    Map<String, UnitResult> results = execute(unit);

    UnitResult result = results.get(unit);
    assertThat(result.outcome()).isEqualTo(Outcome.PASSED);
    assertThat(result.invocations()).hasSize(3);
    assertThat(result.invocations())
        .extracting(InvocationRecord::outcome)
        .containsExactly(Outcome.PASSED, Outcome.SKIPPED, Outcome.PASSED);
    assertThat(result.invocations().get(1).reason()).contains("beta is off");
  }

  /**
   * Runs one failing unit and returns what each side was told: the status the launcher saw,
   * and the outcome the coordinator was given. They are allowed to disagree in exactly one
   * direction, and this is the fixture that proves which.
   */
  private static Map.Entry<TestExecutionResult.Status, Outcome> failingUnitReportedTo(
      boolean retryable) {
    String failing = method(PLAIN, "fails()");
    JupiterDelegate jupiter = new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID));
    Set<ExecutionId> leased = Set.of(new ExecutionId(failing));
    TestDescriptor batch =
        jupiter.discoverIds(
            leased.stream().toList(),
            new MapConfigurationParameters(Map.of()),
            EngineTestHarness.outputDirectoryCreator());

    // Keyed by the leaf's own segment, not the wire id: what reaches the launcher is the
    // nested descriptor, whose uniqueId carries this engine's root as a prefix.
    Map<String, TestExecutionResult.Status> launcherSaw = new HashMap<>();
    EngineExecutionListener downstream =
        new EngineExecutionListener() {
          @Override
          public void executionFinished(TestDescriptor descriptor, TestExecutionResult result) {
            String id = descriptor.getUniqueId().toString();
            if (id.endsWith("[method:fails()]")) {
              launcherSaw.put("fails", result.getStatus());
            }
          }
        };

    Map<String, UnitResult> reported = new HashMap<>();
    UnitOutcomeListener listener =
        new UnitOutcomeListener(
            downstream,
            jupiter.nestedRootId(),
            false,
            leases(leased, unitId -> retryable),
            result -> reported.put(result.unitId().value(), result));
    jupiter.execute(batch, EngineTestHarness.outerRequest(EngineExecutionListener.NOOP), listener);

    assertThat(launcherSaw).as("the launcher must have been told something").containsKey("fails");
    assertThat(reported).as("the coordinator must have been told something").containsKey(failing);
    return Map.entry(launcherSaw.get("fails"), reported.get(failing).outcome());
  }

  @Test
  void givenAFailureWithBudgetLeft_whenReported_thenTheLauncherSeesAbortedAndTheCoordinatorFailed() {
    Map.Entry<TestExecutionResult.Status, Outcome> told = failingUnitReportedTo(true);

    assertThat(told.getKey())
        .as("failsafe must stay green while a retry is still owed")
        .isEqualTo(TestExecutionResult.Status.ABORTED);
    assertThat(told.getValue())
        .as(
            "the coordinator must be told the truth: the coverage verdict counts ABORTED as"
                + " terminal-OK, so downgrading this direction turns a real failure green")
        .isEqualTo(Outcome.FAILED);
  }

  @Test
  void givenARetryableInvocationLeasedInItsOwnRight_whenItFails_thenTheLauncherStillSeesAborted() {
    // The regression this pins: resolving the owning unit by stripping to the template is
    // wrong when the coordinator distributes a method and leases each invocation on its
    // own -- the grant keeps the invocation segment, a lookup by template id misses, and
    // the downgrade silently does nothing for the mode that needs it most. The predicate
    // here is a real id set rather than a constant, which is the only way that can fail.
    String brokenRow = rowsInvocation(2);
    JupiterDelegate jupiter = new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID));
    Set<ExecutionId> leased = Set.of(new ExecutionId(brokenRow));
    TestDescriptor batch =
        jupiter.discoverIds(
            leased.stream().toList(),
            new MapConfigurationParameters(Map.of()),
            EngineTestHarness.outputDirectoryCreator());

    Map<String, TestExecutionResult.Status> launcherSaw = new HashMap<>();
    EngineExecutionListener downstream =
        new EngineExecutionListener() {
          @Override
          public void executionFinished(TestDescriptor descriptor, TestExecutionResult result) {
            if (descriptor.getUniqueId().toString().endsWith("[test-template-invocation:#2]")) {
              launcherSaw.put("row", result.getStatus());
            }
          }
        };

    Map<String, UnitResult> reported = new HashMap<>();
    Set<String> retryableIds = Set.of(brokenRow);
    UnitOutcomeListener listener =
        new UnitOutcomeListener(
            downstream,
            jupiter.nestedRootId(),
            false,
            leases(leased, retryableIds::contains),
            result -> reported.put(result.unitId().value(), result));
    jupiter.execute(batch, EngineTestHarness.outerRequest(EngineExecutionListener.NOOP), listener);

    assertThat(launcherSaw)
        .as("the failing row must have reached the launcher at all")
        .containsKey("row");
    assertThat(launcherSaw.get("row"))
        .as("a leased invocation resolves to itself, not to its template")
        .isEqualTo(TestExecutionResult.Status.ABORTED);
    assertThat(reported.get(brokenRow).outcome())
        .as("the coordinator still hears the truth")
        .isEqualTo(Outcome.FAILED);
  }

  @Test
  void givenAFailureWithNoBudgetLeft_whenReported_thenBothSidesSeeFailed() {
    Map.Entry<TestExecutionResult.Status, Outcome> told = failingUnitReportedTo(false);

    assertThat(told.getKey())
        .as("the last attempt has nothing left to recover it, so the shard must redden")
        .isEqualTo(TestExecutionResult.Status.FAILED);
    assertThat(told.getValue()).isEqualTo(Outcome.FAILED);
  }

  @Test
  void givenAStaleUnitInTheBatch_whenExecuting_thenItProducesNoOutcomeAndIsReportedUnexplained() {
    String ghost = method(PLAIN, "ghost()");
    JupiterDelegate jupiter =
        new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID));
    Set<ExecutionId> leased =
        Set.of(new ExecutionId(method(PLAIN, "passes()")), new ExecutionId(ghost));
    TestDescriptor batch =
        jupiter.discoverIds(
            leased.stream().toList(),
            new MapConfigurationParameters(Map.of()),
            EngineTestHarness.outputDirectoryCreator());
    Map<String, UnitResult> results = new HashMap<>();
    UnitOutcomeListener listener =
        new UnitOutcomeListener(
            EngineExecutionListener.NOOP,
            jupiter.nestedRootId(),
            false,
            leases(leased, unitId -> false),
            result -> results.put(result.unitId().value(), result));
    jupiter.execute(batch, EngineTestHarness.outerRequest(EngineExecutionListener.NOOP), listener);

    // The stale id was dropped in complete silence -- the real unit ran, nothing errored,
    // and the ghost produced no outcome at all. ShardLoop's reconciliation ledger is what
    // turns that silence into a NACK and a loud failure; ReconciliationIT pins it.
    assertThat(results).containsOnlyKeys(method(PLAIN, "passes()"));
  }

  /**
   * Runs the class whose {@code @BeforeAll} throws and returns the status the launcher was
   * given for the class container, with {@code retryable} deciding what the coordinator
   * promised for each unit beneath it.
   */
  private static TestExecutionResult.Status brokenSetupReportedToLauncher(
      Predicate<String> retryable) {
    Set<ExecutionId> leased =
        Set.of(
            new ExecutionId(method(BROKEN_SETUP, "first()")),
            new ExecutionId(method(BROKEN_SETUP, "second()")));
    JupiterDelegate jupiter = new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID));
    TestDescriptor batch =
        jupiter.discoverIds(
            leased.stream().toList(),
            new MapConfigurationParameters(Map.of()),
            EngineTestHarness.outputDirectoryCreator());

    Map<String, TestExecutionResult.Status> launcherSaw = new HashMap<>();
    EngineExecutionListener downstream =
        new EngineExecutionListener() {
          @Override
          public void executionFinished(TestDescriptor descriptor, TestExecutionResult result) {
            if (descriptor.getUniqueId().toString().endsWith("[class:" + BROKEN_SETUP + "]")) {
              launcherSaw.put("class", result.getStatus());
            }
          }
        };

    Map<String, UnitResult> reported = new HashMap<>();
    UnitOutcomeListener listener =
        new UnitOutcomeListener(
            downstream,
            jupiter.nestedRootId(),
            false,
            leases(leased, retryable),
            result -> reported.put(result.unitId().value(), result));
    jupiter.execute(batch, EngineTestHarness.outerRequest(EngineExecutionListener.NOOP), listener);

    // Whatever the launcher was told, the coordinator always hears the truth about both
    // units -- that is the direction the downgrade must never travel.
    assertThat(reported)
        .containsOnlyKeys(method(BROKEN_SETUP, "first()"), method(BROKEN_SETUP, "second()"));
    assertThat(reported.values()).allSatisfy(r -> assertThat(r.outcome()).isEqualTo(Outcome.FAILED));
    assertThat(launcherSaw).as("the class container must have reached the launcher").containsKey("class");
    return launcherSaw.get("class");
  }

  /**
   * The regression this pins: a {@code @BeforeAll} failure emits no leaf events at all, so
   * the class container is the only event that can carry the downgrade. Deciding per
   * descriptor rather than per explained unit left the container out, and a build reddened
   * while every unit under it was about to be retried.
   */
  @Test
  void givenABeforeAllFailureWithBudgetLeft_whenReported_thenTheLauncherSeesTheClassAborted() {
    assertThat(brokenSetupReportedToLauncher(unitId -> true))
        .as("every unit beneath is still owed a retry, so the class must not redden")
        .isEqualTo(TestExecutionResult.Status.ABORTED);
  }

  @Test
  void givenABeforeAllFailureWithOneUnitOutOfBudget_whenReported_thenTheClassStillReddens() {
    assertThat(brokenSetupReportedToLauncher(method(BROKEN_SETUP, "first()")::equals))
        .as("one unit out of budget makes the setup failure terminal for the class")
        .isEqualTo(TestExecutionResult.Status.FAILED);
  }
}
