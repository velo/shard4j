package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.Outcome;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
            EngineTestHarness.outputDirectoryProvider());
    Map<String, UnitResult> results = new HashMap<>();
    UnitOutcomeListener listener =
        new UnitOutcomeListener(
            EngineExecutionListener.NOOP,
            jupiter.nestedRootId(),
            false,
            leased,
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
            EngineTestHarness.outputDirectoryProvider());
    Map<String, UnitResult> results = new HashMap<>();
    UnitOutcomeListener listener =
        new UnitOutcomeListener(
            EngineExecutionListener.NOOP,
            jupiter.nestedRootId(),
            false,
            leased,
            result -> results.put(result.unitId().value(), result));
    jupiter.execute(batch, EngineTestHarness.outerRequest(EngineExecutionListener.NOOP), listener);

    // The stale id was dropped in complete silence -- the real unit ran, nothing errored,
    // and only the listener's own ledger knows a claimed unit never happened.
    assertThat(results).containsOnlyKeys(method(PLAIN, "passes()"));
    assertThat(listener.unitsWithoutOutcome()).containsExactly(ghost);
  }
}
