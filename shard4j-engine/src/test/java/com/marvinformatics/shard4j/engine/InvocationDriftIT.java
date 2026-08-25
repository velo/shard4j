package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.UniqueId;
import org.testcontainers.containers.GenericContainer;

/**
 * Cardinality drift, end to end: history says four invocations, the fixture now has
 * three. The stale {@code #4} is handed out optimistically, JUnit materialises nothing
 * for it, and the run fails loudly naming the real cause -- the parameter set changed --
 * while the NACK corrects duration history so the very next session expands from the
 * shrunken plan and runs green. Fixture files change on most commits that touch them, so
 * this path is routine, and its log lines have to read as "the parameters changed", never
 * as an unexplained engine bug.
 */
class InvocationDriftIT {

  private static final String FIXTURE = DriftRowsFixture.class.getName();
  private static final String TEMPLATE =
      "[engine:junit-jupiter]/[class:" + FIXTURE + "]/[test-template:rows(java.lang.String)]";

  private static GenericContainer<?> coordinator;

  @BeforeAll
  static void seedThenStart() {
    Path dataDir = CoordinatorContainer.newDataDir();
    CoordinatorContainer.seedTemplateHistory(
        dataDir, TEMPLATE, Map.of(1, 30_000L, 2, 35_000L, 3, 45_000L, 4, 55_000L));
    coordinator = CoordinatorContainer.start(Map.of(), dataDir);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void givenAShrunkenParameterSet_whenTheStalePositionIsHandedOut_thenTheRunFailsNamingTheDriftAndTheNextSessionHeals() {
    String sessionId = UUID.randomUUID().toString();
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                new DiscoveredCensus.ClassUnits(FIXTURE, List.of(new ExecutionId(TEMPLATE)))));

    // The drifted run: three rows run, the stale #4 materialises nothing, and the shard
    // fails naming the parameter-set change -- never the probe, whose vanishing at #5 is
    // the expected answer.
    assertThatThrownBy(() -> shardLoop(sessionId).run(census))
        .isInstanceOf(ShardExecutionException.class)
        .hasMessageContaining("the parameter set changed since they were last measured")
        .hasMessageContaining(invocation(4))
        .satisfies(
            failure -> assertThat(failure.getMessage()).doesNotContain(invocation(5)));

    SessionView view = CoordinatorContainer.viewOf(coordinator, sessionId);
    // The stale position stays loud in this session -- still registered, still PENDING --
    // while the probe left the census quietly.
    assertThat(stateOf(view, invocation(4))).isEqualTo(TestState.PENDING);
    assertThat(view.tests().stream().map(SessionView.TestView::testId))
        .doesNotContain(invocation(5));
    assertThat(view.nacks())
        .anySatisfy(
            nack -> {
              assertThat(nack.testId()).isEqualTo(invocation(4));
              assertThat(nack.reason())
                  .contains("the parameter set changed since this invocation was last measured");
            });

    // The heal: the vanished NACK dropped #4 from history, so a fresh session expands to
    // the three real positions (probing #4, which vanishes quietly) and runs green.
    String healedSession = UUID.randomUUID().toString();
    shardLoop(healedSession).run(census);
    SessionView healed = CoordinatorContainer.viewOf(coordinator, healedSession);
    assertThat(healed.tests().stream().map(SessionView.TestView::testId))
        .containsExactlyInAnyOrder(invocation(1), invocation(2), invocation(3));
    assertThat(healed.tests())
        .allSatisfy(test -> assertThat(test.state()).isEqualTo(TestState.PASSED));
  }

  private static String invocation(int position) {
    return TEMPLATE + "/[test-template-invocation:#" + position + "]";
  }

  private static TestState stateOf(SessionView view, String testId) {
    return view.tests().stream()
        .filter(test -> test.testId().equals(testId))
        .findFirst()
        .orElseThrow()
        .state();
  }

  private static ShardLoop shardLoop(String sessionId) {
    ShardConfiguration configuration =
        ShardConfigurationBuilder.coordinatedShard(
                CoordinatorContainer.urlOf(coordinator), sessionId)
            .shardCount(1)
            .build();
    return new ShardLoop(
        configuration,
        new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)),
        new CoordinatorGateway(configuration, List.of(TEMPLATE)),
        EngineTestHarness.outerRequest(EngineExecutionListener.NOOP));
  }
}
