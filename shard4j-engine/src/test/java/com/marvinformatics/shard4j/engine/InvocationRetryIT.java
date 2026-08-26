package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.Outcome;
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
 * Retry accounting for a split method: the invocations spread over two shards in MAIN,
 * one row fails, and RETRY1 re-hands exactly that position -- one unit, never the whole
 * method -- to whichever unreleased shard asks first, which pays its own class setup to
 * run it. Which shard that is the barrier decides: with one retry unit and two waiters it
 * releases the excess waiter, so the retry may land on either index --
 * {@code InvocationDistributionIT} pins the cross-shard case deterministically at the
 * wire. The row that flaked ends PASSED with a MAIN failure and a RETRY1 pass on its
 * record, its siblings never re-run, and the coverage verdict counts every position
 * terminal.
 */
class InvocationRetryIT {

  private static final String FIXTURE = FlakyRowFixture.class.getName();
  private static final String TEMPLATE =
      "[engine:junit-jupiter]/[class:" + FIXTURE + "]/[test-template:rows(java.lang.String)]";

  private static GenericContainer<?> coordinator;

  @BeforeAll
  static void seedThenStart() {
    Path dataDir = CoordinatorContainer.newDataDir();
    CoordinatorContainer.seedTemplateHistory(
        dataDir, TEMPLATE, Map.of(1, 20_000L, 2, 30_000L, 3, 40_000L, 4, 50_000L));
    coordinator = CoordinatorContainer.start(Map.of(), dataDir);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void givenAFailedInvocationOfASplitMethod_whenItIsRequeued_thenOnlyThatPositionRetries()
      throws InterruptedException {
    String sessionId = UUID.randomUUID().toString();
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                new DiscoveredCensus.ClassUnits(FIXTURE, List.of(new ExecutionId(TEMPLATE)))));

    // One run, not two. The failed position is requeued the moment it is reported, so a
    // shard still draining picks it up here -- there is no second pass to enter, and by
    // the time both shards have finished nothing is left in FAILED for a later pass to
    // find. A test that looked for one would find none, which is how this one used to end.
    Thread shard0 = shardThread(sessionId, 0, census);
    Thread shard1 = shardThread(sessionId, 1, census);
    shard0.start();
    shard1.start();
    shard0.join();
    shard1.join();

    SessionView view = CoordinatorContainer.viewOf(coordinator, sessionId);
    SessionView.TestView retried =
        view.tests().stream()
            .filter(test -> test.records().size() > 1)
            .reduce(
                (a, b) -> {
                  throw new AssertionError("exactly one invocation should have been retried");
                })
            .orElseThrow(() -> new AssertionError("no invocation was retried at all"));

    assertThat(retried.state()).isEqualTo(TestState.PASSED);
    assertThat(retried.records())
        .extracting(SessionView.RecordView::attempt, SessionView.RecordView::outcome)
        .containsExactly(tuple(1, Outcome.FAILED), tuple(2, Outcome.PASSED));

    // Its siblings were not re-run, and every position is terminal: full coverage.
    view.tests().stream()
        .filter(test -> !test.testId().equals(retried.testId()))
        .forEach(
            test -> {
              assertThat(test.state()).isEqualTo(TestState.PASSED);
              assertThat(test.records()).hasSize(1);
            });
    long terminal = view.tests().stream().filter(test -> test.state().isAbsorbing()).count();
    assertThat(terminal).isEqualTo(view.registeredCount());
  }

  private static Thread shardThread(
      String sessionId, int shardIndex, DiscoveredCensus census) {
    ShardConfiguration configuration =
        ShardConfigurationBuilder.coordinatedShard(
                CoordinatorContainer.urlOf(coordinator), sessionId)
            .shardIndex(shardIndex)
            .shardCount(2)
            .build();
    ShardLoop loop =
        new ShardLoop(
            configuration,
            new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)),
            new CoordinatorGateway(configuration, census.unitIds()),
            EngineTestHarness.outerRequest(EngineExecutionListener.NOOP));
    return new Thread(() -> loop.run(census), "retry-shard-" + shardIndex);
  }
}
