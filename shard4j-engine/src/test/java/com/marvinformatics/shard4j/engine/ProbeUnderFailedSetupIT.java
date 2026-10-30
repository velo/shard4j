package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.tuple;

import com.marvinformatics.shard4j.protocol.ExecutionId;
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
 * A class whose setup fails takes its cardinality probe down with it, and the probe must not
 * come back FAILED: the coordinator reads any outcome on a probe as proof the position
 * exists, so every failed setup would hand out the next probe, which fails the same way in
 * milliseconds -- a growth walk with no end, recorded row by row until the CI job is killed.
 */
class ProbeUnderFailedSetupIT {

  private static final String FIXTURE = FailedSetupRowsFixture.class.getName();
  private static final String TEMPLATE =
      "[engine:junit-jupiter]/[class:" + FIXTURE + "]/[test-template:rows(java.lang.String)]";

  private static GenericContainer<?> coordinator;

  @BeforeAll
  static void seedThenStart() {
    Path dataDir = CoordinatorContainer.newDataDir();
    CoordinatorContainer.seedTemplateHistory(dataDir, TEMPLATE, Map.of(1, 30_000L, 2, 35_000L));
    coordinator = CoordinatorContainer.start(Map.of(), dataDir);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void givenAClassWhoseSetupFails_whenItsRowsAndProbeAreDrained_thenTheProbeVanishesAndTheRowsFail() {
    String sessionId = UUID.randomUUID().toString();
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                new DiscoveredCensus.ClassUnits(FIXTURE, List.of(new ExecutionId(TEMPLATE)))));

    catchThrowable(() -> shardLoop(sessionId).run(census));

    SessionView view = CoordinatorContainer.viewOf(coordinator, sessionId);
    assertThat(view.tests())
        .extracting(SessionView.TestView::testId, SessionView.TestView::state)
        .containsExactlyInAnyOrder(
            tuple(invocation(1), TestState.FAILED),
            tuple(invocation(2), TestState.FAILED));
    assertThat(view.nacks())
        .singleElement()
        .satisfies(
            nack -> {
              assertThat(nack.testId()).isEqualTo(invocation(3));
              assertThat(nack.vanished()).isTrue();
            });
  }

  private static String invocation(int position) {
    return TEMPLATE + "/[test-template-invocation:#" + position + "]";
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
