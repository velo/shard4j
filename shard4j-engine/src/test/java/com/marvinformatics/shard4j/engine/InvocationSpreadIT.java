package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.nio.file.Path;
import java.time.Duration;
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
 * The headline property of invocation distribution, end to end through the real engine
 * against the real coordinator container: a parameterized method whose per-invocation
 * durations are on record stops being one indivisible block -- two shards run its rows
 * between them, each shard's nested Jupiter execution materialising only the rows it was
 * granted. Before this feature the whole method leased to whichever shard asked first
 * and the second shard ran nothing of it.
 */
class InvocationSpreadIT {

  private static final String FIXTURE = SplitRowsFixture.class.getName();
  private static final String TEMPLATE =
      "[engine:junit-jupiter]/[class:" + FIXTURE + "]/[test-template:rows(java.lang.String)]";

  private static GenericContainer<?> coordinator;

  @BeforeAll
  static void seedThenStart() {
    Path dataDir = CoordinatorContainer.newDataDir();
    CoordinatorContainer.seedTemplateHistory(
        dataDir, TEMPLATE, Map.of(1, 40_000L, 2, 50_000L, 3, 60_000L, 4, 70_000L));
    coordinator = CoordinatorContainer.start(Map.of(), dataDir);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void givenRecordedInvocationDurations_whenTwoShardsRunThePass_thenOneMethodsRowsLandOnBothShards()
      throws InterruptedException {
    String sessionId = UUID.randomUUID().toString();
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                new DiscoveredCensus.ClassUnits(FIXTURE, List.of(new ExecutionId(TEMPLATE)))));

    Thread shard0 = shardThread(sessionId, 0, census);
    Thread shard1 = shardThread(sessionId, 1, census);
    shard0.start();
    shard1.start();
    shard0.join();
    shard1.join();

    SessionView view = CoordinatorContainer.viewOf(coordinator, sessionId);
    List<SessionView.TestView> invocations =
        view.tests().stream()
            .filter(test -> test.testId().startsWith(TEMPLATE + "/"))
            .toList();
    // The census registered one method-level unit; the coordinator handed back its four
    // recorded positions individually (the probe at #5 vanished and left the census).
    assertThat(invocations).hasSize(4);
    assertThat(invocations)
        .allSatisfy(test -> assertThat(test.state()).isEqualTo(TestState.PASSED));
    List<Integer> shardsThatRan =
        invocations.stream()
            .flatMap(test -> test.records().stream())
            .map(SessionView.RecordView::shard)
            .distinct()
            .sorted()
            .toList();
    assertThat(shardsThatRan).containsExactly(0, 1);
  }

  private static Thread shardThread(String sessionId, int shardIndex, DiscoveredCensus census) {
    ShardConfiguration configuration = configuration(sessionId, shardIndex, Pass.MAIN);
    ShardLoop loop =
        new ShardLoop(
            configuration,
            new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)),
            new CoordinatorGateway(configuration, census.unitIds()),
            EngineTestHarness.outerRequest(EngineExecutionListener.NOOP));
    return new Thread(() -> loop.run(census), "spread-shard-" + shardIndex);
  }

  private static ShardConfiguration configuration(String sessionId, int shardIndex, Pass pass) {
    return new ShardConfiguration(
        true,
        CoordinatorContainer.urlOf(coordinator),
        CoordinatorContainer.SECRET,
        sessionId,
        shardIndex,
        pass,
        1,
        1,
        2,
        Map.of(),
        Duration.ofSeconds(30),
        null,
        true);
  }
}
