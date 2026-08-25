package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
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
 * Cross-class slowest-first must survive two drain slots: because a class is fully leased
 * before the next open ask, the second slot's ask ranks the remaining pool and receives
 * the next-slowest class -- never whatever class happens to sit next to the first one,
 * and never a slice of the class the first slot is already draining.
 */
class ConcurrentOrderIT {

  private static final String FAST = FastProbeFixture.class.getName();
  private static final String MID = MidProbeFixture.class.getName();
  private static final String SLOW = SlowProbeFixture.class.getName();

  private static GenericContainer<?> coordinator;

  @BeforeAll
  static void seedThenStart() {
    Path dataDir = CoordinatorContainer.newDataDir();
    CoordinatorContainer.seedHistory(
        dataDir,
        Map.of(
            methodId(SLOW, "slowest"), 300_000L,
            methodId(SLOW, "slower"), 250_000L,
            methodId(MID, "mid"), 120_000L,
            methodId(MID, "milder"), 100_000L,
            methodId(FAST, "fast"), 2_000L,
            methodId(FAST, "faster"), 1_000L));
    coordinator = CoordinatorContainer.start(Map.of(), dataDir);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void givenTwoDrainSlots_whenAsksInterleave_thenEachAskNamesTheSlowestRemainingClass() {
    String sessionId = UUID.randomUUID().toString();
    ShardConfiguration configuration =
        ShardConfigurationBuilder.coordinatedShard(
            CoordinatorContainer.urlOf(coordinator), sessionId)
        .concurrency(2)
        .build();
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                classUnits(FAST, "fast", "faster"),
                classUnits(MID, "mid", "milder"),
                classUnits(SLOW, "slowest", "slower")));
    List<String> askedClasses = Collections.synchronizedList(new ArrayList<>());
    CoordinatorGateway gateway =
        new CoordinatorGateway(configuration, census.unitIds()) {
          @Override
          synchronized NextClassResponse nextClass() {
            NextClassResponse next = super.nextClass();
            if (!next.granted().isEmpty()) {
              askedClasses.add(next.className());
            }
            return next;
          }
        };
    OrderProbeRecorder.reset();

    new ShardLoop(
            configuration,
            new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)),
            gateway,
            EngineTestHarness.outerRequest(EngineExecutionListener.NOOP))
        .run(census);

    assertThat(askedClasses).containsExactly(SLOW, MID, FAST);
    assertThat(OrderProbeRecorder.EVENTS)
        .containsExactlyInAnyOrder(
            "SlowProbeFixture#slowest",
            "SlowProbeFixture#slower",
            "MidProbeFixture#mid",
            "MidProbeFixture#milder",
            "FastProbeFixture#fast",
            "FastProbeFixture#faster");
  }

  private static DiscoveredCensus.ClassUnits classUnits(String className, String... methods) {
    List<ExecutionId> units =
        List.of(methods).stream()
            .map(method -> new ExecutionId(methodId(className, method)))
            .toList();
    return new DiscoveredCensus.ClassUnits(className, units);
  }

  private static String methodId(String className, String method) {
    return "[engine:junit-jupiter]/[class:" + className + "]/[method:" + method + "()]";
  }
}
