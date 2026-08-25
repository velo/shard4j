package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.UniqueId;
import org.testcontainers.containers.GenericContainer;

/**
 * In-shard parallelism, against a real coordinator: with two drain slots, two single-leaf
 * classes must genuinely run at the same time -- each test refuses to finish until the
 * other has started, so a strictly serial engine times the rendezvous out and fails,
 * while overlapping execution windows are the observed proof, never wall-clock speedup.
 * The default single slot must keep today's behaviour exactly: two classes, two disjoint
 * windows, in coordinator order.
 */
class ConcurrentClassesIT {

  private static final String ALPHA = RendezvousAlphaFixture.class.getName();
  private static final String BETA = RendezvousBetaFixture.class.getName();
  private static final String SOLO_ALPHA = SoloAlphaFixture.class.getName();
  private static final String SOLO_BETA = SoloBetaFixture.class.getName();

  private static GenericContainer<?> coordinator;

  @BeforeAll
  static void start() {
    coordinator = CoordinatorContainer.start();
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void givenTwoDrainSlots_whenTwoClassesAreClaimable_thenTheirExecutionsOverlapInTime() {
    String sessionId = UUID.randomUUID().toString();
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                classUnits(ALPHA, "meets"),
                classUnits(BETA, "meets")));
    ConcurrencyProbe.reset(2);

    shardLoop(configuration(sessionId, 2), census).run(census);

    SessionView view = CoordinatorContainer.viewOf(coordinator, sessionId);
    assertThat(stateOf(view, methodId(ALPHA, "meets"))).isEqualTo(TestState.PASSED);
    assertThat(stateOf(view, methodId(BETA, "meets"))).isEqualTo(TestState.PASSED);
    assertThat(
            ConcurrencyProbe.overlapped(
                "RendezvousAlphaFixture#meets", "RendezvousBetaFixture#meets"))
        .as("the two classes' execution windows must overlap")
        .isTrue();
  }

  @Test
  void givenTheDefaultSingleSlot_whenTwoClassesRun_thenTheirExecutionsNeverOverlap() {
    String sessionId = UUID.randomUUID().toString();
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                classUnits(SOLO_ALPHA, "occupies"),
                classUnits(SOLO_BETA, "occupies")));
    ConcurrencyProbe.reset(0);

    shardLoop(configuration(sessionId, 1), census).run(census);

    SessionView view = CoordinatorContainer.viewOf(coordinator, sessionId);
    assertThat(stateOf(view, methodId(SOLO_ALPHA, "occupies"))).isEqualTo(TestState.PASSED);
    assertThat(stateOf(view, methodId(SOLO_BETA, "occupies"))).isEqualTo(TestState.PASSED);
    assertThat(
            ConcurrencyProbe.overlapped(
                "SoloAlphaFixture#occupies", "SoloBetaFixture#occupies"))
        .as("one slot must keep classes strictly serial")
        .isFalse();
  }

  private static ShardConfiguration configuration(String sessionId, int concurrency) {
    return ShardConfigurationBuilder.coordinatedShard(
            CoordinatorContainer.urlOf(coordinator), sessionId)
        .concurrency(concurrency)
        .build();
  }

  private static ShardLoop shardLoop(ShardConfiguration configuration, DiscoveredCensus census) {
    return new ShardLoop(
        configuration,
        new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)),
        new CoordinatorGateway(configuration, census.unitIds()),
        EngineTestHarness.outerRequest(EngineExecutionListener.NOOP));
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

  private static TestState stateOf(SessionView view, String testId) {
    return view.tests().stream()
        .filter(test -> test.testId().equals(testId))
        .findFirst()
        .orElseThrow()
        .state();
  }
}
