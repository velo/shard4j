package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.UniqueId;
import org.testcontainers.containers.GenericContainer;

/**
 * The headline property, observed end to end: with durations seeded before first boot,
 * the order a shard actually executes units in must be descending by duration across
 * classes, not merely inside each class. The class names are deliberately alphabetical in
 * the opposite direction of their durations, so an engine that sweeps its census
 * alphabetically fails here on its very first unit.
 */
class CrossClassOrderIT {

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

  @BeforeEach
  void resetJournals() {
    OrderProbeRecorder.reset();
    CountedSetupFixture.SETUPS.set(0);
  }

  @Test
  void givenSeededDurationsAcrossClasses_whenOneShardRunsThePass_thenUnitsRunSlowestFirstAcrossClasses() {
    ShardConfiguration configuration = configuration(UUID.randomUUID().toString());
    // Census in alphabetical class order, exactly as discovery would hand it over.
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                classUnits(FAST, "fast", "faster"),
                classUnits(MID, "mid", "milder"),
                classUnits(SLOW, "slowest", "slower")));

    shardLoop(configuration, census).run(census);

    assertThat(OrderProbeRecorder.EVENTS)
        .containsExactly(
            "SlowProbeFixture#slowest",
            "SlowProbeFixture#slower",
            "MidProbeFixture#mid",
            "MidProbeFixture#milder",
            "FastProbeFixture#fast",
            "FastProbeFixture#faster");
  }

  @Test
  void givenAClassFullyClaimedByAnotherShard_whenThisShardRunsItsPass_thenThatClassCostsNoSetup() {
    String sessionId = UUID.randomUUID().toString();
    String counted = CountedSetupFixture.class.getName();
    List<String> countedUnits =
        List.of(methodId(counted, "one"), methodId(counted, "two"), methodId(counted, "three"));
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                classUnits(counted, "one", "two", "three"),
                classUnits(FAST, "fast", "faster")));

    // Another shard takes the whole counted class and reports it before this shard runs.
    CoordinatorClient otherShard = CoordinatorContainer.shardApiOf(coordinator);
    otherShard.register(sessionId, new RegisterRequest(1, 1, Map.of(), census.unitIds()));
    List<Grant> taken =
        otherShard.claim(sessionId, new ClaimRequest(1, Pass.MAIN, counted, countedUnits)).granted();
    assertThat(taken).hasSize(3);
    for (Grant grant : taken) {
      otherShard.result(
          sessionId,
          new ResultRequest(
              1, Pass.MAIN, grant.testId(), grant.fence(), Outcome.PASSED, 50, false, null, null));
    }

    ShardConfiguration configuration = configuration(sessionId);
    shardLoop(configuration, census).run(census);

    // The coordinator had nothing left in the counted class, so this shard never entered
    // it: no nested discovery, no class initialiser, no @BeforeAll.
    assertThat(CountedSetupFixture.SETUPS.get()).isZero();
    assertThat(OrderProbeRecorder.EVENTS)
        .containsExactly("FastProbeFixture#fast", "FastProbeFixture#faster");
  }

  private static ShardConfiguration configuration(String sessionId) {
    return new ShardConfiguration(
        true,
        CoordinatorContainer.urlOf(coordinator),
        CoordinatorContainer.SECRET,
        sessionId,
        0,
        Pass.MAIN,
        1,
        Map.of(),
        Duration.ofSeconds(30),
        null,
        true);
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
        List.of(methods).stream().map(method -> new ExecutionId(methodId(className, method))).toList();
    return new DiscoveredCensus.ClassUnits(className, units);
  }

  private static String methodId(String className, String method) {
    return "[engine:junit-jupiter]/[class:" + className + "]/[method:" + method + "()]";
  }
}
