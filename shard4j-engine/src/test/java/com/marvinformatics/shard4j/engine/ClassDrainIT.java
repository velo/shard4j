package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
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
 * The class-drain cost rule, against a real coordinator whose claim batches are capped at
 * one: a class must be drained -- claimed until it yields nothing -- before its nested
 * execution runs, so a consumer whose {@code @BeforeAll} dominates pays it once per shard
 * per pass, not once per capped batch with other classes interleaved between payments.
 */
class ClassDrainIT {

  private static final String COUNTED = CountedSetupFixture.class.getName();
  private static final String ONE =
      "[engine:junit-jupiter]/[class:" + COUNTED + "]/[method:one()]";
  private static final String TWO =
      "[engine:junit-jupiter]/[class:" + COUNTED + "]/[method:two()]";
  private static final String THREE =
      "[engine:junit-jupiter]/[class:" + COUNTED + "]/[method:three()]";

  private static GenericContainer<?> coordinator;

  @BeforeAll
  static void start() {
    coordinator = CoordinatorContainer.start(Map.of("COORDINATOR_MAXCLAIMBATCH", "1"));
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void givenClaimBatchesSmallerThanTheClass_whenRunning_thenTheClassSetupRunsExactlyOnce() {
    String sessionId = UUID.randomUUID().toString();
    ShardConfiguration configuration =
        new ShardConfiguration(
            true,
            CoordinatorContainer.urlOf(coordinator),
            CoordinatorContainer.SECRET,
            sessionId,
            0,
            Pass.MAIN,
            1,
            1,
            Map.of(),
            Duration.ofSeconds(30),
            null,
            true);
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                new DiscoveredCensus.ClassUnits(
                    COUNTED,
                    List.of(new ExecutionId(ONE), new ExecutionId(TWO), new ExecutionId(THREE)))));
    CoordinatorGateway gateway = new CoordinatorGateway(configuration, census.unitIds());
    ShardLoop loop =
        new ShardLoop(
            configuration,
            new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)),
            gateway,
            EngineTestHarness.outerRequest(EngineExecutionListener.NOOP));
    CountedSetupFixture.SETUPS.set(0);

    loop.run(census);

    assertThat(CountedSetupFixture.SETUPS.get()).isEqualTo(1);
    SessionView view = CoordinatorContainer.viewOf(coordinator, sessionId);
    assertThat(stateOf(view, ONE)).isEqualTo(TestState.PASSED);
    assertThat(stateOf(view, TWO)).isEqualTo(TestState.PASSED);
    assertThat(stateOf(view, THREE)).isEqualTo(TestState.PASSED);
  }

  private static TestState stateOf(SessionView view, String testId) {
    return view.tests().stream()
        .filter(test -> test.testId().equals(testId))
        .findFirst()
        .orElseThrow()
        .state();
  }
}
