package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * MUST 3 of the design, against a real coordinator: a stale claimed id is dropped by the
 * nested discovery in complete silence -- no event, no error, clean exit -- so the pass
 * epilogue is the only thing standing between a renamed test and a green run that skipped
 * it. The unexplained lease must be NACKed back to the pool with a reason and the shard
 * must fail naming the id, never exit clean.
 */
class ReconciliationIT {

  private static final String PLAIN = PlainShapesFixture.class.getName();
  private static final String REAL =
      "[engine:junit-jupiter]/[class:" + PLAIN + "]/[method:passes()]";
  private static final String GHOST =
      "[engine:junit-jupiter]/[class:" + PLAIN + "]/[method:ghost()]";

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
  void givenAClaimedIdThatNoLongerResolves_whenThePassEnds_thenItIsNackedAndTheShardFailsNamingIt() {
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
                    PLAIN, List.of(new ExecutionId(REAL), new ExecutionId(GHOST)))));
    CoordinatorGateway gateway = new CoordinatorGateway(configuration, census.unitIds());
    ShardLoop loop =
        new ShardLoop(
            configuration,
            new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)),
            gateway,
            EngineTestHarness.outerRequest(EngineExecutionListener.NOOP));

    assertThatThrownBy(() -> loop.run(census))
        .isInstanceOf(ShardExecutionException.class)
        .hasMessageContaining(GHOST)
        .hasMessageContaining("NACKed");

    SessionView view = CoordinatorContainer.viewOf(coordinator, sessionId);
    assertThat(stateOf(view, REAL)).isEqualTo(TestState.PASSED);
    // NACKed, not stuck LEASED and not silently absorbed: the unit is claimable again.
    assertThat(stateOf(view, GHOST)).isEqualTo(TestState.PENDING);
    assertThat(view.nacks())
        .anyMatch(
            nack -> nack.testId().equals(GHOST) && nack.reason().contains("terminal outcome"));
  }

  private static TestState stateOf(SessionView view, String testId) {
    return view.tests().stream()
        .filter(test -> test.testId().equals(testId))
        .findFirst()
        .orElseThrow()
        .state();
  }
}
