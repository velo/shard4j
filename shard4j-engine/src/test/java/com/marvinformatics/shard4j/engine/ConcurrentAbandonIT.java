package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.UniqueId;
import org.testcontainers.containers.GenericContainer;

/**
 * The mid-pass abnormal exit with two drain slots in flight, against a real coordinator:
 * each slot holds a ghost lease its nested execution can never explain, the rendezvous
 * proves both slots held work concurrently, and the transport dies on the next open ask
 * -- so the shared failure path must NACK both slots' outstanding leases back to the
 * pool, never just the failing slot's, and never abandon either to the TTL.
 */
class ConcurrentAbandonIT {

  private static final String ALPHA = RendezvousAlphaFixture.class.getName();
  private static final String BETA = RendezvousBetaFixture.class.getName();
  private static final String ALPHA_MEETS = methodId(ALPHA, "meets");
  private static final String BETA_MEETS = methodId(BETA, "meets");
  private static final String ALPHA_GHOST = methodId(ALPHA, "ghost");
  private static final String BETA_GHOST = methodId(BETA, "ghost");

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
  void givenTwoSlotsEachHoldingALease_whenTheTransportDies_thenBothOutstandingLeasesAreNacked() {
    String sessionId = UUID.randomUUID().toString();
    ShardConfiguration configuration =
        ShardConfigurationBuilder.coordinatedShard(
            CoordinatorContainer.urlOf(coordinator), sessionId)
        .concurrency(2)
        .build();
    // Each class carries a ghost the nested discovery silently drops, so each slot still
    // holds one unexplained lease when the transport dies on the next ask.
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                new DiscoveredCensus.ClassUnits(
                    ALPHA, List.of(new ExecutionId(ALPHA_MEETS), new ExecutionId(ALPHA_GHOST))),
                new DiscoveredCensus.ClassUnits(
                    BETA, List.of(new ExecutionId(BETA_MEETS), new ExecutionId(BETA_GHOST)))));
    AtomicInteger openAsks = new AtomicInteger();
    CoordinatorGateway gateway =
        new CoordinatorGateway(configuration, census.unitIds()) {
          @Override
          synchronized NextClassResponse nextClass() {
            if (openAsks.incrementAndGet() > 2) {
              throw new IllegalStateException("simulated transport failure: retry budget spent");
            }
            return super.nextClass();
          }
        };
    ShardLoop loop =
        new ShardLoop(
            configuration,
            new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)),
            gateway,
            EngineTestHarness.outerRequest(EngineExecutionListener.NOOP));
    ConcurrencyProbe.reset(2);

    assertThatThrownBy(() -> loop.run(census))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("simulated transport failure");

    // The rendezvous passed, so both slots really were in flight at once.
    assertThat(
            ConcurrencyProbe.overlapped(
                "RendezvousAlphaFixture#meets", "RendezvousBetaFixture#meets"))
        .isTrue();
    SessionView view = CoordinatorContainer.viewOf(coordinator, sessionId);
    assertThat(stateOf(view, ALPHA_MEETS)).isEqualTo(TestState.PASSED);
    assertThat(stateOf(view, BETA_MEETS)).isEqualTo(TestState.PASSED);
    // Both ghosts claimable again right now, not LEASED for the remaining TTL.
    assertThat(stateOf(view, ALPHA_GHOST)).isEqualTo(TestState.PENDING);
    assertThat(stateOf(view, BETA_GHOST)).isEqualTo(TestState.PENDING);
    assertThat(view.nacks())
        .anyMatch(nack -> nack.testId().equals(ALPHA_GHOST) && nack.reason().contains("Abandoned"));
    assertThat(view.nacks())
        .anyMatch(nack -> nack.testId().equals(BETA_GHOST) && nack.reason().contains("Abandoned"));
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
