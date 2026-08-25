package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import com.marvinformatics.shard4j.protocol.Grant;
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
 * The mid-pass abnormal exit, against a real coordinator: when the loop dies between
 * claiming and reconciling -- a transport whose retry budget is exhausted, an engine bug
 * -- any lease still unexplained must be NACKed back to the pool on the way out, never
 * abandoned to the lease TTL. The alternative is every healthy shard sitting at the
 * barrier waiting out {@code earliest_lease_expiry} because one shard crashed.
 */
class AbandonedLeasesIT {

  private static final String PLAIN = PlainShapesFixture.class.getName();
  private static final String ROWS = RowsFixture.class.getName();
  private static final String PASSES =
      "[engine:junit-jupiter]/[class:" + PLAIN + "]/[method:passes()]";
  private static final String GHOST =
      "[engine:junit-jupiter]/[class:" + PLAIN + "]/[method:ghost()]";
  private static final String TEMPLATE =
      "[engine:junit-jupiter]/[class:" + ROWS + "]/[test-template:rows(java.lang.String)]";

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
  void givenATransportDeathMidPass_whenTheLoopDies_thenOutstandingLeasesAreNackedNotLeftToTheTtl() {
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
            Map.of(),
            Duration.ofSeconds(30),
            null,
            true);
    // The ghost never produces an outcome, so its lease is still outstanding when the
    // next class's claim dies -- the exact state a mid-pass abnormal exit strands.
    DiscoveredCensus census =
        DiscoveredCensus.of(
            List.of(
                new DiscoveredCensus.ClassUnits(
                    PLAIN, List.of(new ExecutionId(PASSES), new ExecutionId(GHOST))),
                new DiscoveredCensus.ClassUnits(ROWS, List.of(new ExecutionId(TEMPLATE)))));
    CoordinatorGateway gateway =
        new CoordinatorGateway(configuration, census.unitIds()) {
          @Override
          synchronized List<Grant> claim(String className, List<String> candidates) {
            if (className.equals(ROWS)) {
              throw new IllegalStateException("simulated transport failure: retry budget spent");
            }
            return super.claim(className, candidates);
          }
        };
    ShardLoop loop =
        new ShardLoop(
            configuration,
            new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)),
            gateway,
            EngineTestHarness.outerRequest(EngineExecutionListener.NOOP));

    // The transport failure itself surfaces -- not a reconciliation message: the loop
    // died before its epilogue, and the NACK below happened on the way out.
    assertThatThrownBy(() -> loop.run(census))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("simulated transport failure");

    SessionView view = CoordinatorContainer.viewOf(coordinator, sessionId);
    assertThat(stateOf(view, PASSES)).isEqualTo(TestState.PASSED);
    // Claimable again right now, not LEASED for the remaining TTL.
    assertThat(stateOf(view, GHOST)).isEqualTo(TestState.PENDING);
    assertThat(view.nacks())
        .anyMatch(nack -> nack.testId().equals(GHOST) && nack.reason().contains("Abandoned"));
  }

  private static TestState stateOf(SessionView view, String testId) {
    return view.tests().stream()
        .filter(test -> test.testId().equals(testId))
        .findFirst()
        .orElseThrow()
        .state();
  }
}
