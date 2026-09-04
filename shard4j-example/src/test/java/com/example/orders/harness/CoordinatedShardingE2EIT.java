package com.example.orders.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import com.example.orders.PingResourceIT;
import com.example.orders.fixtures.CatalogSearchIT;
import com.example.orders.fixtures.CheckoutSetupIT;
import com.example.orders.fixtures.FlakyGatewayIT;
import com.example.orders.fixtures.InventoryAuditIT;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestExecutionResult;
import org.testcontainers.containers.GenericContainer;

/**
 * The headline acceptance run: plain, parameterized, disabled, in-body abort and
 * {@code @BeforeAll} abort fixtures across three simulated shards against a real
 * coordinator container -- census registered, units claimed and executed, results
 * reported, barrier honoured, and the coverage verdict satisfiable from the read surface.
 */
@Tag("shard4j-harness")
public class CoordinatedShardingE2EIT {

  private static final List<Class<?>> SUITE =
      List.of(
          PingResourceIT.class,
          InventoryAuditIT.class,
          CheckoutSetupIT.class,
          FlakyGatewayIT.class,
          CatalogSearchIT.class);

  private static GenericContainer<?> coordinator;
  private static String url;

  // A container per test, not per class: the coordinator records per-invocation
  // durations, and a session that completes a parameterized method leaves the next
  // session a distribution plan for it. Every assertion here is about the cold,
  // first-contact behaviour -- a template leasing whole included -- so each test gets a
  // coordinator that has never seen the suite.
  @BeforeEach
  void start() {
    // The flaky fixture's counter is JVM-wide and shared with the other harness
    // tests; without this the first one to run spends the failure the others need.
    FlakyGatewayIT.resetAttempts();
    coordinator = ShardingHarness.startCoordinator();
    url = ShardingHarness.urlOf(coordinator);
  }

  @AfterEach
  void stop() {
    coordinator.stop();
  }

  @Test
  void givenThreeShards_whenTheSuiteDrains_thenEveryUnitReachesOneTerminalNonFailingState()
      throws Exception {
    String sessionId = UUID.randomUUID().toString();
    ExecutorService shards = Executors.newFixedThreadPool(3);
    try {
      List<Future<List<ShardingHarness.ShardRun>>> futures =
          List.of(
              shards.submit(() -> runAllPasses(sessionId, 0)),
              shards.submit(() -> runAllPasses(sessionId, 1)),
              shards.submit(() -> runAllPasses(sessionId, 2)));
      for (Future<List<ShardingHarness.ShardRun>> future : futures) {
        for (ShardingHarness.ShardRun run : future.get(5, TimeUnit.MINUTES)) {
          assertThat(run.engineResult().getStatus())
              .as("no shard may fail at the engine level")
              .isEqualTo(TestExecutionResult.Status.SUCCESSFUL);
        }
      }
    } finally {
      shards.shutdownNow();
    }

    SessionView view = ShardingHarness.viewOf(coordinator, sessionId);
    assertThat(view.registeredCount()).isEqualTo(9);
    assertThat(view.tests())
        .allSatisfy(
            test ->
                assertThat(test.state())
                    .as("unit %s must reach a terminal non-failing state", test.testId())
                    .isIn(TestState.PASSED, TestState.SKIPPED, TestState.ABORTED));

    // The disabled leaf is indistinguishable at discovery, so it is in the census and
    // reports SKIPPED with its reason.
    SessionView.TestView disabled = unit(view, "reconcilesLedger()");
    assertThat(disabled.state()).isEqualTo(TestState.SKIPPED);
    assertThat(disabled.reason()).contains("ledger reconciliation");

    // Abort shape 1: the class container aborted in @BeforeAll and emitted nothing for
    // its leaves; both must still be explained, with the container's reason.
    for (String checkout : List.of("authorisesCard()", "capturesFunds()")) {
      SessionView.TestView aborted = unit(view, checkout);
      assertThat(aborted.state()).isEqualTo(TestState.ABORTED);
      assertThat(aborted.reason()).contains("payment sandbox");
    }

    // Abort shape 2: the leaf started and then aborted in its body.
    SessionView.TestView inBody = unit(view, "needsLocalWarehouse()");
    assertThat(inBody.state()).isEqualTo(TestState.ABORTED);
    assertThat(inBody.reason()).contains("warehouse service");

    // The failure was requeued and taken again -- attempt 1 failed, attempt 2 passed.
    SessionView.TestView flaky = unit(view, "retriesAgainstTheGateway()");
    assertThat(flaky.state()).isEqualTo(TestState.PASSED);
    assertThat(flaky.records())
        .extracting(SessionView.RecordView::attempt, SessionView.RecordView::outcome)
        .containsExactly(tuple(1, Outcome.FAILED), tuple(2, Outcome.PASSED));

    // The parameterized method leased and reported as one unit.
    SessionView.TestView template = unit(view, "findsProducts(java.lang.String)");
    assertThat(template.testId()).contains("[test-template:");
    assertThat(template.state()).isEqualTo(TestState.PASSED);
  }

  @Test
  void givenAShardThatAlreadyDrained_whenItRunsAgain_thenItCostsOnlyDiscoveryAndExecutesNothing() {
    String sessionId = UUID.randomUUID().toString();
    List<Class<?>> suite = List.of(PingResourceIT.class, CatalogSearchIT.class);
    ShardingHarness.ShardRun main = ShardingHarness.runShard(url, sessionId, 0, suite);
    assertThat(main.engineResult().getStatus()).isEqualTo(TestExecutionResult.Status.SUCCESSFUL);
    assertThat(main.startedTests()).isNotEmpty();

    ShardingHarness.ShardRun retry = ShardingHarness.runShard(url, sessionId, 0, suite);

    assertThat(retry.engineResult().getStatus()).isEqualTo(TestExecutionResult.Status.SUCCESSFUL);
    assertThat(retry.startedTests())
        .as("an empty-claim pass must execute nothing at all")
        .isEmpty();
    SessionView view = ShardingHarness.viewOf(coordinator, sessionId);
    assertThat(view.tests())
        .allSatisfy(test -> assertThat(test.records()).hasSize(1));
  }

  @Test
  void givenADivergentCensusOnRejoin_whenRegistering_thenTheShardFailsLoudlyNamingTheIds() {
    String sessionId = UUID.randomUUID().toString();
    ShardingHarness.ShardRun first =
        ShardingHarness.runShard(
            url, sessionId, 0, List.of(PingResourceIT.class, CatalogSearchIT.class));
    assertThat(first.engineResult().getStatus()).isEqualTo(TestExecutionResult.Status.SUCCESSFUL);

    // A second shard whose discovery produced a different set: the coordinator refuses
    // and the engine surfaces the refusal as an engine-level failure naming the ids.
    ShardingHarness.ShardRun divergent =
        ShardingHarness.runShard(url, sessionId, 1, List.of(PingResourceIT.class));

    assertThat(divergent.engineResult().getStatus()).isEqualTo(TestExecutionResult.Status.FAILED);
    assertThat(divergent.engineResult().getThrowable().orElseThrow().getMessage())
        .contains("mismatch")
        .contains("findsProducts");
  }

  /**
   * One run, where there used to be three. A requeued failure becomes claimable again
   * inside the same drain, so a shard no longer needs an outer pass loop to pick up its
   * own retries -- if this ever needs a second call to go green, the requeue is broken.
   */
  private static List<ShardingHarness.ShardRun> runAllPasses(String sessionId, int shard) {
    return List.of(ShardingHarness.runShard(url, sessionId, shard, SUITE));
  }

  private static SessionView.TestView unit(SessionView view, String methodSuffix) {
    return view.tests().stream()
        .filter(test -> test.testId().endsWith(":" + methodSuffix + "]"))
        .findFirst()
        .orElseThrow(
            () -> new AssertionError("no census unit ends with " + methodSuffix));
  }

}
