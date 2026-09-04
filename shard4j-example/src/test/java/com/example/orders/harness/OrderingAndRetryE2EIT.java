package com.example.orders.harness;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.orders.fixtures.AlwaysFailsIT;
import com.example.orders.fixtures.CatalogSearchIT;
import com.example.orders.fixtures.FlakyGatewayIT;
import com.example.orders.fixtures.InventoryAuditIT;
import com.example.orders.fixtures.MediumRoastIT;
import com.example.orders.fixtures.QuickShotIT;
import com.example.orders.fixtures.SlowBrewIT;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * The three behaviours the engine is bought for, each against a real coordinator in a
 * container: work is handed out slowest-first once durations are known, a failure with
 * budget left is retried and recovered, and a failure without budget stays failed.
 *
 * <p>Kept apart from {@link CoordinatedShardingE2EIT}, which asserts cold first-contact
 * behaviour on a coordinator that has never seen the suite. Ordering is the opposite case
 * -- it cannot exist without history -- so these tests deliberately run a session to
 * record durations and then a second one to observe the order they produce.
 */
@Tag("shard4j-harness")
public class OrderingAndRetryE2EIT {

  private static GenericContainer<?> coordinator;
  private static String url;

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
  void givenRecordedDurations_whenASecondSessionDrains_thenClassesAreHandedOutSlowestFirst() {
    List<Class<?>> suite = List.of(QuickShotIT.class, MediumRoastIT.class, SlowBrewIT.class);

    // Session one exists only to teach the coordinator what these classes cost. Its own
    // order is unconstrained -- with no history there is nothing to rank by.
    ShardingHarness.runShard(url, UUID.randomUUID().toString(), 0, suite);

    ShardingHarness.ShardRun ranked =
        ShardingHarness.runShard(url, UUID.randomUUID().toString(), 0, suite);

    assertThat(classOrderOf(ranked.startedTests()))
        .as("slowest class first, once the coordinator knows what each costs")
        .containsExactly("SlowBrewIT", "MediumRoastIT", "QuickShotIT");
  }

  @Test
  void givenATestThatFailsEveryAttempt_whenTheBudgetIsSpent_thenItEndsFailedCarryingEveryAttempt() {
    String sessionId = UUID.randomUUID().toString();

    ShardingHarness.runShard(url, sessionId, 0, List.of(AlwaysFailsIT.class));

    SessionView view = ShardingHarness.viewOf(coordinator, sessionId);
    SessionView.TestView doomed = only(view);
    assertThat(doomed.state())
        .as("an exhausted budget must stay FAILED, never drain into a green session")
        .isEqualTo(TestState.FAILED);
    assertThat(doomed.records())
        .extracting(SessionView.RecordView::attempt, SessionView.RecordView::outcome)
        .containsExactly(
            org.assertj.core.api.Assertions.tuple(1, Outcome.FAILED),
            org.assertj.core.api.Assertions.tuple(2, Outcome.FAILED),
            org.assertj.core.api.Assertions.tuple(3, Outcome.FAILED));
  }

  @Test
  void givenATestThatFailsOnceThenPasses_whenItIsRequeued_thenItRecoversInsideTheSameDrain() {
    String sessionId = UUID.randomUUID().toString();

    ShardingHarness.runShard(url, sessionId, 0, List.of(FlakyGatewayIT.class));

    SessionView view = ShardingHarness.viewOf(coordinator, sessionId);
    SessionView.TestView flaky = only(view);
    assertThat(flaky.state()).isEqualTo(TestState.PASSED);
    assertThat(flaky.records())
        .as("no second failsafe execution was involved; the requeue was taken in one drain")
        .extracting(SessionView.RecordView::attempt, SessionView.RecordView::outcome)
        .containsExactly(
            org.assertj.core.api.Assertions.tuple(1, Outcome.FAILED),
            org.assertj.core.api.Assertions.tuple(2, Outcome.PASSED));
  }

  @Test
  void givenTwoConcurrentShards_whenTheSuiteDrains_thenBothDidWorkAndNothingRanTwice()
      throws Exception {
    String sessionId = UUID.randomUUID().toString();
    List<Class<?>> suite =
        List.of(
            SlowBrewIT.class,
            MediumRoastIT.class,
            QuickShotIT.class,
            CatalogSearchIT.class,
            InventoryAuditIT.class);

    List<ShardingHarness.ShardRun> runs = new CopyOnWriteArrayList<>();
    List<Thread> shards = new ArrayList<>();
    for (int shard = 0; shard < 2; shard++) {
      int index = shard;
      shards.add(
          new Thread(
              () -> runs.add(ShardingHarness.runShard(url, sessionId, index, suite)),
              "shard-" + index));
    }
    shards.forEach(Thread::start);
    for (Thread shard : shards) {
      shard.join();
    }

    SessionView view = ShardingHarness.viewOf(coordinator, sessionId);
    assertThat(view.shards())
        .as("both shards must have taken work; one shard doing everything is not sharding")
        .hasSize(2)
        .allSatisfy(shard -> assertThat(shard.completed()).isPositive());
    assertThat(view.tests())
        .as("every unit runs once: a claimed unit is leased, never handed to two shards")
        .allSatisfy(test -> assertThat(test.records()).hasSize(1));
    long terminal = view.tests().stream().filter(test -> test.state().isAbsorbing()).count();
    assertThat(terminal).isEqualTo(view.registeredCount());
  }

  /** Class simple names in the order their first test started, de-duplicated. */
  private static List<String> classOrderOf(List<String> startedTests) {
    List<String> order = new ArrayList<>();
    for (String started : startedTests) {
      int classAt = started.indexOf("[class:");
      if (classAt < 0) {
        continue;
      }
      String fqcn = started.substring(classAt + "[class:".length(), started.indexOf(']', classAt));
      String simple = fqcn.substring(fqcn.lastIndexOf('.') + 1);
      if (!order.contains(simple)) {
        order.add(simple);
      }
    }
    return order;
  }

  private static SessionView.TestView only(SessionView view) {
    assertThat(view.tests()).hasSize(1);
    return view.tests().get(0);
  }
}
