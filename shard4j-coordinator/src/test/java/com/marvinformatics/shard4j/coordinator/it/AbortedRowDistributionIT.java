package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NextClassRequest;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.SessionView;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * A suite that skips by assumption must still be distributable. An aborting row makes the
 * shard's aggregate over a template ABORTED, and a duration store that only learns from
 * PASSED learns nothing about such a method at all -- so it leases whole in every session
 * and one shard runs the lot. One test per site that has to agree: the whole-unit arm, the
 * individually-leased arm, and the cold-load fold.
 *
 * <p>{@link InvocationDistributionIT} covers distribution itself.
 */
class AbortedRowDistributionIT {

  private static final String CLASS_NAME = "com.example.orders.AssumingSuiteIT";
  private static final String FRESH =
      Ids.template(CLASS_NAME, "freshAssumingRows(java.lang.String)");
  private static final String SEEDED =
      Ids.template(CLASS_NAME, "seededAssumingRows(java.lang.String)");

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void seedThenStart() throws IOException {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "aborted-row-data");
    History.seedTemplate(dataDir, SEEDED, Map.of(1, 25_000L, 2, 15_000L));
    coordinator = CoordinatorContainers.coordinator(dataDir, Map.of());
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  /**
   * A whole report is the only chance to learn a breakdown, because a method that
   * distributes is never leased whole again.
   */
  @Test
  void givenATemplateWithAnAssumptionSkippedRow_whenItReportsWhole_thenTheNextSessionDistributes() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, new RegisterRequest(0, 1, Map.of(), List.of(FRESH), null));

    // No history: the template leases whole, as it must on a cold coordinator.
    NextClassResponse next = client.next(sessionId, new NextClassRequest(0));
    assertThat(next.granted().stream().map(Grant::testId)).containsExactly(FRESH);

    client.result(
        sessionId,
        new ResultRequest(
            0,
            FRESH,
            next.granted().get(0).fence(),
            Outcome.ABORTED,
            75_000,
            false,
            "assumption failed: staging quota exhausted",
            List.of(
                new InvocationRecord(Ids.invocation(FRESH, 1), Outcome.PASSED, 30_000, null),
                new InvocationRecord(Ids.invocation(FRESH, 2), Outcome.PASSED, 45_000, null),
                new InvocationRecord(
                    Ids.invocation(FRESH, 3), Outcome.ABORTED, 0, "assumption failed"))));

    // The aborting position stays in the plan: it is the row that runs the day the
    // assumption holds again.
    assertThat(planOf(FRESH))
        .containsExactlyInAnyOrder(
            Ids.invocation(FRESH, 1),
            Ids.invocation(FRESH, 2),
            Ids.invocation(FRESH, 3),
            Ids.invocation(FRESH, 4));
  }

  /**
   * A distributed method's plan is refreshed only by individually-leased rows, and only
   * once every measured row absorbs -- so an aborting row must record its own position and
   * let the refresh through, here while a probe proves the parameter set grew.
   */
  @Test
  void givenAnAbortedRowBesideGrowth_whenTheNextSessionRegisters_thenTheGrownPlanIsRemembered() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, new RegisterRequest(0, 1, Map.of(), List.of(SEEDED), null));

    // A lone shard is never capped: both measured positions plus the probe past them.
    NextClassResponse next = client.next(sessionId, new NextClassRequest(0));
    assertThat(next.granted().stream().map(Grant::testId))
        .containsExactly(
            Ids.invocation(SEEDED, 1), Ids.invocation(SEEDED, 2), Ids.invocation(SEEDED, 3));

    report(sessionId, next.granted().get(0), Outcome.PASSED, null);
    report(sessionId, next.granted().get(1), Outcome.ABORTED, "assumption failed");
    // The probe materialised: there really is a third row now, and #4 is probed in turn.
    report(sessionId, next.granted().get(2), Outcome.PASSED, null);

    NextClassResponse walked = client.next(sessionId, new NextClassRequest(0));
    assertThat(walked.granted().stream().map(Grant::testId))
        .containsExactly(Ids.invocation(SEEDED, 4));
    nackVanished(sessionId, walked.granted().get(0));

    assertThat(planOf(SEEDED))
        .as("the growth was absorbed into the plan, not rediscovered from scratch next run")
        .containsExactlyInAnyOrder(
            Ids.invocation(SEEDED, 1),
            Ids.invocation(SEEDED, 2),
            Ids.invocation(SEEDED, 3),
            Ids.invocation(SEEDED, 4));
  }

  /**
   * The live path and the replay path must agree, or a restart silently un-distributes a
   * method and nothing about the failure names a restart as its cause. Pinned against a
   * real kill and a real reboot on the same volume rather than against the fold alone.
   */
  @Test
  void givenAnAbortedTemplate_whenTheCoordinatorRestarts_thenTheReplayedPlanStillDistributes()
      throws Exception {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "aborted-row-replay-data");
    String templateId = Ids.template(CLASS_NAME, "replayedAssumingRows(java.lang.String)");
    String sessionId = UUID.randomUUID().toString();

    GenericContainer<?> first = CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      first.start();
      CoordinatorClient firstClient = new CoordinatorClient(first);
      firstClient.register(
          sessionId, new RegisterRequest(0, 1, Map.of(), List.of(templateId), null));
      NextClassResponse next = firstClient.next(sessionId, new NextClassRequest(0));
      firstClient.result(
          sessionId,
          new ResultRequest(
              0,
              templateId,
              next.granted().get(0).fence(),
              Outcome.ABORTED,
              60_000,
              false,
              "assumption failed: staging quota exhausted",
              List.of(
                  new InvocationRecord(
                      Ids.invocation(templateId, 1), Outcome.PASSED, 20_000, null),
                  new InvocationRecord(
                      Ids.invocation(templateId, 2), Outcome.ABORTED, 0, "assumption failed"),
                  new InvocationRecord(
                      Ids.invocation(templateId, 3), Outcome.PASSED, 40_000, null))));

      first.getDockerClient().killContainerCmd(first.getContainerId()).exec();
    } finally {
      first.stop();
    }

    GenericContainer<?> second = CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      second.start();
      CoordinatorClient secondClient = new CoordinatorClient(second);
      String replayedSession = UUID.randomUUID().toString();
      secondClient.register(
          replayedSession, new RegisterRequest(0, 1, Map.of(), List.of(templateId), null));

      assertThat(
              secondClient.view(replayedSession).tests().stream()
                  .map(SessionView.TestView::testId))
          .containsExactlyInAnyOrder(
              Ids.invocation(templateId, 1),
              Ids.invocation(templateId, 2),
              Ids.invocation(templateId, 3),
              Ids.invocation(templateId, 4));
    } finally {
      second.stop();
    }
  }

  /** What a shard would actually be handed: a plan is not an endpoint, so read one back. */
  private static List<String> planOf(String templateId) {
    String probeSession = UUID.randomUUID().toString();
    client.register(probeSession, new RegisterRequest(0, 1, Map.of(), List.of(templateId), null));
    return client.view(probeSession).tests().stream().map(SessionView.TestView::testId).toList();
  }

  private static void report(String sessionId, Grant grant, Outcome outcome, String reason) {
    client.result(
        sessionId,
        new ResultRequest(
            0, grant.testId(), grant.fence(), outcome, 45_000, false, reason, null));
  }

  private static void nackVanished(String sessionId, Grant grant) {
    client.nack(
        sessionId,
        new NackRequest(
            0,
            List.of(
                new NackRequest.NackedLease(
                    grant.testId(), grant.fence(), "probe vanished", true))));
  }
}
