package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.marvinformatics.shard4j.protocol.Failure;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Failure statistics through the real container: a flake recorded over the wire is reported with
 * its trace, ranked and flagged as new. It is kept across a hard restart, and it is never served
 * without the secret, even where session views are public.
 */
class FailureStatsIT {

  private static final String CLASS_NAME = "com.example.orders.PaymentGatewayIT";
  private static final String FLAKY_TEST_ID = Ids.method(CLASS_NAME, "authorize");
  private static final String BROKEN_TEST_ID = Ids.method(CLASS_NAME, "capture");
  private static final String PASSING_TEST_ID = Ids.method(CLASS_NAME, "refund");
  private static final Failure TIMEOUT =
      new Failure(
          "java.net.SocketTimeoutException",
          "Read timed out",
          "java.net.SocketTimeoutException: Read timed out\n"
              + "\tat com.example.orders.PaymentGatewayIT.authorize(PaymentGatewayIT.java:42)\n");

  @Test
  void
      givenAFlakyABrokenAndAPassingTest_whenStatsAreQueried_thenOnlyTheFlakeIsEmergingAndTheBrokenTestRanksFirstAsUnstable()
          throws Exception {
    GenericContainer<?> coordinator = CoordinatorContainers.coordinator(newDataDir(), Map.of());
    try {
      coordinator.start();
      CoordinatorClient authenticatedClient = new CoordinatorClient(coordinator);
      String sessionId = runFlakyBrokenAndPassingTests(authenticatedClient);

      JsonNode emergingTests =
          authenticatedClient
              .getStatsReportAssertingOk("emerging", Map.of("sessionId", sessionId))
              .get("tests");
      assertThat(emergingTests).hasSize(1);
      assertThat(emergingTests.get(0).get("historyKey").asText())
          .isEqualTo(CLASS_NAME + "#authorize()");
      assertThat(emergingTests.get(0).get("retryCostMs").asLong()).isEqualTo(3_000);
      assertThat(emergingTests.get(0).get("baselineSessions").asInt()).isZero();

      JsonNode unstableTests =
          authenticatedClient
              .getStatsReportAssertingOk("unstable", Map.of("since", "1d", "sort", "failures"))
              .get("tests");
      assertThat(unstableTests).hasSize(2);
      assertThat(unstableTests.get(0).get("historyKey").asText())
          .isEqualTo(CLASS_NAME + "#capture()");
      assertThat(unstableTests.get(0).get("failedSessions").asInt()).isOne();
      assertThat(unstableTests.get(1).get("flakySessions").asInt()).isOne();
    } finally {
      coordinator.stop();
    }
  }

  @Test
  void
      givenPublicSessionViews_whenFailureStatsAreRequestedAnonymouslyOrMalformed_thenTheyAreRejected()
          throws Exception {
    GenericContainer<?> coordinator =
        CoordinatorContainers.coordinator(newDataDir(), Map.of("COORDINATOR_PUBLIC_READ", "true"));
    try {
      coordinator.start();
      CoordinatorClient authenticatedClient = new CoordinatorClient(coordinator);
      CoordinatorClient anonymousClient = new CoordinatorClient(coordinator, null);
      String sessionId = runFlakyBrokenAndPassingTests(authenticatedClient);

      assertThat(anonymousClient.viewRaw(sessionId).status())
          .as("the session view is public here")
          .isEqualTo(200);
      assertThat(anonymousClient.getStatsReportRaw("failures", Map.of()).status())
          .as("stack traces never are")
          .isEqualTo(401);
      assertThat(authenticatedClient.getStatsReportRaw("failures", Map.of("since", "5x")).body())
          .contains("since must be");
      assertThat(
              authenticatedClient
                  .getStatsReportRaw("emerging", Map.of("sessionId", "never-registered"))
                  .status())
          .isEqualTo(404);
    } finally {
      coordinator.stop();
    }
  }

  @Test
  void
      givenRecordedFailures_whenTheCoordinatorIsKilledAndRestarted_thenFailuresDistinctFailuresAndTrendAreStillReported()
          throws Exception {
    Path dataDir = newDataDir();
    GenericContainer<?> coordinatorBeforeRestart =
        CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      coordinatorBeforeRestart.start();
      runFlakyBrokenAndPassingTests(new CoordinatorClient(coordinatorBeforeRestart));
      coordinatorBeforeRestart
          .getDockerClient()
          .killContainerCmd(coordinatorBeforeRestart.getContainerId())
          .exec();
    } finally {
      coordinatorBeforeRestart.stop();
    }

    GenericContainer<?> coordinatorAfterRestart =
        CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      coordinatorAfterRestart.start();
      CoordinatorClient authenticatedClient = new CoordinatorClient(coordinatorAfterRestart);

      JsonNode failureStats =
          authenticatedClient.getStatsReportAssertingOk("failures", Map.of("since", "1d"));
      assertThat(failureStats.get("failedAttempts")).hasSize(4);
      assertThat(failureStats.get("distinctFailures")).hasSize(1);
      assertThat(failureStats.get("distinctFailures").elements().next().get("stackTrace").asText())
          .isEqualTo(TIMEOUT.stackTrace());

      JsonNode trend =
          authenticatedClient.getStatsReportAssertingOk(
              "trend", Map.of("since", "1d", "bucketSpan", "1d"));
      assertThat(trend.get("buckets")).hasSize(1);
      assertThat(trend.get("buckets").get(0).get("flakyTests").asInt()).isOne();
      assertThat(trend.get("buckets").get(0).get("retriedAttempts").asInt()).isEqualTo(3);
    } finally {
      coordinatorAfterRestart.stop();
    }
  }

  private static String runFlakyBrokenAndPassingTests(CoordinatorClient client) {
    String sessionId = UUID.randomUUID().toString();
    client.register(
        sessionId,
        new RegisterRequest(
            0, 1, Map.of(), List.of(FLAKY_TEST_ID, BROKEN_TEST_ID, PASSING_TEST_ID), null, 3));
    claimAndSubmitResult(client, sessionId, FLAKY_TEST_ID, Outcome.FAILED, 3_000, TIMEOUT);
    claimAndSubmitResult(client, sessionId, FLAKY_TEST_ID, Outcome.PASSED, 1_000, null);
    claimAndSubmitResult(client, sessionId, PASSING_TEST_ID, Outcome.PASSED, 500, null);
    for (int attempt = 0; attempt < 3; attempt++) {
      claimAndSubmitResult(client, sessionId, BROKEN_TEST_ID, Outcome.FAILED, 200, TIMEOUT);
    }
    return sessionId;
  }

  private static void claimAndSubmitResult(
      CoordinatorClient client,
      String sessionId,
      String testId,
      Outcome outcome,
      long durationMs,
      Failure failure) {
    Fence fence = client.claimOne(sessionId, 0, testId);
    client.result(
        sessionId,
        new ResultRequest(0, testId, fence, outcome, durationMs, false, null, null, failure));
  }

  private static Path newDataDir() throws Exception {
    return Files.createTempDirectory(Path.of("target"), "failure-stats-data");
  }
}
