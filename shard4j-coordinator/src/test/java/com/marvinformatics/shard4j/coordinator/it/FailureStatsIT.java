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
 * Failure statistics through the real container: a flake recorded over the wire is
 * reported with its trace, ranked, flagged as new, kept across a hard restart -- and never
 * served without the secret, even where session views are public.
 */
class FailureStatsIT {

  private static final String CLASS_NAME = "com.example.orders.PaymentGatewayIT";
  private static final Failure TIMEOUT =
      new Failure(
          "java.net.SocketTimeoutException",
          "Read timed out",
          "java.net.SocketTimeoutException: Read timed out\n"
              + "\tat com.example.orders.PaymentGatewayIT.authorize(PaymentGatewayIT.java:42)\n");

  @Test
  void givenAFlakeAndABrokenTest_whenQueried_thenBothAreReportedAndSurviveARestart()
      throws Exception {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "failure-stats-data");
    String sessionId = UUID.randomUUID().toString();
    String flaky = Ids.method(CLASS_NAME, "authorize");
    String broken = Ids.method(CLASS_NAME, "capture");
    String steady = Ids.method(CLASS_NAME, "refund");

    GenericContainer<?> first =
        CoordinatorContainers.coordinator(dataDir, Map.of("COORDINATOR_PUBLIC_READ", "true"));
    try {
      first.start();
      CoordinatorClient client = new CoordinatorClient(first);
      client.register(
          sessionId, new RegisterRequest(0, 1, Map.of(), List.of(flaky, broken, steady), null));
      report(client, sessionId, flaky, Outcome.FAILED, 3_000, TIMEOUT);
      report(client, sessionId, flaky, Outcome.PASSED, 1_000, null);
      report(client, sessionId, steady, Outcome.PASSED, 500, null);
      for (int attempt = 0; attempt < 3; attempt++) {
        report(client, sessionId, broken, Outcome.FAILED, 200, TIMEOUT);
      }

      JsonNode emerging = client.stats("emerging", Map.of("session", sessionId)).get("tests");
      assertThat(emerging).hasSize(1);
      assertThat(emerging.get(0).get("test").asText()).isEqualTo(CLASS_NAME + "#authorize()");
      assertThat(emerging.get(0).get("retryCostMs").asLong()).isEqualTo(3_000);
      assertThat(emerging.get(0).get("baselineSessions").asInt()).isZero();

      JsonNode unstable =
          client.stats("unstable", Map.of("since", "1d", "sort", "failures")).get("tests");
      assertThat(unstable).hasSize(2);
      assertThat(unstable.get(0).get("test").asText()).isEqualTo(CLASS_NAME + "#capture()");
      assertThat(unstable.get(0).get("failedSessions").asInt()).isOne();
      assertThat(unstable.get(1).get("flakySessions").asInt()).isOne();

      CoordinatorClient anonymous = new CoordinatorClient(first, null);
      assertThat(anonymous.viewRaw(sessionId).status())
          .as("the session view is public here")
          .isEqualTo(200);
      assertThat(anonymous.statsRaw("failures", Map.of()).status())
          .as("stack traces never are")
          .isEqualTo(401);
      assertThat(client.statsRaw("failures", Map.of("since", "5x")).body()).contains("since must be");

      first.getDockerClient().killContainerCmd(first.getContainerId()).exec();
    } finally {
      first.stop();
    }

    GenericContainer<?> second = CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      second.start();
      CoordinatorClient client = new CoordinatorClient(second);

      JsonNode report = client.stats("failures", Map.of("since", "1d"));
      assertThat(report.get("failures")).hasSize(4);
      assertThat(report.get("signatures")).hasSize(1);
      JsonNode signature = report.get("signatures").elements().next();
      assertThat(signature.get("stackTrace").asText()).isEqualTo(TIMEOUT.stackTrace());

      JsonNode trend = client.stats("trend", Map.of("since", "1d", "bucket", "1d"));
      assertThat(trend.get("buckets")).hasSize(1);
      assertThat(trend.get("buckets").get(0).get("flakyTests").asInt()).isOne();
      assertThat(trend.get("buckets").get(0).get("retriedAttempts").asInt()).isEqualTo(3);
    } finally {
      second.stop();
    }
  }

  private static void report(
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
}
