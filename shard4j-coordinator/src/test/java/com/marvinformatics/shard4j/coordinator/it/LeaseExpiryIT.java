package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.TestState;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Lease expiry is the backstop for a shard that died without a word: with no heartbeat in
 * the design, the TTL is the only thing that returns an unfinished unit to the queue, and
 * the next grant must carry a strictly higher fence so the dead shard's late write loses.
 */
class LeaseExpiryIT {

  private static final String CLASS_NAME = "com.example.orders.SilentDeathIT";

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void start() throws IOException {
    coordinator =
        CoordinatorContainers.coordinator(
            Files.createTempDirectory(Path.of("target"), "lease-expiry-data"),
            Map.of("COORDINATOR_LEASE_TTL", "2s"));
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void anExpiredLeaseReturnsTheUnitToTheQueueAndFencesOutTheLateWriter() throws Exception {
    String sessionId = UUID.randomUUID().toString();
    String testId = Ids.method(CLASS_NAME, "hangs");
    List<String> census = List.of(testId);
    client.register(
        sessionId, new RegisterRequest(0, 1, Map.of(), CoordinatorClient.hashOf(census), census));
    client.register(
        sessionId, new RegisterRequest(1, 1, Map.of(), CoordinatorClient.hashOf(census), census));

    ClaimResponse silent =
        client.claim(sessionId, new ClaimRequest(0, Pass.MAIN, CLASS_NAME, List.of(testId)));
    assertThat(silent.granted()).hasSize(1);
    Fence deadFence = silent.granted().get(0).fence();
    assertThat(stateOf(sessionId, testId)).isEqualTo(TestState.LEASED);

    // Immediately, before expiry, the unit is not claimable by anyone else.
    ClaimResponse tooSoon =
        client.claim(sessionId, new ClaimRequest(1, Pass.MAIN, CLASS_NAME, List.of(testId)));
    assertThat(tooSoon.granted()).isEmpty();

    waitUntil(
        () -> stateOf(sessionId, testId) == TestState.PENDING,
        "the lease should expire back to PENDING");

    ClaimResponse reclaimed =
        client.claim(sessionId, new ClaimRequest(1, Pass.MAIN, CLASS_NAME, List.of(testId)));
    assertThat(reclaimed.granted()).hasSize(1);
    Fence liveFence = reclaimed.granted().get(0).fence();
    assertThat(liveFence).isGreaterThan(deadFence);

    HttpResponse<String> lateWrite =
        client.resultRaw(
            sessionId,
            new ResultRequest(
                0, Pass.MAIN, testId, deadFence, Outcome.PASSED, 60_000, false, null, null));
    assertThat(lateWrite.statusCode()).isEqualTo(409);

    client.result(
        sessionId,
        new ResultRequest(1, Pass.MAIN, testId, liveFence, Outcome.PASSED, 900, false, null, null));
    assertThat(stateOf(sessionId, testId)).isEqualTo(TestState.PASSED);
  }

  private static TestState stateOf(String sessionId, String testId) {
    return client.view(sessionId).tests().stream()
        .filter(test -> test.testId().equals(testId))
        .findFirst()
        .orElseThrow()
        .state();
  }

  private static void waitUntil(BooleanSupplier condition, String what)
      throws InterruptedException {
    Instant deadline = Instant.now().plusSeconds(15);
    while (!condition.getAsBoolean()) {
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("Timed out waiting: " + what);
      }
      Thread.sleep(250);
    }
  }
}
