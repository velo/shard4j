package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.coordinator.core.CoverageVerdict;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.SessionVerdict;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.io.IOException;
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
  void givenExpiredLease_whenAnotherShardReclaims_thenUnitRequeuedAndLateWriterFencedOut()
      throws Exception {
    String sessionId = UUID.randomUUID().toString();
    String testId = Ids.method(CLASS_NAME, "hangs");
    List<String> census = List.of(testId);
    client.register(
        sessionId, new RegisterRequest(0, 1, Map.of(), census));
    client.register(
        sessionId, new RegisterRequest(1, 1, Map.of(), census));

    ClaimResponse silent =
        client.claim(sessionId, new ClaimRequest(0, Pass.MAIN, CLASS_NAME, List.of(testId)));
    assertThat(silent.granted()).hasSize(1);
    Fence deadFence = silent.granted().get(0).fence();
    assertThat(client.stateOf(sessionId, testId)).isEqualTo(TestState.LEASED);

    // While LEASED, the read surface names the holder, the fence and the expiry -- the
    // stranded-lease detail a human needs to see who is sitting on a unit.
    SessionView.TestView leasedTest =
        client.view(sessionId).tests().stream()
            .filter(test -> test.testId().equals(testId))
            .findFirst()
            .orElseThrow();
    assertThat(leasedTest.lease()).isNotNull();
    assertThat(leasedTest.lease().shard()).isEqualTo(0);
    assertThat(leasedTest.lease().fence()).isEqualTo(deadFence);
    assertThat(leasedTest.lease().expiresAt()).isNotNull();

    // Immediately, before expiry, the unit is not claimable by anyone else.
    ClaimResponse tooSoon =
        client.claim(sessionId, new ClaimRequest(1, Pass.MAIN, CLASS_NAME, List.of(testId)));
    assertThat(tooSoon.granted()).isEmpty();

    waitUntil(
        () -> client.stateOf(sessionId, testId) == TestState.PENDING,
        "the lease should expire back to PENDING");

    ClaimResponse reclaimed =
        client.claim(sessionId, new ClaimRequest(1, Pass.MAIN, CLASS_NAME, List.of(testId)));
    assertThat(reclaimed.granted()).hasSize(1);
    Fence liveFence = reclaimed.granted().get(0).fence();
    assertThat(liveFence).isGreaterThan(deadFence);

    CoordinatorClient.RawResponse lateWrite =
        client.resultRaw(
            sessionId,
            new ResultRequest(
                0, Pass.MAIN, testId, deadFence, Outcome.PASSED, 60_000, false, null, null));
    assertThat(lateWrite.status()).isEqualTo(409);

    client.result(
        sessionId,
        new ResultRequest(1, Pass.MAIN, testId, liveFence, Outcome.PASSED, 900, false, null, null));
    assertThat(client.stateOf(sessionId, testId)).isEqualTo(TestState.PASSED);
  }

  /**
   * A runner that dies without a word never announces departure, so the roster must treat
   * it as departed once its lease expires -- otherwise a stranded session reads FAILED
   * forever and INCOMPLETE, the diagnosis built for exactly this, is unreachable.
   */
  @Test
  void givenSilentlyDeadShard_whenItsLeaseExpires_thenShardIsDepartedAndSessionIncomplete()
      throws Exception {
    String sessionId = UUID.randomUUID().toString();
    String hanging = Ids.method(CLASS_NAME, "hangsForever");
    String untouched = Ids.method(CLASS_NAME, "neverClaimed");
    List<String> census = List.of(hanging, untouched);
    client.register(
        sessionId, new RegisterRequest(0, 1, Map.of(), census));

    ClaimResponse claimed =
        client.claim(sessionId, new ClaimRequest(0, Pass.MAIN, CLASS_NAME, List.of(hanging)));
    assertThat(claimed.granted()).hasSize(1);

    waitUntil(
        () -> client.stateOf(sessionId, hanging) == TestState.PENDING,
        "the lease should expire back to PENDING");

    SessionView stranded = client.view(sessionId);
    assertThat(stranded.shards()).allMatch(SessionView.ShardView::departed);
    assertThat(CoverageVerdict.of(stranded)).isEqualTo(SessionVerdict.INCOMPLETE);
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
