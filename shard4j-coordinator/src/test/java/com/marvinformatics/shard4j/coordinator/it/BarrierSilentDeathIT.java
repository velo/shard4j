package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.coordinator.core.CoverageVerdict;
import com.marvinformatics.shard4j.protocol.BarrierRequest;
import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.DepartRequest;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * A shard dying without a word while another waits at the barrier: lease expiry is the
 * only signal, and when it fires the dead shard must fall out of the quorum so the
 * survivor is released into the retry pass instead of waiting for a ghost.
 */
class BarrierSilentDeathIT {

  private static final String CLASS_NAME = "com.example.orders.BarrierSilentDeathIT";

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void start() throws IOException {
    coordinator =
        CoordinatorContainers.coordinator(
            Files.createTempDirectory(Path.of("target"), "barrier-silent-death-data"),
            Map.of("COORDINATOR_LEASE_TTL", "2s"));
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void givenAShardDiesHoldingALease_whenAnotherWaitsAtTheBarrier_thenExpiryUnblocksTheSurvivor()
      throws Exception {
    String sessionId = UUID.randomUUID().toString();
    String flaky = Ids.method(CLASS_NAME, "failsOnTheSurvivor");
    String doomed = Ids.method(CLASS_NAME, "diesWithItsShard");
    List<String> census = List.of(flaky, doomed);
    client.register(
        sessionId, new RegisterRequest(0, 1, Map.of(), census));
    client.register(
        sessionId, new RegisterRequest(1, 1, Map.of(), census));

    // Shard 1 claims its unit and is never heard from again.
    client.claimOne(sessionId, 1, doomed);

    Fence flakyFence = client.claimOne(sessionId, 0, flaky);
    client.result(
        sessionId,
        new ResultRequest(0, Pass.MAIN, flaky, flakyFence, Outcome.FAILED, 1_000, false, null, null));

    // While the dead shard's lease is live, the survivor is told how long that can last.
    BarrierResponse waiting =
        client.barrier(sessionId, new BarrierRequest(0, 1, Pass.MAIN));
    assertThat(waiting.action()).isEqualTo(BarrierResponse.Action.WAIT);
    assertThat(waiting.earliestLeaseExpiry()).isNotNull();

    // Expiry marks the holder departed and drops it from the quorum; the survivor runs.
    BarrierResponse unblocked = pollUntilNotWaiting(sessionId, 0, Pass.MAIN);
    assertThat(unblocked.action()).isEqualTo(BarrierResponse.Action.RUN);

    // Only the genuine failure is retry work; the dead shard's unit fell back to the main
    // pool, which no live shard will ever claim from again.
    ClaimResponse retry =
        client.claim(sessionId, new ClaimRequest(0, Pass.RETRY1, CLASS_NAME, census));
    assertThat(retry.granted()).hasSize(1);
    assertThat(retry.granted().get(0).testId()).isEqualTo(flaky);
    client.result(
        sessionId,
        new ResultRequest(
            0,
            Pass.RETRY1,
            flaky,
            retry.granted().get(0).fence(),
            Outcome.PASSED,
            1_200,
            false,
            null,
            null));

    // The stranded unit must not hold the next barrier open: nothing can retry it.
    assertThat(client.barrier(sessionId, new BarrierRequest(0, 1, Pass.RETRY1)).action())
        .isEqualTo(BarrierResponse.Action.DONE);

    client.depart(sessionId, new DepartRequest(0, 1));
    SessionView view = client.view(sessionId);
    assertThat(
            view.shards().stream()
                .filter(shard -> shard.shard() == 1)
                .findFirst()
                .orElseThrow()
                .departed())
        .isTrue();
    assertThat(CoordinatorClient.stateOf(view, doomed)).isEqualTo(TestState.PENDING);
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.INCOMPLETE);
  }

  private static BarrierResponse pollUntilNotWaiting(String sessionId, int shard, Pass pass)
      throws InterruptedException {
    Instant deadline = Instant.now().plusSeconds(15);
    while (true) {
      BarrierResponse response = client.barrier(sessionId, new BarrierRequest(shard, 1, pass));
      if (response.action() != BarrierResponse.Action.WAIT) {
        return response;
      }
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("Timed out waiting for the barrier to unblock");
      }
      Thread.sleep(250);
    }
  }
}
