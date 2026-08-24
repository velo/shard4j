package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NackResponse;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.ResultResponse;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.io.IOException;
import java.net.http.HttpResponse;
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
 * The zombie write: a shard that lost its lease posts anyway. The write is rejected with
 * the fence that beat it, kept aside with zero effect on state, and the legitimate holder's
 * subsequent write lands untouched. Accepting a stale PASSED is exactly the write a leaked
 * secret would forge, so the rejection is the security property, not an inconvenience.
 */
class StaleFenceIT {

  private static final String CLASS_NAME = "com.example.orders.ZombieIT";

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void start() throws IOException {
    coordinator =
        CoordinatorContainers.coordinator(
            Files.createTempDirectory(Path.of("target"), "stale-fence-data"), Map.of());
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void aZombieWriteIsRejectedWithTheCurrentFenceAndDoesNotCorruptState() {
    String sessionId = UUID.randomUUID().toString();
    String contested = Ids.method(CLASS_NAME, "contested");
    String bystander = Ids.method(CLASS_NAME, "bystander");
    List<String> census = List.of(contested, bystander);
    client.register(
        sessionId, new RegisterRequest(0, 1, Map.of(), CoordinatorClient.hashOf(census), census));
    client.register(
        sessionId, new RegisterRequest(1, 1, Map.of(), CoordinatorClient.hashOf(census), census));

    Fence zombieFence = claim(0, sessionId, contested);
    NackResponse nack =
        client.nack(
            sessionId,
            new NackRequest(
                0,
                List.of(new NackRequest.NackedLease(contested, zombieFence, "job cancelled"))));
    assertThat(nack.released()).containsExactly(contested);

    Fence holderFence = claim(1, sessionId, contested);

    HttpResponse<String> zombieWrite =
        client.resultRaw(
            sessionId,
            new ResultRequest(
                0, Pass.MAIN, contested, zombieFence, Outcome.PASSED, 4_000, false, null, null));
    assertThat(zombieWrite.statusCode()).isEqualTo(409);
    ResultResponse rejection =
        CoordinatorClient.JSON.readValue(zombieWrite.body(), ResultResponse.class);
    assertThat(rejection.accepted()).isFalse();
    assertThat(rejection.currentFence())
        .as("the rejection names the fence that beat the writer")
        .isEqualTo(holderFence);

    SessionView afterZombie = client.view(sessionId);
    assertThat(stateOf(afterZombie, contested))
        .as("the rejected write must not have moved the state machine")
        .isEqualTo(TestState.LEASED);
    assertThat(afterZombie.staleResults())
        .as("the zombie's payload is kept as the signal that a shard went zombie")
        .hasSize(1);

    // A stale NACK is fenced identically: it would otherwise requeue the holder's work.
    NackResponse staleNack =
        client.nack(
            sessionId,
            new NackRequest(
                0, List.of(new NackRequest.NackedLease(contested, zombieFence, "late teardown"))));
    assertThat(staleNack.released()).isEmpty();
    assertThat(staleNack.rejected()).containsExactly(contested);

    client.result(
        sessionId,
        new ResultRequest(
            1, Pass.MAIN, contested, holderFence, Outcome.PASSED, 5_000, false, null, null));
    SessionView finalView = client.view(sessionId);
    assertThat(stateOf(finalView, contested)).isEqualTo(TestState.PASSED);
    assertThat(
            finalView.tests().stream()
                .filter(test -> test.testId().equals(contested))
                .findFirst()
                .orElseThrow()
                .records())
        .as("exactly one recorded result despite two writers")
        .hasSize(1);
  }

  private static Fence claim(int shard, String sessionId, String testId) {
    ClaimResponse response =
        client.claim(sessionId, new ClaimRequest(shard, Pass.MAIN, CLASS_NAME, List.of(testId)));
    assertThat(response.granted()).hasSize(1);
    return response.granted().get(0).fence();
  }

  private static TestState stateOf(SessionView view, String testId) {
    return view.tests().stream()
        .filter(test -> test.testId().equals(testId))
        .findFirst()
        .orElseThrow()
        .state();
  }
}
