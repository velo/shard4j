package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * A restart mid-session is the normal case, not a disaster: the container is killed hard
 * (no graceful shutdown, so only what was fsynced survives) and brought back on the same
 * volume, and the session must come back by replay -- terminal states intact, leases
 * deliberately gone, fences unforgeable by pre-restart zombies.
 */
class RestartReplayIT {

  private static final String CLASS_A = "com.example.orders.ReplayAlphaIT";
  private static final String CLASS_B = "com.example.orders.ReplayBetaIT";

  @Test
  void givenAKilledCoordinator_whenRestartedOnTheSameVolume_thenPassedSetIntactAndLeasesReturned() throws Exception {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "restart-replay-data");
    String sessionId = UUID.randomUUID().toString();
    String passed = Ids.method(CLASS_A, "alreadyDone");
    String failed = Ids.method(CLASS_A, "flaky");
    String skipped = Ids.method(CLASS_A, "disabledUpstream");
    String leased = Ids.method(CLASS_B, "inFlightAtCrash");
    String untouched = Ids.method(CLASS_B, "neverClaimed");
    List<String> census = List.of(passed, failed, skipped, leased, untouched);
    RegisterRequest registration =
        new RegisterRequest(0, 1, Map.of(), census);

    long incarnationBefore;
    GenericContainer<?> first = CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      first.start();
      CoordinatorClient client = new CoordinatorClient(first);
      client.register(sessionId, registration);
      Fence passedFence = client.claimOne(sessionId, 0, passed);
      incarnationBefore = passedFence.incarnation();
      client.result(
          sessionId,
          new ResultRequest(0, Pass.MAIN, passed, passedFence, Outcome.PASSED, 5_000, false, null, null));
      Fence failedFence = client.claimOne(sessionId, 0, failed);
      client.result(
          sessionId,
          new ResultRequest(0, Pass.MAIN, failed, failedFence, Outcome.FAILED, 3_000, false, null, null));
      Fence skippedFence = client.claimOne(sessionId, 0, skipped);
      client.result(
          sessionId,
          new ResultRequest(
              0, Pass.MAIN, skipped, skippedFence, Outcome.SKIPPED, 2, false, "disabled", null));
      client.claimOne(sessionId, 0, leased);

      first.getDockerClient().killContainerCmd(first.getContainerId()).exec();
    } finally {
      first.stop();
    }

    GenericContainer<?> second = CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      second.start();
      CoordinatorClient client = new CoordinatorClient(second);

      SessionView view = client.view(sessionId);
      assertThat(view.registeredCount()).isEqualTo(5);
      assertThat(CoordinatorClient.stateOf(view, passed)).isEqualTo(TestState.PASSED);
      assertThat(CoordinatorClient.stateOf(view, failed)).isEqualTo(TestState.FAILED);
      assertThat(CoordinatorClient.stateOf(view, skipped)).isEqualTo(TestState.SKIPPED);
      assertThat(
              view.tests().stream()
                  .filter(test -> test.testId().equals(skipped))
                  .findFirst()
                  .orElseThrow()
                  .reason())
          .isEqualTo("disabled");
      // A lease is a liveness claim and liveness did not survive the crash.
      assertThat(CoordinatorClient.stateOf(view, leased)).isEqualTo(TestState.PENDING);
      assertThat(CoordinatorClient.stateOf(view, untouched)).isEqualTo(TestState.PENDING);

      // The recovered duration survives too: it came from the history files on the volume.
      assertThat(dataDir.resolve(CoordinatorContainers.TENANT_SLUG).resolve("current.json"))
          .exists();

      Fence reclaimed = client.claimOne(sessionId, 0, leased);
      assertThat(reclaimed.incarnation())
          .as("a restart must fence out every pre-restart zombie")
          .isGreaterThan(incarnationBefore);
    } finally {
      second.stop();
    }
  }

  /**
   * A quiet shard -- registered, but with no completion or pass record yet -- must survive
   * replay in the roster: were it forgotten, every quorum would resolve without it and the
   * next barrier would answer RUN prematurely, handing out retry work while the quiet
   * shard's main pass is still running.
   */
  @Test
  void givenAQuietShard_whenTheCoordinatorRestarts_thenTheReplayedRosterStillHoldsIt()
      throws Exception {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "quiet-shard-data");
    String sessionId = UUID.randomUUID().toString();
    String worked = Ids.method(CLASS_A, "worked");
    String pending = Ids.method(CLASS_A, "stillPending");
    List<String> census = List.of(worked, pending);

    GenericContainer<?> first = CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      first.start();
      CoordinatorClient client = new CoordinatorClient(first);
      client.register(
          sessionId, new RegisterRequest(0, 1, Map.of(), census));
      client.register(
          sessionId, new RegisterRequest(1, 1, Map.of(), census));
      Fence fence = client.claimOne(sessionId, 0, worked);
      client.result(
          sessionId,
          new ResultRequest(0, Pass.MAIN, worked, fence, Outcome.PASSED, 500, false, null, null));
      first.getDockerClient().killContainerCmd(first.getContainerId()).exec();
    } finally {
      first.stop();
    }

    GenericContainer<?> second = CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      second.start();
      CoordinatorClient client = new CoordinatorClient(second);
      assertThat(
              client.view(sessionId).shards().stream().map(SessionView.ShardView::shard).toList())
          .containsExactly(0, 1);
    } finally {
      second.stop();
    }
  }

}
