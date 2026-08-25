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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * What a barrier decides must survive a restart, and the completion log is how: the
 * container is killed hard mid-barrier and brought back on the same volume. Three records
 * are load-bearing here -- the pass watermark (or the quorum re-opens for a shard that
 * already finished), the departure (or a ghost holds the barrier forever), and the early
 * release (or a released shard is granted work it will never run).
 */
class BarrierRestartIT {

  private static final String CLASS_NAME = "com.example.orders.BarrierRestartIT";

  @Test
  void givenAKilledCoordinator_whenRestartedMidBarrier_thenWatermarksDeparturesAndReleasesSurvive()
      throws Exception {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "barrier-restart-data");
    String sessionId = UUID.randomUUID().toString();
    String mine = Ids.method(CLASS_NAME, "mine");
    String yours = Ids.method(CLASS_NAME, "yours");
    String flaky = Ids.method(CLASS_NAME, "flaky");
    List<String> census = List.of(mine, yours, flaky);

    GenericContainer<?> first = CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      first.start();
      CoordinatorClient client = new CoordinatorClient(first);
      for (int shard = 0; shard <= 2; shard++) {
        client.register(
            sessionId,
            new RegisterRequest(shard, 1, Map.of(), census));
      }
      // Shard 2 hits its deadline before claiming anything and goes home.
      client.depart(sessionId, new DepartRequest(2, 1));

      Fence mineFence = client.claimOne(sessionId, 0, mine);
      client.result(
          sessionId,
          new ResultRequest(0, Pass.MAIN, mine, mineFence, Outcome.PASSED, 900, false, null, null));
      Fence flakyFence = client.claimOne(sessionId, 0, flaky);
      client.result(
          sessionId,
          new ResultRequest(0, Pass.MAIN, flaky, flakyFence, Outcome.FAILED, 700, false, null, null));
      assertThat(client.barrier(sessionId, new BarrierRequest(0, 1, Pass.MAIN)).action())
          .isEqualTo(BarrierResponse.Action.WAIT);

      Fence yoursFence = client.claimOne(sessionId, 1, yours);
      client.result(
          sessionId,
          new ResultRequest(1, Pass.MAIN, yours, yoursFence, Outcome.PASSED, 800, false, null, null));
      // Two waiters for one unit of retry work: shard 1 is released.
      assertThat(client.barrier(sessionId, new BarrierRequest(1, 1, Pass.MAIN)).action())
          .isEqualTo(BarrierResponse.Action.DONE);

      first.getDockerClient().killContainerCmd(first.getContainerId()).exec();
    } finally {
      first.stop();
    }

    GenericContainer<?> second = CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      second.start();
      CoordinatorClient client = new CoordinatorClient(second);

      // The release survived: the failed unit is in shard 1's candidates, and it still
      // claims nothing.
      ClaimResponse releasedClaim =
          client.claim(sessionId, new ClaimRequest(1, Pass.RETRY1, CLASS_NAME, census));
      assertThat(releasedClaim.granted()).isEmpty();

      // The departure and shard 1's watermark survived: neither the ghost nor the released
      // shard holds the quorum, so the retained waiter is released into the retry pass.
      assertThat(client.barrier(sessionId, new BarrierRequest(0, 1, Pass.MAIN)).action())
          .isEqualTo(BarrierResponse.Action.RUN);

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
              750,
              false,
              null,
              null));

      SessionView view = client.view(sessionId);
      assertThat(
              view.shards().stream()
                  .filter(shard -> shard.shard() == 2)
                  .findFirst()
                  .orElseThrow()
                  .departed())
          .isTrue();
      assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.PASSED);
    } finally {
      second.stop();
    }
  }
}
