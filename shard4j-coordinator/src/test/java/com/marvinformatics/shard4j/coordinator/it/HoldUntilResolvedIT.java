package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.marvinformatics.shard4j.coordinator.core.CoverageVerdict;
import com.marvinformatics.shard4j.protocol.BarrierRequest;
import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.SessionVerdict;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Two shards, one test: the smallest fleet in which a shard can be starved, and the one
 * where releasing an idle shard too early is unrecoverable rather than merely wasteful.
 *
 * <p>The rule under test is that a shard is held until every outstanding lease has produced
 * a result, because until then the work may come back. Release the idle shard instead and
 * the last attempt is stranded on whichever shard happens to be holding it.
 */
@Tag("shard4j-harness")
class HoldUntilResolvedIT {

  private static final String CLASS_NAME = "com.example.orders.LoneIT";

  private static GenericContainer<?> coordinator;
  private static CoordinatorClient client;

  @BeforeAll
  static void start() throws IOException {
    coordinator =
        CoordinatorContainers.coordinator(
            Files.createTempDirectory(Path.of("target"), "hold-until-resolved-data"), Map.of());
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void givenOneTestAndTwoShards_whenItFailsOnTheFirst_thenTheIdleShardHeldLongEnoughToTakeIt() {
    String sessionId = UUID.randomUUID().toString();
    String only = Ids.method(CLASS_NAME, "lonely");
    List<String> census = List.of(only);
    client.register(sessionId, new RegisterRequest(0, 1, Map.of(), census, 2));
    client.register(sessionId, new RegisterRequest(1, 1, Map.of(), census, 2));

    // s1 takes the only unit.
    Fence first = client.claimOne(sessionId, 0, only);

    // s2 has nothing to do, and no decision exists on that unit yet -- so it is held, not
    // released. This is the assertion the whole design turns on: were s2 sent home here,
    // the retry below would have nowhere else to go.
    ClaimResponse nothingYet = client.claim(sessionId, new ClaimRequest(1, CLASS_NAME, census));
    assertThat(nothingYet.granted()).isEmpty();
    BarrierResponse heldWhileLeased = client.barrier(sessionId, new BarrierRequest(1, 1));
    assertThat(heldWhileLeased.action())
        .as("an outstanding lease that could still requeue must hold the idle shard")
        .isEqualTo(BarrierResponse.Action.WAIT);

    // s1 fails it. The unit requeues, and s2 -- which has been waiting the longest -- is
    // the one told to run.
    client.result(
        sessionId, new ResultRequest(0, only, first, Outcome.FAILED, 1_000, false, null, null));
    BarrierResponse releasedToRun = client.barrier(sessionId, new BarrierRequest(1, 1));
    assertThat(releasedToRun.action())
        .as("the longest-waiting shard gets first refusal on the requeued unit")
        .isEqualTo(BarrierResponse.Action.RUN);

    ClaimResponse retry = client.claim(sessionId, new ClaimRequest(1, CLASS_NAME, census));
    assertThat(retry.granted()).hasSize(1);
    Fence second = retry.granted().get(0).fence();

    // Now s1 is the one with nothing to do, and it is held in turn: s2's attempt could
    // fail, and a third attempt would need somewhere to land.
    assertThat(client.barrier(sessionId, new BarrierRequest(0, 1)).action())
        .as("the shard that just failed is now the spare capacity, and must be kept")
        .isEqualTo(BarrierResponse.Action.WAIT);

    // s2 passes it. Nothing is outstanding, so both shards are free to finish.
    client.result(
        sessionId, new ResultRequest(1, only, second, Outcome.PASSED, 1_200, false, null, null));
    assertThat(client.barrier(sessionId, new BarrierRequest(0, 1)).action())
        .isEqualTo(BarrierResponse.Action.DONE);
    assertThat(client.barrier(sessionId, new BarrierRequest(1, 1)).action())
        .isEqualTo(BarrierResponse.Action.DONE);
    client.depart(sessionId, new DepartRequest(0, 1));
    client.depart(sessionId, new DepartRequest(1, 1));

    SessionView view = client.view(sessionId);
    assertThat(CoordinatorClient.stateOf(view, only)).isEqualTo(TestState.PASSED);
    assertThat(view.tests().get(0).records())
        .as("attempt one failed on shard 0, attempt two passed on shard 1")
        .extracting(
            SessionView.RecordView::attempt,
            SessionView.RecordView::shard,
            SessionView.RecordView::outcome)
        .containsExactly(tuple(1, 0, Outcome.FAILED), tuple(2, 1, Outcome.PASSED));
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.PASSED);
  }

  @Test
  void givenALeaseOnItsFinalAttempt_whenAnIdleShardAsks_thenItIsStillHeldBecauseExpiryCanReturnIt() {
    String sessionId = UUID.randomUUID().toString();
    String only = Ids.method(CLASS_NAME, "doomed");
    List<String> census = List.of(only);
    client.register(sessionId, new RegisterRequest(0, 1, Map.of(), census, 2));
    client.register(sessionId, new RegisterRequest(1, 1, Map.of(), census, 2));

    // Spend the first two attempts, then leave the third outstanding on shard 0.
    for (int attempt = 1; attempt <= 2; attempt++) {
      Fence fence = client.claimOne(sessionId, 0, only);
      client.result(
          sessionId, new ResultRequest(0, only, fence, Outcome.FAILED, 500, false, null, null));
    }
    client.claimOne(sessionId, 0, only);

    // A final attempt still has to be held for. Releasing the spare here reads plausible --
    // that lease cannot requeue by failing -- but failing is not its only exit: expiry and
    // NACK both hand the unit back with the attempt un-spent. Release shard 1 now and a
    // SIGTERM on shard 0 a second later strands the unit with nobody left to poll.
    assertThat(client.barrier(sessionId, new BarrierRequest(1, 1)).action())
        .as("an outstanding lease can still return via expiry or NACK, so the spare is kept")
        .isEqualTo(BarrierResponse.Action.WAIT);

    // Proving it: shard 0 abandons the unit exactly as a deadline-killed job would, and the
    // shard that was nearly released is the one that finishes the run.
    client.nack(
        sessionId,
        new NackRequest(
            0,
            List.of(
                new NackRequest.NackedLease(
                    only, client.view(sessionId).tests().get(0).lease().fence(), "job deadline", false))));
    client.depart(sessionId, new DepartRequest(0, 1));

    assertThat(client.barrier(sessionId, new BarrierRequest(1, 1)).action())
        .isEqualTo(BarrierResponse.Action.RUN);
    ClaimResponse rescued = client.claim(sessionId, new ClaimRequest(1, CLASS_NAME, census));
    assertThat(rescued.granted()).hasSize(1);
    client.result(
        sessionId,
        new ResultRequest(
            1, only, rescued.granted().get(0).fence(), Outcome.PASSED, 600, false, null, null));

    SessionView view = client.view(sessionId);
    assertThat(CoordinatorClient.stateOf(view, only)).isEqualTo(TestState.PASSED);
    assertThat(CoverageVerdict.of(view))
        .as("a final-attempt lease abandoned by a dying shard must not cost the run")
        .isEqualTo(SessionVerdict.PASSED);
  }
}
