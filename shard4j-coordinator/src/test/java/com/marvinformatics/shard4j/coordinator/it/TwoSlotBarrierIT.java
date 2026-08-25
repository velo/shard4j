package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.BarrierRequest;
import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.NextClassRequest;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
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
 * The barrier arithmetic under in-shard parallelism, over real HTTP: a shard draining two
 * classes at once holds two leases from two open asks, and each of them counts as work
 * that may yet fail -- so another shard arriving at the barrier waits while either drain
 * is still in flight, exactly as it would for two single-slot shards, and a failure from
 * the second drain still lands in the retry pool for whichever shard asks first.
 */
class TwoSlotBarrierIT {

  private static final String ALPHA_CLASS = "com.example.orders.TwoSlotAlphaIT";
  private static final String BETA_CLASS = "com.example.orders.TwoSlotBetaIT";

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void start() throws IOException {
    coordinator =
        CoordinatorContainers.coordinator(
            Files.createTempDirectory(Path.of("target"), "two-slot-barrier-data"), Map.of());
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  private static BarrierResponse arrive(String sessionId, int shard, Pass completedPass) {
    return client.barrier(sessionId, new BarrierRequest(shard, 1, completedPass));
  }

  @Test
  void givenAShardWithTwoDrainsInFlight_whenAnotherShardArrives_thenItWaitsForBothDrains() {
    String sessionId = UUID.randomUUID().toString();
    String alpha = Ids.method(ALPHA_CLASS, "alpha");
    String beta = Ids.method(BETA_CLASS, "beta");
    List<String> census = List.of(alpha, beta);
    client.register(sessionId, new RegisterRequest(0, 1, Map.of(), census, null));
    client.register(sessionId, new RegisterRequest(1, 1, Map.of(), census, null));

    // Shard 0's two slots: two open asks, two different classes, both leases outstanding.
    NextClassResponse firstAsk =
        client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    NextClassResponse secondAsk =
        client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    assertThat(firstAsk.granted()).hasSize(1);
    assertThat(secondAsk.granted()).hasSize(1);
    assertThat(secondAsk.className()).isNotEqualTo(firstAsk.className());

    // Nothing left for shard 1; it finishes its (empty) pass and arrives at the barrier.
    NextClassResponse nothingLeft =
        client.next(sessionId, new NextClassRequest(1, Pass.MAIN));
    assertThat(nothingLeft.granted()).isEmpty();
    BarrierResponse bothBusy = arrive(sessionId, 1, Pass.MAIN);
    assertThat(bothBusy.action()).isEqualTo(BarrierResponse.Action.WAIT);
    assertThat(bothBusy.earliestLeaseExpiry()).isNotNull();

    // One drain resolves; the other slot still holds work, so the barrier still waits.
    Grant firstGrant = firstAsk.granted().get(0);
    client.result(
        sessionId,
        new ResultRequest(
            0, Pass.MAIN, firstGrant.testId(), firstGrant.fence(), Outcome.PASSED, 1_000, true,
            null, null));
    BarrierResponse oneStillBusy = arrive(sessionId, 1, Pass.MAIN);
    assertThat(oneStillBusy.action()).isEqualTo(BarrierResponse.Action.WAIT);

    // The second drain fails its unit. Shard 0 arrives only now -- a shard never reaches
    // the barrier while a slot still holds work -- and with two waiters for one retry
    // unit the early-release rule lets it go; the remaining waiter runs the retry.
    Grant secondGrant = secondAsk.granted().get(0);
    client.result(
        sessionId,
        new ResultRequest(
            0, Pass.MAIN, secondGrant.testId(), secondGrant.fence(), Outcome.FAILED, 1_000, false,
            null, null));
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.DONE);
    assertThat(arrive(sessionId, 1, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.RUN);

    // The second slot's failure is ordinary retry-pool work: the other shard claims it.
    String failedId = secondGrant.testId();
    ClaimResponse retryClaim =
        client.claim(
            sessionId,
            new ClaimRequest(1, Pass.RETRY1, Ids.classNameOf(failedId), List.of(failedId)));
    assertThat(retryClaim.granted()).hasSize(1);
  }
}
