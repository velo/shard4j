package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.coordinator.core.CoverageVerdict;
import com.marvinformatics.shard4j.protocol.BarrierRequest;
import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.NackRequest;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * The pass barrier, driven over real HTTP: a retry pass must not start until every live
 * shard has finished the previous one -- otherwise the first finisher sees an empty
 * failure pool and the straggler retries its own failures alone, which is zero rebalance
 * -- and a shard is released the moment the coordinator knows it cannot be needed.
 */
class RetryBarrierIT {

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void start() throws IOException {
    coordinator =
        CoordinatorContainers.coordinator(
            Files.createTempDirectory(Path.of("target"), "retry-barrier-data"), Map.of());
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  private static RegisterRequest registration(int shard, List<String> census) {
    return new RegisterRequest(shard, 1, Map.of(), CoordinatorClient.hashOf(census), census);
  }

  private static ResultRequest result(
      int shard, Pass pass, String testId, Fence fence, Outcome outcome) {
    return new ResultRequest(shard, pass, testId, fence, outcome, 1_000, false, null, null);
  }

  private static BarrierResponse arrive(String sessionId, int shard, Pass completedPass) {
    return client.barrier(sessionId, new BarrierRequest(shard, completedPass));
  }

  @Test
  void givenAStragglerStillLeased_whenTheFirstFinisherArrives_thenItWaitsUntilQuorumThenRuns() {
    String sessionId = UUID.randomUUID().toString();
    String alphaClass = "com.example.orders.QuorumAlphaIT";
    String fast = Ids.method(alphaClass, "fast");
    String slow = Ids.method(alphaClass, "slow");
    List<String> census = List.of(fast, slow);
    client.register(sessionId, registration(0, census));
    client.register(sessionId, registration(1, census));

    Fence slowFence = client.claimOne(sessionId, 1, slow);
    Fence fastFence = client.claimOne(sessionId, 0, fast);
    client.result(sessionId, result(0, Pass.MAIN, fast, fastFence, Outcome.PASSED));

    // The straggler is still leased: its unit may yet fail, so nobody starts retry1.
    BarrierResponse waiting = arrive(sessionId, 0, Pass.MAIN);
    assertThat(waiting.action()).isEqualTo(BarrierResponse.Action.WAIT);
    assertThat(waiting.retryAfterSeconds()).isPositive();
    assertThat(waiting.earliestLeaseExpiry()).isNotNull();
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.WAIT);

    client.result(sessionId, result(1, Pass.MAIN, slow, slowFence, Outcome.FAILED));

    // The straggler's arrival completes the quorum -- and makes it surplus: two shards for
    // one unit of retry work, so the shard that just failed the unit goes home and the
    // held-back shard is the one that runs the retry. Maximal rebalance.
    assertThat(arrive(sessionId, 1, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.DONE);
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.RUN);
    ClaimResponse retry =
        client.claim(sessionId, new ClaimRequest(0, Pass.RETRY1, alphaClass, census));
    assertThat(retry.granted()).hasSize(1);
    assertThat(retry.granted().get(0).testId()).isEqualTo(slow);
  }

  /**
   * The rebalance itself, with a live concurrent shard: shard 0 runs the engine's loop in
   * its own thread -- claim, report, poll the barrier, only then claim the next pass --
   * while this thread plays a straggler whose unit fails. The failure must be retried by
   * shard 0, not by the shard that just failed it. Remove the barrier's quorum gate and
   * this goes red: shard 0 races through an empty failure pool and departs before the
   * straggler's failure ever lands in it.
   */
  @Test
  void givenAStragglersFailure_whenTheBarrierGatesTheRetryPass_thenAnotherShardRetriesIt()
      throws Exception {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.RebalanceIT";
    String steady = Ids.method(className, "steady");
    String flaky = Ids.method(className, "flakyOnTheStraggler");
    List<String> census = List.of(steady, flaky);
    client.register(sessionId, registration(0, census));
    client.register(sessionId, registration(1, census));

    // The straggler takes its unit first, so shard 0's main pass only finds the other.
    Fence flakyFence = client.claimOne(sessionId, 1, flaky);

    AtomicInteger waitsObserved = new AtomicInteger();
    AtomicReference<Throwable> shardFailure = new AtomicReference<>();
    Thread shard0 =
        new Thread(
            () -> engineLoop(sessionId, 0, census, waitsObserved),
            "engine-loop-shard-0");
    shard0.setUncaughtExceptionHandler((thread, failure) -> shardFailure.set(failure));
    shard0.start();

    // Let shard 0 finish its main pass and start polling the barrier.
    Thread.sleep(1_500);
    client.result(sessionId, result(1, Pass.MAIN, flaky, flakyFence, Outcome.FAILED));

    // The straggler's arrival completes the quorum and makes it surplus (two shards, one
    // unit of retry work): released on the spot, its own failure handed to the other shard.
    BarrierResponse afterFailure = arrive(sessionId, 1, Pass.MAIN);
    assertThat(afterFailure.action()).isEqualTo(BarrierResponse.Action.DONE);
    ClaimResponse stragglerRetry =
        client.claim(sessionId, new ClaimRequest(1, Pass.RETRY1, className, census));
    assertThat(stragglerRetry.granted()).isEmpty();
    client.depart(sessionId, new DepartRequest(1));

    shard0.join(30_000);
    assertThat(shard0.isAlive()).as("shard 0's engine loop must terminate").isFalse();
    assertThat(shardFailure.get()).isNull();
    assertThat(waitsObserved.get())
        .as("shard 0 must have been held at the barrier while the straggler still ran")
        .isPositive();

    SessionView view = client.view(sessionId);
    assertThat(CoordinatorClient.stateOf(view, flaky)).isEqualTo(TestState.PASSED);
    SessionView.RecordView retryRecord =
        view.tests().stream()
            .filter(test -> test.testId().equals(flaky))
            .findFirst()
            .orElseThrow()
            .records()
            .stream()
            .filter(record -> record.pass() == Pass.RETRY1)
            .findFirst()
            .orElseThrow();
    assertThat(retryRecord.shard())
        .as("the straggler's failure must rebalance to the shard that was held back")
        .isEqualTo(0);
    assertThat(retryRecord.outcome()).isEqualTo(Outcome.PASSED);
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.PASSED);
  }

  @Test
  void givenAnAllGreenMainPass_whenTheLastShardArrives_thenEveryShardIsReleased() {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.AllGreenIT";
    String first = Ids.method(className, "first");
    String second = Ids.method(className, "second");
    List<String> census = List.of(first, second);
    client.register(sessionId, registration(0, census));
    client.register(sessionId, registration(1, census));

    Fence secondFence = client.claimOne(sessionId, 1, second);
    Fence firstFence = client.claimOne(sessionId, 0, first);
    client.result(sessionId, result(0, Pass.MAIN, first, firstFence, Outcome.PASSED));

    // While the other shard's unit is still leased it may yet fail, so no release.
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.WAIT);

    client.result(sessionId, result(1, Pass.MAIN, second, secondFence, Outcome.PASSED));

    // Nothing can ever land in a retry pool now, so both shards go home immediately.
    assertThat(arrive(sessionId, 1, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.DONE);
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.DONE);

    // Release is sticky, and a released shard always receives an empty grant.
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.DONE);
    ClaimResponse afterRelease =
        client.claim(sessionId, new ClaimRequest(0, Pass.RETRY1, className, census));
    assertThat(afterRelease.granted()).isEmpty();
  }

  @Test
  void givenMoreWaitersThanRetryWork_whenAShardPolls_thenTheExcessShardIsReleased() {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.ExcessWaitersIT";
    String mine = Ids.method(className, "mine");
    String yours = Ids.method(className, "yours");
    String stragglers = Ids.method(className, "stragglers");
    List<String> census = List.of(mine, yours, stragglers);
    client.register(sessionId, registration(0, census));
    client.register(sessionId, registration(1, census));
    client.register(sessionId, registration(2, census));

    Fence stragglersFence = client.claimOne(sessionId, 2, stragglers);
    Fence mineFence = client.claimOne(sessionId, 0, mine);
    client.result(sessionId, result(0, Pass.MAIN, mine, mineFence, Outcome.PASSED));
    Fence yoursFence = client.claimOne(sessionId, 1, yours);
    client.result(sessionId, result(1, Pass.MAIN, yours, yoursFence, Outcome.PASSED));

    // One leased unit can produce at most one retry; the first waiter must stay.
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.WAIT);
    // The second waiter makes two shards for at most one unit of work: released.
    assertThat(arrive(sessionId, 1, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.DONE);
    // The remaining waiter is still needed and keeps waiting.
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.WAIT);

    client.result(sessionId, result(2, Pass.MAIN, stragglers, stragglersFence, Outcome.FAILED));
    // The straggler's arrival leaves two waiters for one unit of work: it is surplus too.
    assertThat(arrive(sessionId, 2, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.DONE);
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.RUN);

    // The released shard claims nothing, even with the failed unit in its candidates.
    ClaimResponse releasedClaim =
        client.claim(sessionId, new ClaimRequest(1, Pass.RETRY1, className, census));
    assertThat(releasedClaim.granted()).isEmpty();

    // The retained waiter is the one that picks the failure up.
    ClaimResponse retainedClaim =
        client.claim(sessionId, new ClaimRequest(0, Pass.RETRY1, className, census));
    assertThat(retainedClaim.granted()).hasSize(1);
    assertThat(retainedClaim.granted().get(0).testId()).isEqualTo(stragglers);
  }

  /**
   * A shard whose deadline expires at a barrier departs and exits 0; the coordinator must
   * drop it from the quorum or the remaining shards wait for a ghost forever.
   */
  @Test
  void givenADepartedShard_whenTheBarrierEvaluatesItsQuorum_thenTheGhostDoesNotHoldIt() {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.DeadlineDepartIT";
    String flaky = Ids.method(className, "flaky");
    String stranded = Ids.method(className, "stranded");
    List<String> census = List.of(flaky, stranded);
    client.register(sessionId, registration(0, census));
    client.register(sessionId, registration(1, census));

    Fence flakyFence = client.claimOne(sessionId, 0, flaky);
    client.result(sessionId, result(0, Pass.MAIN, flaky, flakyFence, Outcome.FAILED));

    // Shard 1 is mid-pass, so the failure's retry must wait for it.
    Fence strandedFence = client.claimOne(sessionId, 1, stranded);
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.WAIT);

    // Shard 1 hits its deadline: it NACKs what it cannot finish and announces departure.
    client.nack(
        sessionId,
        new NackRequest(
            1, List.of(new NackRequest.NackedLease(stranded, strandedFence, "job deadline"))));
    client.depart(sessionId, new DepartRequest(1));

    // The departed shard no longer holds the barrier; the survivor runs the retry alone.
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.RUN);
    ClaimResponse retry =
        client.claim(sessionId, new ClaimRequest(0, Pass.RETRY1, className, census));
    assertThat(retry.granted()).hasSize(1);
    client.result(
        sessionId,
        result(0, Pass.RETRY1, flaky, retry.granted().get(0).fence(), Outcome.PASSED));

    // The NACKed unit is back in the main pool with every live shard past main: it is
    // stranded, must not hold the next barrier, and names the session INCOMPLETE.
    assertThat(arrive(sessionId, 0, Pass.RETRY1).action()).isEqualTo(BarrierResponse.Action.DONE);
    client.depart(sessionId, new DepartRequest(0));
    SessionView view = client.view(sessionId);
    assertThat(CoordinatorClient.stateOf(view, stranded)).isEqualTo(TestState.PENDING);
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.INCOMPLETE);
  }

  @Test
  void givenTheLastRetryPass_whenAShardCompletesIt_thenTheBarrierAnswersDone() {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.LastPassIT";
    String only = Ids.method(className, "only");
    List<String> census = List.of(only);
    client.register(sessionId, registration(0, census));
    Fence fence = client.claimOne(sessionId, 0, only);
    client.result(sessionId, result(0, Pass.MAIN, only, fence, Outcome.PASSED));

    assertThat(arrive(sessionId, 0, Pass.RETRY2).action()).isEqualTo(BarrierResponse.Action.DONE);
  }

  /**
   * DONE means stop pulling, so a released shard stops polling; nothing normative ever
   * brings it back to a barrier. Its watermark is frozen at MAIN forever -- it can produce
   * no retry work and holds no lease for expiry to depart -- so a quorum that counts it
   * can never close once the fleet moves past MAIN.
   */
  @Test
  void givenAReleasedShardThatStopsPolling_whenARetryFails_thenLaterQuorumsAdvanceWithoutIt() {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.ReleasedGoesSilentIT";
    String mine = Ids.method(className, "mine");
    String yours = Ids.method(className, "yours");
    String flaky = Ids.method(className, "flaky");
    List<String> census = List.of(mine, yours, flaky);
    client.register(sessionId, registration(0, census));
    client.register(sessionId, registration(1, census));
    client.register(sessionId, registration(2, census));

    Fence flakyFence = client.claimOne(sessionId, 2, flaky);
    Fence mineFence = client.claimOne(sessionId, 0, mine);
    client.result(sessionId, result(0, Pass.MAIN, mine, mineFence, Outcome.PASSED));
    Fence yoursFence = client.claimOne(sessionId, 1, yours);
    client.result(sessionId, result(1, Pass.MAIN, yours, yoursFence, Outcome.PASSED));

    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.WAIT);
    // Two waiters for at most one unit of retry work: shard 1 is surplus, released, and
    // from here on it is never heard from again -- no poll, no depart.
    assertThat(arrive(sessionId, 1, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.DONE);

    client.result(sessionId, result(2, Pass.MAIN, flaky, flakyFence, Outcome.FAILED));
    assertThat(arrive(sessionId, 2, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.DONE);
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.RUN);

    ClaimResponse retry1 =
        client.claim(sessionId, new ClaimRequest(0, Pass.RETRY1, className, census));
    assertThat(retry1.granted()).hasSize(1);
    client.result(
        sessionId, result(0, Pass.RETRY1, flaky, retry1.granted().get(0).fence(), Outcome.FAILED));

    // The retry1 quorum must resolve from the survivor alone: the released shard's frozen
    // MAIN watermark must not hold it, or retry2 never runs and the fleet burns to its
    // deadlines.
    assertThat(arrive(sessionId, 0, Pass.RETRY1).action()).isEqualTo(BarrierResponse.Action.RUN);
    ClaimResponse retry2 =
        client.claim(sessionId, new ClaimRequest(0, Pass.RETRY2, className, census));
    assertThat(retry2.granted()).hasSize(1);
    client.result(
        sessionId, result(0, Pass.RETRY2, flaky, retry2.granted().get(0).fence(), Outcome.PASSED));
    assertThat(arrive(sessionId, 0, Pass.RETRY2).action()).isEqualTo(BarrierResponse.Action.DONE);

    client.depart(sessionId, new DepartRequest(0));
    SessionView view = client.view(sessionId);
    assertThat(CoordinatorClient.stateOf(view, flaky)).isEqualTo(TestState.PASSED);
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.PASSED);
  }

  /**
   * A shard that finishes green, polls once and dies silently holds no lease, so lease
   * expiry -- the usual silent-death backstop -- can never fire for it. Only the mandated
   * poll cadence says it is gone. Its stale watermark must not count it as a waiter,
   * or the straggler that failed a unit is released as surplus against a corpse and the
   * failure is never retried by anyone.
   */
  @Test
  void givenAWaiterDiesSilentlyAtTheBarrier_whenTheStragglerArrives_thenItIsRetainedAndRuns()
      throws Exception {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.DeadWaiterIT";
    String green = Ids.method(className, "green");
    String flaky = Ids.method(className, "flakyOnTheStraggler");
    List<String> census = List.of(green, flaky);
    client.register(sessionId, registration(0, census));
    client.register(sessionId, registration(1, census));

    // The straggler is busy with its unit for this whole scenario.
    Fence flakyFence = client.claimOne(sessionId, 1, flaky);

    Fence greenFence = client.claimOne(sessionId, 0, green);
    client.result(sessionId, result(0, Pass.MAIN, green, greenFence, Outcome.PASSED));
    assertThat(arrive(sessionId, 0, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.WAIT);
    // That poll was the last thing shard 0 ever said.

    // The poll cadence is the liveness signal: once shard 0 has been silent for longer
    // than the tolerated number of missed polls, the coordinator presumes it dead.
    Instant deadline = Instant.now().plusSeconds(45);
    while (client.view(sessionId).shards().stream()
        .noneMatch(shard -> shard.shard() == 0 && shard.departed())) {
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("Timed out waiting for the silent waiter to be presumed dead");
      }
      Thread.sleep(500);
    }

    client.result(sessionId, result(1, Pass.MAIN, flaky, flakyFence, Outcome.FAILED));
    // The straggler is the only live shard: it must be retained and run its own retry,
    // not be released against the corpse's watermark.
    assertThat(arrive(sessionId, 1, Pass.MAIN).action()).isEqualTo(BarrierResponse.Action.RUN);

    ClaimResponse retry =
        client.claim(sessionId, new ClaimRequest(1, Pass.RETRY1, className, census));
    assertThat(retry.granted()).hasSize(1);
    assertThat(retry.granted().get(0).testId()).isEqualTo(flaky);
    client.result(
        sessionId, result(1, Pass.RETRY1, flaky, retry.granted().get(0).fence(), Outcome.PASSED));
    assertThat(arrive(sessionId, 1, Pass.RETRY1).action()).isEqualTo(BarrierResponse.Action.DONE);

    client.depart(sessionId, new DepartRequest(1));
    SessionView view = client.view(sessionId);
    assertThat(CoordinatorClient.stateOf(view, flaky)).isEqualTo(TestState.PASSED);
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.PASSED);
  }

  @Test
  void givenMalformedOrUnknownBarrierCalls_whenPosted_thenRejectedWithoutSideEffects() {
    String unknownSession = UUID.randomUUID().toString();
    CoordinatorClient.RawResponse unknown =
        client.barrierRaw(unknownSession, new BarrierRequest(0, Pass.MAIN));
    assertThat(unknown.status()).isEqualTo(404);

    String sessionId = UUID.randomUUID().toString();
    String only = Ids.method("com.example.orders.MalformedBarrierIT", "only");
    client.register(sessionId, registration(0, List.of(only)));
    CoordinatorClient.RawResponse missingPass =
        client.barrierRaw(sessionId, Map.of("shard", 0));
    assertThat(missingPass.status()).isEqualTo(400);
  }

  /**
   * The engine's loop, faithfully: claim and report the current pass, arrive at the
   * barrier, poll while told to wait, run the next pass only on RUN -- and on DONE stop
   * pulling entirely, exactly as {@link BarrierResponse}'s contract says. A released
   * shard never arrives at another barrier, so nothing here may depend on it doing so.
   */
  private static void engineLoop(
      String sessionId, int shard, List<String> census, AtomicInteger waitsObserved) {
    for (Pass pass : Pass.values()) {
      for (Map.Entry<String, List<String>> byClass : byClass(census).entrySet()) {
        ClaimResponse claimed =
            client.claim(
                sessionId,
                new ClaimRequest(shard, pass, byClass.getKey(), byClass.getValue()));
        for (Grant grant : claimed.granted()) {
          client.result(
              sessionId, result(shard, pass, grant.testId(), grant.fence(), Outcome.PASSED));
        }
      }
      BarrierResponse response;
      while ((response = client.barrier(sessionId, new BarrierRequest(shard, pass))).action()
          == BarrierResponse.Action.WAIT) {
        waitsObserved.incrementAndGet();
        sleep(200);
      }
      if (response.action() == BarrierResponse.Action.DONE) {
        break;
      }
    }
    client.depart(sessionId, new DepartRequest(shard));
  }

  private static Map<String, List<String>> byClass(List<String> census) {
    Map<String, List<String>> classes = new LinkedHashMap<>();
    for (String testId : census) {
      classes.computeIfAbsent(Ids.classNameOf(testId), key -> new ArrayList<>()).add(testId);
    }
    return classes;
  }


  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
