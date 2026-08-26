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
import com.marvinformatics.shard4j.protocol.Grant;
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
    return new RegisterRequest(shard, 1, Map.of(), census, null);
  }

  private static ResultRequest result(
      int shard, String testId, Fence fence, Outcome outcome) {
    return new ResultRequest(shard, testId, fence, outcome, 1_000, false, null, null);
  }

  private static BarrierResponse arrive(String sessionId, int shard) {
    return client.barrier(sessionId, new BarrierRequest(shard, 1));
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
    client.result(sessionId, result(0, fast, fastFence, Outcome.PASSED));

    // The straggler is still leased: its unit may yet fail, so nobody starts retry1.
    BarrierResponse waiting = arrive(sessionId, 0);
    assertThat(waiting.action()).isEqualTo(BarrierResponse.Action.WAIT);
    assertThat(waiting.retryAfterSeconds()).isPositive();
    assertThat(waiting.earliestLeaseExpiry()).isNotNull();
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.WAIT);

    client.result(sessionId, result(1, slow, slowFence, Outcome.FAILED));

    // The straggler's arrival completes the quorum -- and makes it surplus: two shards for
    // one unit of retry work, so the shard that just failed the unit goes home and the
    // held-back shard is the one that runs the retry. Maximal rebalance.
    assertThat(arrive(sessionId, 1).action()).isEqualTo(BarrierResponse.Action.DONE);
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.RUN);
    ClaimResponse retry =
        client.claim(sessionId, new ClaimRequest(0, alphaClass, census));
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
  void givenAFailureOnOneShard_whenAnotherShardAsks_thenTheRetryLandsOnTheOtherShard() {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.RebalanceIT";
    String steady = Ids.method(className, "steady");
    String flaky = Ids.method(className, "flakyOnTheStraggler");
    List<String> census = List.of(steady, flaky);
    client.register(sessionId, registration(0, census));
    client.register(sessionId, registration(1, census));

    // Shard 1 takes the flaky unit and fails it. There is no pass to wait for: the unit
    // is back on the queue before this call returns.
    Fence flakyFence = client.claimOne(sessionId, 1, flaky);
    client.result(sessionId, result(1, flaky, flakyFence, Outcome.FAILED));

    // Shard 0 asks next and gets it -- the retry crosses shards by ordering alone, with no
    // barrier gating it and no wait for the rest of the fleet to finish a pass.
    ClaimResponse retry = client.claim(sessionId, new ClaimRequest(0, className, census));
    assertThat(retry.granted())
        .as("the requeued unit must be immediately available to a different shard")
        .extracting(Grant::testId)
        .contains(flaky);

    Fence retryFence =
        retry.granted().stream()
            .filter(grant -> grant.testId().equals(flaky))
            .findFirst()
            .orElseThrow()
            .fence();
    client.result(sessionId, result(0, flaky, retryFence, Outcome.PASSED));

    SessionView view = client.view(sessionId);
    SessionView.TestView recovered =
        view.tests().stream()
            .filter(test -> test.testId().equals(flaky))
            .findFirst()
            .orElseThrow();
    assertThat(recovered.state()).isEqualTo(TestState.PASSED);
    assertThat(recovered.records())
        .as("attempt one failed on shard 1, attempt two passed on shard 0")
        .extracting(
            SessionView.RecordView::attempt,
            SessionView.RecordView::shard,
            SessionView.RecordView::outcome)
        .containsExactly(tuple(1, 1, Outcome.FAILED), tuple(2, 0, Outcome.PASSED));
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
    client.result(sessionId, result(0, first, firstFence, Outcome.PASSED));

    // While the other shard's unit is still leased it may yet fail, so no release.
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.WAIT);

    client.result(sessionId, result(1, second, secondFence, Outcome.PASSED));

    // Nothing can ever land in a retry pool now, so both shards go home immediately.
    assertThat(arrive(sessionId, 1).action()).isEqualTo(BarrierResponse.Action.DONE);
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.DONE);

    // Release is sticky, and a released shard always receives an empty grant.
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.DONE);
    ClaimResponse afterRelease =
        client.claim(sessionId, new ClaimRequest(0, className, census));
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
    client.result(sessionId, result(0, mine, mineFence, Outcome.PASSED));
    Fence yoursFence = client.claimOne(sessionId, 1, yours);
    client.result(sessionId, result(1, yours, yoursFence, Outcome.PASSED));

    // One leased unit can produce at most one retry; the first waiter must stay.
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.WAIT);
    // The second waiter makes two shards for at most one unit of work: released.
    assertThat(arrive(sessionId, 1).action()).isEqualTo(BarrierResponse.Action.DONE);
    // The remaining waiter is still needed and keeps waiting.
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.WAIT);

    client.result(sessionId, result(2, stragglers, stragglersFence, Outcome.FAILED));
    // The straggler's arrival leaves two waiters for one unit of work: it is surplus too.
    assertThat(arrive(sessionId, 2).action()).isEqualTo(BarrierResponse.Action.DONE);
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.RUN);

    // The released shard claims nothing, even with the failed unit in its candidates.
    ClaimResponse releasedClaim =
        client.claim(sessionId, new ClaimRequest(1, className, census));
    assertThat(releasedClaim.granted()).isEmpty();

    // The retained waiter is the one that picks the failure up.
    ClaimResponse retainedClaim =
        client.claim(sessionId, new ClaimRequest(0, className, census));
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
    client.result(sessionId, result(0, flaky, flakyFence, Outcome.FAILED));

    // Shard 0 can retry its own failure immediately; it does not wait for shard 1.
    Fence strandedFence = client.claimOne(sessionId, 1, stranded);
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.RUN);

    // Shard 1 hits its deadline: it NACKs what it cannot finish and announces departure.
    client.nack(
        sessionId,
        new NackRequest(
            1, List.of(new NackRequest.NackedLease(stranded, strandedFence, "job deadline", false))));
    client.depart(sessionId, new DepartRequest(1, 1));

    // The departed shard no longer holds the barrier, and the survivor inherits both its
    // own retry and the work the ghost abandoned. Under pass-gated retries the NACKed unit
    // went back to a pool every live shard had already moved past, so it was stranded and
    // the run was INCOMPLETE however healthy the survivor was. With no pools, PENDING is
    // PENDING: whoever is still alive picks it up.
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.RUN);
    ClaimResponse retry = client.claim(sessionId, new ClaimRequest(0, className, census));
    assertThat(retry.granted())
        .as("the survivor takes its own requeued failure and the ghost's abandoned unit")
        .extracting(Grant::testId)
        .containsExactlyInAnyOrder(flaky, stranded);
    for (Grant grant : retry.granted()) {
      client.result(sessionId, result(0, grant.testId(), grant.fence(), Outcome.PASSED));
    }

    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.DONE);
    client.depart(sessionId, new DepartRequest(0, 1));
    SessionView view = client.view(sessionId);
    assertThat(CoordinatorClient.stateOf(view, stranded)).isEqualTo(TestState.PASSED);
    assertThat(CoverageVerdict.of(view))
        .as("a shard leaving mid-flight no longer costs the run its verdict")
        .isEqualTo(SessionVerdict.PASSED);
  }

  @Test
  void givenTheLastRetryPass_whenAShardCompletesIt_thenTheBarrierAnswersDone() {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.LastPassIT";
    String only = Ids.method(className, "only");
    List<String> census = List.of(only);
    client.register(sessionId, registration(0, census));
    Fence fence = client.claimOne(sessionId, 0, only);
    client.result(sessionId, result(0, only, fence, Outcome.PASSED));

    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.DONE);
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
    client.result(sessionId, result(0, mine, mineFence, Outcome.PASSED));
    Fence yoursFence = client.claimOne(sessionId, 1, yours);
    client.result(sessionId, result(1, yours, yoursFence, Outcome.PASSED));

    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.WAIT);
    // Two waiters for at most one unit of retry work: shard 1 is surplus, released, and
    // from here on it is never heard from again -- no poll, no depart.
    assertThat(arrive(sessionId, 1).action()).isEqualTo(BarrierResponse.Action.DONE);

    client.result(sessionId, result(2, flaky, flakyFence, Outcome.FAILED));
    assertThat(arrive(sessionId, 2).action()).isEqualTo(BarrierResponse.Action.DONE);
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.RUN);

    ClaimResponse retry1 =
        client.claim(sessionId, new ClaimRequest(0, className, census));
    assertThat(retry1.granted()).hasSize(1);
    client.result(
        sessionId, result(0, flaky, retry1.granted().get(0).fence(), Outcome.FAILED));

    // The retry1 quorum must resolve from the survivor alone: the released shard's frozen
    // MAIN watermark must not hold it, or retry2 never runs and the fleet burns to its
    // deadlines.
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.RUN);
    ClaimResponse retry2 =
        client.claim(sessionId, new ClaimRequest(0, className, census));
    assertThat(retry2.granted()).hasSize(1);
    client.result(
        sessionId, result(0, flaky, retry2.granted().get(0).fence(), Outcome.PASSED));
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.DONE);

    client.depart(sessionId, new DepartRequest(0, 1));
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
    client.result(sessionId, result(0, green, greenFence, Outcome.PASSED));
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.WAIT);
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

    client.result(sessionId, result(1, flaky, flakyFence, Outcome.FAILED));
    // The straggler is the only live shard: it must be retained and run its own retry,
    // not be released against the corpse's watermark.
    assertThat(arrive(sessionId, 1).action()).isEqualTo(BarrierResponse.Action.RUN);

    ClaimResponse retry =
        client.claim(sessionId, new ClaimRequest(1, className, census));
    assertThat(retry.granted()).hasSize(1);
    assertThat(retry.granted().get(0).testId()).isEqualTo(flaky);
    client.result(
        sessionId, result(1, flaky, retry.granted().get(0).fence(), Outcome.PASSED));
    assertThat(arrive(sessionId, 1).action()).isEqualTo(BarrierResponse.Action.DONE);

    client.depart(sessionId, new DepartRequest(1, 1));
    SessionView view = client.view(sessionId);
    assertThat(CoordinatorClient.stateOf(view, flaky)).isEqualTo(TestState.PASSED);
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.PASSED);
  }

  /**
   * After an epoch bump the previous attempt's shards are known-dead. A zombie of that
   * attempt still polling must be fenced out exactly as its result writes are: an accepted
   * barrier arrival mutates the roster, and would resurrect the zombie into the waiter
   * tally and the quorum of an attempt it is not part of.
   */
  @Test
  void givenAnEpochBump_whenAZombiePollsTheBarrierOrDeparts_thenItIsFencedOutAndStaysDead() {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.ZombieBarrierIT";
    String early = Ids.method(className, "early");
    String late = Ids.method(className, "late");
    List<String> census = List.of(early, late);
    client.register(sessionId, registration(0, census));
    client.register(sessionId, registration(1, census));

    Fence earlyFence = client.claimOne(sessionId, 0, early);
    client.result(sessionId, result(0, early, earlyFence, Outcome.PASSED));
    // "late" is still unclaimed, so there is work: RUN, not WAIT. The fencing this test
    // exists for begins at the epoch bump below.
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.RUN);

    // A registration at a higher attempt: the old shards are known-dead, the epoch moves on.
    client.register(
        sessionId, new RegisterRequest(5, 2, Map.of(), census, null));

    CoordinatorClient.RawResponse zombiePoll =
        client.barrierRaw(sessionId, new BarrierRequest(0, 1));
    assertThat(zombiePoll.status()).isEqualTo(409);
    CoordinatorClient.RawResponse zombieDepart =
        client.departRaw(sessionId, new DepartRequest(0, 1));
    assertThat(zombieDepart.status()).isEqualTo(409);

    SessionView view = client.view(sessionId);
    assertThat(
            view.shards().stream().filter(shard -> shard.shard() == 0).findFirst().orElseThrow().departed())
        .as("the fenced zombie must not be resurrected into the roster")
        .isTrue();
    assertThat(
            view.shards().stream().filter(shard -> shard.shard() == 5).findFirst().orElseThrow().departed())
        .isFalse();
  }

  /**
   * An explicit departure is goodbye: the only barrier packet that can follow it is a
   * delayed or duplicated one, and accepting its side effects would revive a shard that
   * will never poll again -- permanently, since nothing departs a shard that holds no
   * lease and counts in no cadence.
   */
  @Test
  void givenAnExplicitlyDepartedShard_whenADelayedBarrierPostLands_thenItIsNotResurrected() {
    String sessionId = UUID.randomUUID().toString();
    String className = "com.example.orders.DelayedPacketIT";
    String green = Ids.method(className, "green");
    String flaky = Ids.method(className, "flaky");
    List<String> census = List.of(green, flaky);
    client.register(sessionId, registration(0, census));
    client.register(sessionId, registration(1, census));

    Fence flakyFence = client.claimOne(sessionId, 1, flaky);
    Fence greenFence = client.claimOne(sessionId, 0, green);
    client.result(sessionId, result(0, green, greenFence, Outcome.PASSED));
    assertThat(arrive(sessionId, 0).action()).isEqualTo(BarrierResponse.Action.WAIT);
    client.depart(sessionId, new DepartRequest(0, 1));

    // The delayed packet: same epoch, landing after the goodbye.
    arrive(sessionId, 0);
    assertThat(
            client.view(sessionId).shards().stream()
                .filter(shard -> shard.shard() == 0)
                .findFirst()
                .orElseThrow()
                .departed())
        .isTrue();

    // The straggler must be retained and run its own retry -- were the corpse revived, its
    // MAIN watermark would count as a waiter and the straggler would be released instead.
    client.result(sessionId, result(1, flaky, flakyFence, Outcome.FAILED));
    assertThat(arrive(sessionId, 1).action()).isEqualTo(BarrierResponse.Action.RUN);
    ClaimResponse retry =
        client.claim(sessionId, new ClaimRequest(1, className, census));
    assertThat(retry.granted()).hasSize(1);
    assertThat(retry.granted().get(0).testId()).isEqualTo(flaky);
  }

  @Test
  void givenMalformedOrUnknownBarrierCalls_whenPosted_thenRejectedWithoutSideEffects() {
    String unknownSession = UUID.randomUUID().toString();
    CoordinatorClient.RawResponse unknown =
        client.barrierRaw(unknownSession, new BarrierRequest(0, 1));
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
  /**
   * One drain loop, not three passes: claim whatever is claimable, report it, and go back
   * for more. A failure the coordinator requeues simply reappears as claimable on a later
   * ask, so a retry needs no second trip round an outer loop -- which is the whole point
   * of the model this exercises. The loop ends when the barrier says the shard cannot be
   * needed again.
   */
  private static void engineLoop(
      String sessionId, int shard, List<String> census, AtomicInteger waitsObserved) {
    while (true) {
      boolean claimedAnything = false;
      for (Map.Entry<String, List<String>> byClass : byClass(census).entrySet()) {
        ClaimResponse claimed =
            client.claim(
                sessionId, new ClaimRequest(shard, byClass.getKey(), byClass.getValue()));
        for (Grant grant : claimed.granted()) {
          claimedAnything = true;
          client.result(sessionId, result(shard, grant.testId(), grant.fence(), Outcome.PASSED));
        }
      }
      if (claimedAnything) {
        continue;
      }
      BarrierResponse response =
          client.barrier(sessionId, new BarrierRequest(shard, 1));
      if (response.action() == BarrierResponse.Action.WAIT) {
        waitsObserved.incrementAndGet();
        sleep(200);
        continue;
      }
      break;
    }
    client.depart(sessionId, new DepartRequest(shard, 1));
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
