package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.coordinator.core.CoverageVerdict;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.RegisterResponse;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.SessionVerdict;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * The single-pass session loop, driven over real HTTP against the containerised service:
 * register, claim, report, read, verdict.
 */
class SessionLoopIT {

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void start() throws IOException {
    coordinator =
        CoordinatorContainers.coordinator(
            Files.createTempDirectory(Path.of("target"), "session-loop-data"), Map.of());
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  private static final String ALPHA = "com.example.orders.AlphaIT";
  private static final String BETA = "com.example.orders.BetaIT";
  private static final String GAMMA = "com.example.orders.GammaIT";

  private static List<String> census() {
    return census("rows");
  }

  // The container's duration store is shared across these tests, and a template a prior
  // test completed acquires an invocation plan -- so any test that pins whole-template
  // semantics registers its own template method, keeping its history virgin.
  private static List<String> census(String rowsMethod) {
    return List.of(
        Ids.method(ALPHA, "first"),
        Ids.method(ALPHA, "second"),
        Ids.template(BETA, rowsMethod + "(java.lang.String)"),
        Ids.method(GAMMA, "disabledUpstream"),
        Ids.method(GAMMA, "needsLocalService"));
  }

  private static RegisterRequest registration(int shard, int attempt) {
    return registration(shard, attempt, "rows");
  }

  private static RegisterRequest registration(int shard, int attempt, String rowsMethod) {
    return new RegisterRequest(
        shard,
        attempt,
        Map.of("ci", "example-ci", "run", "42"),
        census(rowsMethod), null);
  }

  private static ResultRequest passed(int shard, String testId, Fence fence, long durationMs) {
    return new ResultRequest(shard, testId, fence, Outcome.PASSED, durationMs, false, null, null);
  }

  @Test
  void givenAFullSession_whenEveryUnitReachesOneTerminalState_thenVerdictIsCoverage() {
    String sessionId = UUID.randomUUID().toString();
    RegisterResponse first = client.register(sessionId, registration(0, 1, "rowsWholeVerdict"));
    RegisterResponse second = client.register(sessionId, registration(1, 1, "rowsWholeVerdict"));
    assertThat(first.epoch()).isOne();
    assertThat(second.epoch()).isOne();
    assertThat(second.registeredCount()).isEqualTo(5);

    ClaimResponse alphaByShard0 =
        client.claim(
            sessionId,
            new ClaimRequest(
                0, ALPHA, List.of(Ids.method(ALPHA, "first"), Ids.method(ALPHA, "second"))));
    assertThat(alphaByShard0.granted()).hasSize(2);

    // The whole class is already leased: the other shard gets an empty grant and skips it.
    ClaimResponse alphaByShard1 =
        client.claim(
            sessionId,
            new ClaimRequest(
                1, ALPHA, List.of(Ids.method(ALPHA, "first"), Ids.method(ALPHA, "second"))));
    assertThat(alphaByShard1.granted()).isEmpty();

    for (var grant : alphaByShard0.granted()) {
      client.result(sessionId, passed(0, grant.testId(), grant.fence(), 1_500));
    }

    String templateId = Ids.template(BETA, "rowsWholeVerdict(java.lang.String)");
    Fence templateFence = client.claimOne(sessionId, 1, templateId);
    client.result(
        sessionId,
        new ResultRequest(1,
            templateId,
            templateFence,
            Outcome.PASSED,
            9_000,
            false,
            null,
            List.of(
                new InvocationRecord(Ids.invocation(templateId, 1), Outcome.PASSED, 4_000, null),
                new InvocationRecord(Ids.invocation(templateId, 2), Outcome.PASSED, 5_000, null))));

    String skippedId = Ids.method(GAMMA, "disabledUpstream");
    Fence skippedFence = client.claimOne(sessionId, 1, skippedId);
    client.result(
        sessionId,
        new ResultRequest(1, skippedId, skippedFence, Outcome.SKIPPED, 5, false, "disabled by annotation", null));

    String abortedId = Ids.method(GAMMA, "needsLocalService");
    Fence abortedFence = client.claimOne(sessionId, 1, abortedId);
    client.result(
        sessionId,
        new ResultRequest(1,
            abortedId,
            abortedFence,
            Outcome.ABORTED,
            40,
            false,
            "assumption failed: local service not running",
            null));

    SessionView view = client.view(sessionId);
    assertThat(view.registeredCount()).isEqualTo(5);
    Map<String, TestState> states = new HashMap<>();
    view.tests().forEach(test -> states.put(test.testId(), test.state()));
    assertThat(states)
        .containsEntry(Ids.method(ALPHA, "first"), TestState.PASSED)
        .containsEntry(Ids.method(ALPHA, "second"), TestState.PASSED)
        .containsEntry(templateId, TestState.PASSED)
        .containsEntry(skippedId, TestState.SKIPPED)
        .containsEntry(abortedId, TestState.ABORTED);
    assertThat(
            view.tests().stream()
                .filter(test -> test.testId().equals(abortedId))
                .findFirst()
                .orElseThrow()
                .reason())
        .contains("local service not running");
    assertThat(view.shards()).hasSize(2);
    assertThat(view.shards().stream().mapToInt(SessionView.ShardView::completed).sum()).isEqualTo(5);

    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.PASSED);

    // At-most-once recording: the same lease cannot be written twice.
    CoordinatorClient.RawResponse duplicate =
        client.resultRaw(sessionId, passed(0, Ids.method(ALPHA, "first"), alphaByShard0.granted().get(0).fence(), 1_500));
    assertThat(duplicate.status()).isEqualTo(409);
  }

  @Test
  void givenAFailedUnit_whenClaiming_thenImmediatelyClaimableUntilTheBudgetIsSpent() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));
    String failing = Ids.method(ALPHA, "first");

    // Attempts one and two: each failure puts the unit straight back on the queue, with
    // no pass to wait for. The grant says how much budget is left, which is what lets the
    // engine decide whether to report the failure to the launcher as aborted or as failed.
    for (int attempt = 1; attempt <= 2; attempt++) {
      ClaimResponse claimed =
          client.claim(sessionId, new ClaimRequest(0, ALPHA, List.of(failing)));
      assertThat(claimed.granted()).as("attempt %d must be claimable", attempt).hasSize(1);
      assertThat(claimed.granted().get(0).attemptsRemaining()).isEqualTo(4 - attempt);
      client.result(
          sessionId,
          new ResultRequest(
              0,
              failing,
              claimed.granted().get(0).fence(),
              Outcome.FAILED,
              2_000,
              false,
              null,
              null));
    }

    // The third and final attempt: still claimable, but with no budget behind it.
    ClaimResponse last = client.claim(sessionId, new ClaimRequest(0, ALPHA, List.of(failing)));
    assertThat(last.granted()).hasSize(1);
    assertThat(last.granted().get(0).attemptsRemaining()).isEqualTo(1);
    assertThat(last.granted().get(0).retryable()).isFalse();
    client.result(
        sessionId,
        new ResultRequest(
            0, failing, last.granted().get(0).fence(), Outcome.FAILED, 2_000, false, null, null));

    // Spent: terminally FAILED, and never handed out again however often it is asked for.
    assertThat(client.claim(sessionId, new ClaimRequest(0, ALPHA, List.of(failing))).granted())
        .as("an exhausted unit must never be granted again")
        .isEmpty();
    SessionView view = client.view(sessionId);
    assertThat(CoordinatorClient.stateOf(view, failing)).isEqualTo(TestState.FAILED);
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.FAILED);
  }

  @Test
  void givenAHigherAttempt_whenReRegistering_thenEpochBumpsAndFailuresAreReHanded() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));

    String failing = Ids.method(ALPHA, "first");
    Fence fence = client.claimOne(sessionId, 0, failing);
    client.result(
        sessionId,
        new ResultRequest(0, failing, fence, Outcome.FAILED, 500, false, null, null));
    String leased = Ids.method(ALPHA, "second");
    Fence leasedFence = client.claimOne(sessionId, 0, leased);
    String absorbed = Ids.method(GAMMA, "disabledUpstream");
    Fence absorbedFence = client.claimOne(sessionId, 0, absorbed);
    client.result(
        sessionId,
        new ResultRequest(0, absorbed, absorbedFence, Outcome.SKIPPED, 1, false, "disabled", null));

    RegisterResponse rejoined = client.register(sessionId, registration(3, 2));
    assertThat(rejoined.epoch()).isEqualTo(2);

    // Failures and leases return to PENDING for the new attempt; absorbed states stay.
    assertThat(client.stateOf(sessionId, failing)).isEqualTo(TestState.PENDING);
    assertThat(client.stateOf(sessionId, leased)).isEqualTo(TestState.PENDING);
    assertThat(client.stateOf(sessionId, absorbed)).isEqualTo(TestState.SKIPPED);

    // A write under the previous epoch's fence is a zombie write.
    CoordinatorClient.RawResponse zombie =
        client.resultRaw(sessionId, passed(0, leased, leasedFence, 700));
    assertThat(zombie.status()).isEqualTo(409);
  }

  @Test
  void givenARegisteredSession_whenCensusDiverges_thenConflictNamesTheDifferingIds() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));

    String neverRegistered = Ids.method(ALPHA, "phantom");
    List<String> divergent = List.of(Ids.method(ALPHA, "first"), neverRegistered);
    CoordinatorClient.RawResponse response =
        client.registerRaw(sessionId, new RegisterRequest(1, 1, Map.of(), divergent, null));
    assertThat(response.status()).isEqualTo(409);
    assertThat(response.body())
        .contains(neverRegistered)
        .contains(Ids.method(ALPHA, "second"))
        .contains(Ids.template(BETA, "rows(java.lang.String)"))
        .contains(Ids.method(GAMMA, "disabledUpstream"))
        .contains(Ids.method(GAMMA, "needsLocalService"))
        .doesNotContain(Ids.method(ALPHA, "first"));
  }

  @Test
  void givenMalformedCensusesAndResults_whenPosted_thenRejectedWith400() {
    String sessionId = UUID.randomUUID().toString();
    String invocationId = Ids.invocation(Ids.template(BETA, "rows(java.lang.String)"), 1);
    CoordinatorClient.RawResponse invocationInCensus =
        client.registerRaw(
            sessionId,
            new RegisterRequest(
                0, 1, Map.of(), List.of(invocationId), null));
    assertThat(invocationInCensus.status()).isEqualTo(400);

    CoordinatorClient.RawResponse emptyCensus =
        client.registerRaw(
            sessionId,
            new RegisterRequest(0, 1, Map.of(), List.of(), null));
    assertThat(emptyCensus.status()).isEqualTo(400);

    client.register(sessionId, registration(0, 1));
    String skipped = Ids.method(GAMMA, "disabledUpstream");
    Fence fence = client.claimOne(sessionId, 0, skipped);
    CoordinatorClient.RawResponse reasonless =
        client.resultRaw(
            sessionId,
            new ResultRequest(0, skipped, fence, Outcome.SKIPPED, 1, false, null, null));
    assertThat(reasonless.status()).isEqualTo(400);

    CoordinatorClient.RawResponse inconsistentAggregate =
        client.resultRaw(
            sessionId,
            new ResultRequest(0,
                skipped,
                fence,
                Outcome.PASSED,
                10,
                false,
                null,
                List.of(new InvocationRecord(invocationId, Outcome.FAILED, 5, "boom"))));
    assertThat(inconsistentAggregate.status()).isEqualTo(400);

    // Neither rejection consumed the lease: a well-formed report still lands.
    client.result(
        sessionId,
        new ResultRequest(0, skipped, fence, Outcome.SKIPPED, 1, false, "disabled", null));
    assertThat(client.stateOf(sessionId, skipped)).isEqualTo(TestState.SKIPPED);
  }

  /**
   * The one admissible mixed aggregate: a per-invocation disabling condition skipping a
   * row of an otherwise-passing template still means the unit ran and passed everything
   * it ran. Rejecting it would 400 a healthy shard mid-listener and strand its other
   * leases; FAILED and ABORTED rows under PASSED stay rejected above.
   */
  @Test
  void givenASkippedRowUnderAPassingTemplate_whenReported_thenAcceptedAsPassed() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1, "rowsSkippedRow"));
    String templateId = Ids.template(BETA, "rowsSkippedRow(java.lang.String)");
    Fence fence = client.claimOne(sessionId, 0, templateId);

    client.result(
        sessionId,
        new ResultRequest(0,
            templateId,
            fence,
            Outcome.PASSED,
            7_000,
            false,
            null,
            List.of(
                new InvocationRecord(Ids.invocation(templateId, 1), Outcome.PASSED, 4_000, null),
                new InvocationRecord(
                    Ids.invocation(templateId, 2),
                    Outcome.SKIPPED,
                    0,
                    "row disabled in this environment"),
                new InvocationRecord(Ids.invocation(templateId, 3), Outcome.PASSED, 3_000, null))));

    assertThat(client.stateOf(sessionId, templateId)).isEqualTo(TestState.PASSED);
  }

  @Test
  void givenAnUnregisteredCandidate_whenClaiming_thenConflictNeverAnAutoRegistration() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));
    CoordinatorClient.RawResponse response =
        client.claimRaw(
            sessionId,
            new ClaimRequest(0, ALPHA, List.of(Ids.method(ALPHA, "neverRegistered"))));
    assertThat(response.status()).isEqualTo(409);
  }

  /**
   * The fence already proves who holds the lease, so a result whose shard or pass disagrees
   * with it is a client bug -- and a mislabelled pass would file the failure in the wrong
   * retry pool.
   */
  @Test
  void givenALeasedUnit_whenTheResultNamesAnotherShard_thenRejectedWith400() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));
    String testId = Ids.method(ALPHA, "first");
    Fence fence = client.claimOne(sessionId, 0, testId);

    CoordinatorClient.RawResponse wrongShard =
        client.resultRaw(
            sessionId,
            new ResultRequest(3, testId, fence, Outcome.PASSED, 10, false, null, null));
    assertThat(wrongShard.status()).isEqualTo(400);

    // The rejection did not consume the lease: the honest report still lands.
    client.result(sessionId, passed(0, testId, fence, 10));
    assertThat(client.stateOf(sessionId, testId)).isEqualTo(TestState.PASSED);
  }

  @Test
  void givenStrandedWork_whenEveryShardDeparted_thenVerdictIsIncomplete() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));
    String done = Ids.method(ALPHA, "first");
    Fence fence = client.claimOne(sessionId, 0, done);
    client.result(sessionId, passed(0, done, fence, 800));
    client.depart(sessionId, new DepartRequest(0, 1));

    SessionView view = client.view(sessionId);
    assertThat(view.shards()).allMatch(SessionView.ShardView::departed);
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.INCOMPLETE);
  }

}
