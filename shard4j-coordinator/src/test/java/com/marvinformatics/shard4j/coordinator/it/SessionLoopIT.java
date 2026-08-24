package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.coordinator.core.CoverageVerdict;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.RegisterResponse;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import com.marvinformatics.shard4j.protocol.SessionVerdict;
import com.marvinformatics.shard4j.protocol.SessionView;
import com.marvinformatics.shard4j.protocol.TestState;
import java.io.IOException;
import java.net.http.HttpResponse;
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
    return List.of(
        Ids.method(ALPHA, "first"),
        Ids.method(ALPHA, "second"),
        Ids.template(BETA, "rows(java.lang.String)"),
        Ids.method(GAMMA, "disabledUpstream"),
        Ids.method(GAMMA, "needsLocalService"));
  }

  private static RegisterRequest registration(int shard, int attempt) {
    return new RegisterRequest(
        shard,
        attempt,
        Map.of("ci", "example-ci", "run", "42"),
        CoordinatorClient.hashOf(census()),
        census());
  }

  private static ResultRequest passed(int shard, String testId, Fence fence, long durationMs) {
    return new ResultRequest(
        shard, Pass.MAIN, testId, fence, Outcome.PASSED, durationMs, false, null, null);
  }

  private Fence claimOne(String sessionId, int shard, String testId) {
    ClaimResponse response =
        client.claim(
            sessionId,
            new ClaimRequest(shard, Pass.MAIN, Ids.classNameOf(testId), List.of(testId)));
    assertThat(response.granted()).hasSize(1);
    return response.granted().get(0).fence();
  }

  @Test
  void everyUnitReachesExactlyOneTerminalStateAndTheVerdictIsCoverage() {
    String sessionId = UUID.randomUUID().toString();
    RegisterResponse first = client.register(sessionId, registration(0, 1));
    RegisterResponse second = client.register(sessionId, registration(1, 1));
    assertThat(first.epoch()).isEqualTo(1);
    assertThat(second.epoch()).isEqualTo(1);
    assertThat(second.registeredCount()).isEqualTo(5);

    ClaimResponse alphaByShard0 =
        client.claim(
            sessionId,
            new ClaimRequest(
                0, Pass.MAIN, ALPHA, List.of(Ids.method(ALPHA, "first"), Ids.method(ALPHA, "second"))));
    assertThat(alphaByShard0.granted()).hasSize(2);

    // The whole class is already leased: the other shard gets an empty grant and skips it.
    ClaimResponse alphaByShard1 =
        client.claim(
            sessionId,
            new ClaimRequest(
                1, Pass.MAIN, ALPHA, List.of(Ids.method(ALPHA, "first"), Ids.method(ALPHA, "second"))));
    assertThat(alphaByShard1.granted()).isEmpty();

    for (var grant : alphaByShard0.granted()) {
      client.result(sessionId, passed(0, grant.testId(), grant.fence(), 1_500));
    }

    String templateId = Ids.template(BETA, "rows(java.lang.String)");
    Fence templateFence = claimOne(sessionId, 1, templateId);
    client.result(
        sessionId,
        new ResultRequest(
            1,
            Pass.MAIN,
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
    Fence skippedFence = claimOne(sessionId, 1, skippedId);
    client.result(
        sessionId,
        new ResultRequest(
            1, Pass.MAIN, skippedId, skippedFence, Outcome.SKIPPED, 5, false, "disabled by annotation", null));

    String abortedId = Ids.method(GAMMA, "needsLocalService");
    Fence abortedFence = claimOne(sessionId, 1, abortedId);
    client.result(
        sessionId,
        new ResultRequest(
            1,
            Pass.MAIN,
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
    HttpResponse<String> duplicate =
        client.resultRaw(sessionId, passed(0, Ids.method(ALPHA, "first"), alphaByShard0.granted().get(0).fence(), 1_500));
    assertThat(duplicate.statusCode()).isEqualTo(409);
  }

  @Test
  void aFailedUnitIsClaimableInTheNextPassAndNeverBefore() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));

    String failing = Ids.method(ALPHA, "first");
    Fence fence = claimOne(sessionId, 0, failing);
    client.result(
        sessionId,
        new ResultRequest(0, Pass.MAIN, failing, fence, Outcome.FAILED, 2_000, false, null, null));

    ClaimResponse mainAgain =
        client.claim(sessionId, new ClaimRequest(0, Pass.MAIN, ALPHA, List.of(failing)));
    assertThat(mainAgain.granted()).isEmpty();

    ClaimResponse retry2 =
        client.claim(sessionId, new ClaimRequest(0, Pass.RETRY2, ALPHA, List.of(failing)));
    assertThat(retry2.granted()).isEmpty();

    ClaimResponse retry1 =
        client.claim(sessionId, new ClaimRequest(0, Pass.RETRY1, ALPHA, List.of(failing)));
    assertThat(retry1.granted()).hasSize(1);
    client.result(
        sessionId,
        new ResultRequest(
            0,
            Pass.RETRY1,
            failing,
            retry1.granted().get(0).fence(),
            Outcome.PASSED,
            2_500,
            false,
            null,
            null));
    assertThat(stateOf(sessionId, failing)).isEqualTo(TestState.PASSED);
  }

  @Test
  void aHigherAttemptBumpsTheEpochAndReHandsFailures() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));

    String failing = Ids.method(ALPHA, "first");
    Fence fence = claimOne(sessionId, 0, failing);
    client.result(
        sessionId,
        new ResultRequest(0, Pass.MAIN, failing, fence, Outcome.FAILED, 500, false, null, null));
    String leased = Ids.method(ALPHA, "second");
    Fence leasedFence = claimOne(sessionId, 0, leased);
    String absorbed = Ids.method(GAMMA, "disabledUpstream");
    Fence absorbedFence = claimOne(sessionId, 0, absorbed);
    client.result(
        sessionId,
        new ResultRequest(
            0, Pass.MAIN, absorbed, absorbedFence, Outcome.SKIPPED, 1, false, "disabled", null));

    RegisterResponse rejoined = client.register(sessionId, registration(3, 2));
    assertThat(rejoined.epoch()).isEqualTo(2);

    // Failures and leases return to PENDING for the new attempt; absorbed states stay.
    assertThat(stateOf(sessionId, failing)).isEqualTo(TestState.PENDING);
    assertThat(stateOf(sessionId, leased)).isEqualTo(TestState.PENDING);
    assertThat(stateOf(sessionId, absorbed)).isEqualTo(TestState.SKIPPED);

    // A write under the previous epoch's fence is a zombie write.
    HttpResponse<String> zombie =
        client.resultRaw(sessionId, passed(0, leased, leasedFence, 700));
    assertThat(zombie.statusCode()).isEqualTo(409);
  }

  @Test
  void aCensusHashMismatchIsAConflictThatScreams() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));

    List<String> divergent = List.of(Ids.method(ALPHA, "first"));
    HttpResponse<String> response =
        client.post(
            "/sessions/" + sessionId + "/register",
            new RegisterRequest(1, 1, Map.of(), CoordinatorClient.hashOf(divergent), divergent));
    assertThat(response.statusCode()).isEqualTo(409);
    assertThat(response.body()).contains("hash");
  }

  @Test
  void malformedCensusesAndResultsAreRejectedWith400() {
    String sessionId = UUID.randomUUID().toString();
    String invocationId = Ids.invocation(Ids.template(BETA, "rows(java.lang.String)"), 1);
    HttpResponse<String> invocationInCensus =
        client.post(
            "/sessions/" + sessionId + "/register",
            new RegisterRequest(
                0, 1, Map.of(), CoordinatorClient.hashOf(List.of(invocationId)), List.of(invocationId)));
    assertThat(invocationInCensus.statusCode()).isEqualTo(400);

    HttpResponse<String> emptyCensus =
        client.post(
            "/sessions/" + sessionId + "/register",
            new RegisterRequest(0, 1, Map.of(), CoordinatorClient.hashOf(List.of()), List.of()));
    assertThat(emptyCensus.statusCode()).isEqualTo(400);

    client.register(sessionId, registration(0, 1));
    String skipped = Ids.method(GAMMA, "disabledUpstream");
    Fence fence = claimOne(sessionId, 0, skipped);
    HttpResponse<String> reasonless =
        client.resultRaw(
            sessionId,
            new ResultRequest(0, Pass.MAIN, skipped, fence, Outcome.SKIPPED, 1, false, null, null));
    assertThat(reasonless.statusCode()).isEqualTo(400);

    HttpResponse<String> inconsistentAggregate =
        client.resultRaw(
            sessionId,
            new ResultRequest(
                0,
                Pass.MAIN,
                skipped,
                fence,
                Outcome.PASSED,
                10,
                false,
                null,
                List.of(new InvocationRecord(invocationId, Outcome.FAILED, 5, "boom"))));
    assertThat(inconsistentAggregate.statusCode()).isEqualTo(400);

    // Neither rejection consumed the lease: a well-formed report still lands.
    client.result(
        sessionId,
        new ResultRequest(0, Pass.MAIN, skipped, fence, Outcome.SKIPPED, 1, false, "disabled", null));
    assertThat(stateOf(sessionId, skipped)).isEqualTo(TestState.SKIPPED);
  }

  @Test
  void claimingAnUnregisteredCandidateIsAConflictNeverAnAutoRegistration() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));
    HttpResponse<String> response =
        client.post(
            "/sessions/" + sessionId + "/claims",
            new ClaimRequest(0, Pass.MAIN, ALPHA, List.of(Ids.method(ALPHA, "neverRegistered"))));
    assertThat(response.statusCode()).isEqualTo(409);
  }

  @Test
  void everyShardDepartedWithStrandedWorkReadsAsIncomplete() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, registration(0, 1));
    String done = Ids.method(ALPHA, "first");
    Fence fence = claimOne(sessionId, 0, done);
    client.result(sessionId, passed(0, done, fence, 800));
    client.depart(sessionId, new DepartRequest(0));

    SessionView view = client.view(sessionId);
    assertThat(view.shards()).allMatch(SessionView.ShardView::departed);
    assertThat(CoverageVerdict.of(view)).isEqualTo(SessionVerdict.INCOMPLETE);
  }

  private static TestState stateOf(String sessionId, String testId) {
    return client.view(sessionId).tests().stream()
        .filter(test -> test.testId().equals(testId))
        .findFirst()
        .orElseThrow()
        .state();
  }
}
