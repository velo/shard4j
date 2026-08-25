package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NackResponse;
import com.marvinformatics.shard4j.protocol.NextClassRequest;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Invocation distribution over the real wire: a template method whose duration history
 * carries a complete per-invocation breakdown is handed out position by position, so two
 * shards split what used to be one indivisible block -- while a template the coordinator
 * has never seen still leases whole. The probe past the recorded plan, the fair-share
 * hold-back, retry of a single failed position and the vanished-position heal are all
 * pinned here against the shipped container.
 */
class InvocationDistributionIT {

  private static final String CLASS_NAME = "com.example.orders.ParameterizedSuiteIT";
  private static final String TEMPLATE = Ids.template(CLASS_NAME, "rows(java.lang.String)");
  private static final String FRESH = Ids.template(CLASS_NAME, "freshRows(java.lang.String)");
  private static final String DRIFTING = Ids.template(CLASS_NAME, "driftingRows(java.lang.String)");

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void seedThenStart() throws IOException {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "distribution-data");
    History.seedTemplate(
        dataDir, TEMPLATE, Map.of(1, 40_000L, 2, 50_000L, 3, 60_000L, 4, 70_000L));
    History.seedTemplate(dataDir, DRIFTING, Map.of(1, 30_000L, 2, 35_000L, 3, 45_000L));
    coordinator = CoordinatorContainers.coordinator(dataDir, Map.of());
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void givenACompleteBreakdown_whenTwoShardsAsk_thenInvocationsSpreadAndTheProbeVanishesQuietly() {
    String sessionId = UUID.randomUUID().toString();
    registerBoth(sessionId, List.of(TEMPLATE));

    NextClassResponse first = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    assertThat(first.className()).isEqualTo(CLASS_NAME);
    // Fair share of five claimable units (four measured plus the probe) over a fleet of
    // two is three, and within the class the measured positions come slowest first.
    assertThat(first.granted().stream().map(Grant::testId))
        .containsExactly(invocation(4), invocation(3), invocation(2));

    NextClassResponse second = client.next(sessionId, new NextClassRequest(1, Pass.MAIN));
    assertThat(second.className()).isEqualTo(CLASS_NAME);
    assertThat(second.granted().stream().map(Grant::testId))
        .containsExactly(invocation(1), invocation(5));
    assertThat(second.granted().stream().map(Grant::probe)).containsExactly(false, true);

    // The method is fully leased across the two shards; a third ask finds nothing.
    assertThat(client.next(sessionId, new NextClassRequest(0, Pass.MAIN)).className()).isNull();

    reportPassed(sessionId, 0, first.granted());
    reportPassed(sessionId, 1, List.of(second.granted().get(0)));
    NackResponse nack =
        client.nack(
            sessionId,
            new NackRequest(
                1,
                List.of(
                    new NackRequest.NackedLease(
                        invocation(5),
                        second.granted().get(1).fence(),
                        "Cardinality probe past recorded history: the invocation does not"
                            + " exist; the recorded parameter count stands",
                        true))));
    assertThat(nack.released()).containsExactly(invocation(5));

    // The vanished probe leaves the census entirely: four positions, four passes, and a
    // verdict that would read PASSED -- a nonexistent row must not cost an INCOMPLETE.
    SessionView view = client.view(sessionId);
    assertThat(view.registeredCount()).isEqualTo(4);
    assertThat(view.tests())
        .allSatisfy(test -> assertThat(test.state()).isEqualTo(TestState.PASSED));
    assertThat(shardsThatRan(view, TEMPLATE)).containsExactlyInAnyOrder(0, 1);
  }

  @Test
  void givenNoHistory_whenAShardAsks_thenTheTemplateStillLeasesWhole() {
    String sessionId = UUID.randomUUID().toString();
    registerBoth(sessionId, List.of(FRESH));

    NextClassResponse next = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    assertThat(next.className()).isEqualTo(CLASS_NAME);
    assertThat(next.granted().stream().map(Grant::testId)).containsExactly(FRESH);
  }

  @Test
  void givenOneFailedInvocation_whenTheRetryPassRuns_thenOnlyThatPositionRetriesOnAnyShard() {
    String sessionId = UUID.randomUUID().toString();
    registerBoth(sessionId, List.of(TEMPLATE));

    NextClassResponse first = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    NextClassResponse second = client.next(sessionId, new NextClassRequest(1, Pass.MAIN));

    // Shard 0 fails the slowest position and passes the rest of its share.
    report(sessionId, 0, first.granted().get(0), Outcome.FAILED, "row rejected");
    reportPassed(sessionId, 0, first.granted().subList(1, first.granted().size()));
    reportPassed(sessionId, 1, List.of(second.granted().get(0)));
    client.nack(
        sessionId,
        new NackRequest(
            1,
            List.of(
                new NackRequest.NackedLease(
                    invocation(5), second.granted().get(1).fence(), "probe vanished", true))));

    // The retry pool holds exactly the failed position, and the other shard may take it --
    // paying its own class setup, which is the agreed cost of spreading.
    NextClassResponse retry = client.next(sessionId, new NextClassRequest(1, Pass.RETRY1));
    assertThat(retry.className()).isEqualTo(CLASS_NAME);
    assertThat(retry.granted().stream().map(Grant::testId)).containsExactly(invocation(4));
    reportPassed(sessionId, 1, retry.granted(), Pass.RETRY1);

    SessionView view = client.view(sessionId);
    long terminal = view.tests().stream().filter(test -> test.state().isAbsorbing()).count();
    assertThat(terminal).isEqualTo(view.registeredCount());
  }

  @Test
  void givenAVanishedMeasuredPosition_whenTheNextSessionRegisters_thenThePlanHasHealed() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, new RegisterRequest(0, 1, Map.of(), List.of(DRIFTING), null));

    // A lone shard is never capped: it takes all three measured positions plus the probe.
    NextClassResponse next = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    assertThat(next.granted()).hasSize(4);
    Grant stale = next.granted().get(0);
    assertThat(stale.testId()).isEqualTo(DRIFTING + "/[test-template-invocation:#3]");

    // The shard proves #3 no longer exists -- the parameter set shrank since it was
    // measured -- and returns the probe the same way.
    client.nack(
        sessionId,
        new NackRequest(
            0,
            List.of(
                new NackRequest.NackedLease(
                    stale.testId(),
                    stale.fence(),
                    "the parameter set changed since it was last measured",
                    true),
                new NackRequest.NackedLease(
                    next.granted().get(3).testId(),
                    next.granted().get(3).fence(),
                    "probe vanished",
                    true))));

    // The stale position stays loud in this session: still registered, still PENDING.
    assertThat(client.stateOf(sessionId, stale.testId())).isEqualTo(TestState.PENDING);

    // But the next session expands from the corrected plan: two positions and a probe at
    // the position that just vanished.
    String healedSession = UUID.randomUUID().toString();
    client.register(healedSession, new RegisterRequest(0, 1, Map.of(), List.of(DRIFTING), null));
    assertThat(client.view(healedSession).tests().stream().map(SessionView.TestView::testId))
        .containsExactlyInAnyOrder(
            DRIFTING + "/[test-template-invocation:#1]",
            DRIFTING + "/[test-template-invocation:#2]",
            DRIFTING + "/[test-template-invocation:#3]");
  }

  private static void registerBoth(String sessionId, List<String> census) {
    client.register(sessionId, new RegisterRequest(0, 1, Map.of(), census, 2));
    client.register(sessionId, new RegisterRequest(1, 1, Map.of(), census, 2));
  }

  private static String invocation(int position) {
    return Ids.invocation(TEMPLATE, position);
  }

  private static void reportPassed(String sessionId, int shard, List<Grant> grants) {
    reportPassed(sessionId, shard, grants, Pass.MAIN);
  }

  private static void reportPassed(String sessionId, int shard, List<Grant> grants, Pass pass) {
    for (Grant grant : grants) {
      report(sessionId, shard, grant, pass, Outcome.PASSED, null);
    }
  }

  private static void report(
      String sessionId, int shard, Grant grant, Outcome outcome, String reason) {
    report(sessionId, shard, grant, Pass.MAIN, outcome, reason);
  }

  private static void report(
      String sessionId, int shard, Grant grant, Pass pass, Outcome outcome, String reason) {
    client.result(
        sessionId,
        new ResultRequest(
            shard, pass, grant.testId(), grant.fence(), outcome, 45_000, false, reason, null));
  }

  private static List<Integer> shardsThatRan(SessionView view, String templateId) {
    return view.tests().stream()
        .filter(test -> test.testId().startsWith(templateId + "/"))
        .flatMap(test -> test.records().stream())
        .map(SessionView.RecordView::shard)
        .distinct()
        .sorted()
        .toList();
  }
}
