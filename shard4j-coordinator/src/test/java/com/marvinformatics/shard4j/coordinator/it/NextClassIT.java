package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.coordinator.core.HistoryKeys;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.HistoryKey;
import com.marvinformatics.shard4j.protocol.NextClassRequest;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * The open ask, end to end: the coordinator -- not the shard -- decides which class runs
 * next, by the same unit rules that already govern order inside a class. Known classes
 * are handed out by their slowest remaining unit; a class holding a no-history unit
 * outranks every fully-measured class, in the pinned hash order of its unknowns; and the
 * named class arrives with its first batch of leases so it is never an empty promise.
 */
class NextClassIT {

  private static final String BIG = "com.example.orders.BigHistoryIT";
  private static final String MID = "com.example.orders.MidHistoryIT";
  private static final String MIXED = "com.example.orders.MixedFreshIT";
  private static final String BIG_CASE = Ids.method(BIG, "bigCase");
  private static final String MID_CASE = Ids.method(MID, "midCase");
  private static final String MIXED_NEW_A = Ids.method(MIXED, "brandNewAlpha");
  private static final String MIXED_NEW_B = Ids.method(MIXED, "brandNewBeta");
  private static final String MIXED_SMALL = Ids.method(MIXED, "smallCase");

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void seedThenStart() throws IOException {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "next-class-data");
    History.seed(dataDir, Map.of(BIG_CASE, 500_000L, MID_CASE, 200_000L, MIXED_SMALL, 10_000L));

    coordinator = CoordinatorContainers.coordinator(dataDir, Map.of());
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  private static List<String> census() {
    return List.of(BIG_CASE, MID_CASE, MIXED_NEW_A, MIXED_NEW_B, MIXED_SMALL);
  }

  @Test
  void givenPartialHistory_whenAskingWhatNext_thenClassesArriveUnknownsFirstThenSlowestFirstUntilNothingRemains() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, new RegisterRequest(0, 1, Map.of(), census()));

    // The class holding no-history units beats the class with the biggest measured
    // duration, and inside it the unknowns lead in pinned hash order before its known unit.
    NextClassResponse first = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    assertThat(first.className()).isEqualTo(MIXED);
    List<String> expectedUnknowns = new ArrayList<>(List.of(MIXED_NEW_A, MIXED_NEW_B));
    expectedUnknowns.sort(
        Comparator.comparing((String id) -> HistoryKeys.of(id), HistoryKey.NO_HISTORY_ORDER));
    List<String> expectedMixed = new ArrayList<>(expectedUnknowns);
    expectedMixed.add(MIXED_SMALL);
    assertThat(first.granted()).extracting(Grant::testId).isEqualTo(expectedMixed);
    reportAllPassed(sessionId, first.granted());

    NextClassResponse second = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    assertThat(second.className()).isEqualTo(BIG);
    assertThat(second.granted()).extracting(Grant::testId).containsExactly(BIG_CASE);
    reportAllPassed(sessionId, second.granted());

    NextClassResponse third = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    assertThat(third.className()).isEqualTo(MID);
    assertThat(third.granted()).extracting(Grant::testId).containsExactly(MID_CASE);
    reportAllPassed(sessionId, third.granted());

    // Nothing claimable names no class and grants nothing.
    NextClassResponse done = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    assertThat(done.className()).isNull();
    assertThat(done.granted()).isEmpty();
  }

  @Test
  void givenEverythingLeasedElsewhere_whenAskingWhatNext_thenTheAnswerIsEmptyNotAPromise() {
    String sessionId = UUID.randomUUID().toString();
    client.register(sessionId, new RegisterRequest(0, 1, Map.of(), census()));
    client.register(sessionId, new RegisterRequest(1, 1, Map.of(), census()));

    NextClassResponse everythingMixed = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    NextClassResponse everythingBig = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    NextClassResponse everythingMid = client.next(sessionId, new NextClassRequest(0, Pass.MAIN));
    assertThat(everythingMixed.granted()).hasSize(3);
    assertThat(everythingBig.granted()).hasSize(1);
    assertThat(everythingMid.granted()).hasSize(1);

    NextClassResponse starved = client.next(sessionId, new NextClassRequest(1, Pass.MAIN));
    assertThat(starved.className()).isNull();
    assertThat(starved.granted()).isEmpty();
  }

  private static void reportAllPassed(String sessionId, List<Grant> grants) {
    for (Grant grant : grants) {
      client.result(
          sessionId,
          new ResultRequest(
              0, Pass.MAIN, grant.testId(), grant.fence(), Outcome.PASSED, 1_000, false, null, null));
    }
  }
}
