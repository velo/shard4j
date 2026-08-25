package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.coordinator.core.HistoryKeys;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.ClaimResponse;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.HistoryKey;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
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
 * Ordering, proven end to end: the data volume is seeded before first start with plain
 * history records (no import endpoint, no special record type -- the seed IS the format),
 * and a claim over a mixed candidate set must come back no-history-first in pinned hash
 * order, then slowest-first by the measured aggregate.
 */
class SlowestFirstOrderingIT {

  private static final String CLASS_NAME = "com.example.orders.MixedHistoryIT";
  private static final String SLOW = Ids.method(CLASS_NAME, "slowCase");
  private static final String MID = Ids.method(CLASS_NAME, "midCase");
  private static final String FAST = Ids.method(CLASS_NAME, "fastCase");
  private static final String NEW_A = Ids.method(CLASS_NAME, "brandNewAlpha");
  private static final String NEW_B = Ids.method(CLASS_NAME, "brandNewBeta");

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void seedThenStart() throws IOException {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "ordering-data");
    Path historyDir =
        dataDir.resolve(CoordinatorContainers.TENANT_SLUG).resolve("history");
    Files.createDirectories(historyDir);
    String today = LocalDate.now(ZoneOffset.UTC).toString();
    Files.writeString(
        historyDir.resolve(today + ".jsonl"),
        seedLine(SLOW, 300_000) + seedLine(MID, 120_000) + seedLine(FAST, 2_000));

    coordinator = CoordinatorContainers.coordinator(dataDir, Map.of());
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  private static String seedLine(String testId, long durationMs) {
    return "{\"type\":\"COMPLETION\",\"project\":\""
        + CoordinatorContainers.TENANT_KEY
        + "\",\"session\":\"seeded-elsewhere\",\"epoch\":1,\"testId\":\""
        + testId.replace("\\", "\\\\")
        + "\",\"unit\":true,\"shard\":0,\"pass\":\"MAIN\",\"outcome\":\"PASSED\",\"durationMs\":"
        + durationMs
        + ",\"firstOnShard\":false,\"ts\":\"2026-08-20T10:00:00Z\"}\n";
  }

  @Test
  void unknownsLeadInPinnedHashOrderThenKnownsSlowestFirst() {
    List<String> census = List.of(FAST, MID, NEW_A, SLOW, NEW_B);
    String sessionId = UUID.randomUUID().toString();
    client.register(
        sessionId, new RegisterRequest(0, 1, Map.of(), census));

    ClaimResponse response =
        client.claim(sessionId, new ClaimRequest(0, Pass.MAIN, CLASS_NAME, census));
    List<String> grantedOrder = response.granted().stream().map(Grant::testId).toList();

    List<String> expectedUnknowns = new ArrayList<>(List.of(NEW_A, NEW_B));
    expectedUnknowns.sort(
        Comparator.comparing(
            (String id) -> HistoryKeys.of(id), HistoryKey.NO_HISTORY_ORDER));

    List<String> expected = new ArrayList<>(expectedUnknowns);
    expected.add(SLOW);
    expected.add(MID);
    expected.add(FAST);
    assertThat(grantedOrder).isEqualTo(expected);
  }
}
