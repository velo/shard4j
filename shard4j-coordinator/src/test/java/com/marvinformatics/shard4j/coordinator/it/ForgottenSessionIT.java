package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
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
 * Sessions exist only because a registration created one. Every other call on a session
 * the coordinator has never heard of is a 404 -- and crucially, it stays a 404: nothing is
 * auto-created as a side effect of being asked.
 */
class ForgottenSessionIT {

  static GenericContainer<?> coordinator;
  static CoordinatorClient client;

  @BeforeAll
  static void start() throws IOException {
    coordinator =
        CoordinatorContainers.coordinator(
            Files.createTempDirectory(Path.of("target"), "forgotten-session-data"), Map.of());
    coordinator.start();
    client = new CoordinatorClient(coordinator);
  }

  @AfterAll
  static void stop() {
    coordinator.stop();
  }

  @Test
  void everyCallOnAnUnknownSessionIs404AndNeverCreatesOne() {
    String sessionId = UUID.randomUUID().toString();
    String testId = Ids.method("com.example.orders.GhostIT", "vanish");
    Fence fence = new Fence(1, 1, 1);

    assertThat(
            client
                .post(
                    "/sessions/" + sessionId + "/claims",
                    new ClaimRequest(0, Pass.MAIN, "com.example.orders.GhostIT", List.of(testId)))
                .statusCode())
        .isEqualTo(404);
    assertThat(
            client
                .resultRaw(
                    sessionId,
                    new ResultRequest(
                        0, Pass.MAIN, testId, fence, Outcome.PASSED, 100, false, null, null))
                .statusCode())
        .isEqualTo(404);
    assertThat(
            client
                .post(
                    "/sessions/" + sessionId + "/nack",
                    new NackRequest(
                        0, List.of(new NackRequest.NackedLease(testId, fence, "shutting down"))))
                .statusCode())
        .isEqualTo(404);
    assertThat(
            client.post("/sessions/" + sessionId + "/depart", new DepartRequest(0)).statusCode())
        .isEqualTo(404);

    assertThat(client.get("/sessions/" + sessionId).statusCode())
        .as("being asked about must not have created the session")
        .isEqualTo(404);
  }
}
