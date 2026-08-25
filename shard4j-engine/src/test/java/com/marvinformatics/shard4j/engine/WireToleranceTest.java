package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.Grant;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * The mixed-version contract: the coordinator deploys first and consumers keep the engine
 * version they pinned, so a field this engine has never heard of must decode as silence,
 * never as an UnrecognizedPropertyException that turns every consumer red the moment the
 * coordinator ships a wire addition. Grant.probe was the first such addition; this pins
 * the tolerance for every one after it, against the gateway's real mapper.
 */
class WireToleranceTest {

  @Test
  void givenAResponseFieldThisEngineDoesNotKnow_whenDecoding_thenItIsIgnored()
      throws Exception {
    String futureGrant =
        """
        {
          "testId": "[engine:junit-jupiter]/[class:com.example.orders.OrderIT]/[method:slow()]",
          "fence": {"epoch": 1, "incarnation": 1, "seq": 7},
          "expiresAt": "2026-08-20T10:00:00Z",
          "probe": false,
          "aFieldFromANewerCoordinator": {"nested": true}
        }
        """;
    Grant grant = CoordinatorGateway.JSON.readValue(futureGrant, Grant.class);
    assertThat(grant.testId()).endsWith("[method:slow()]");
    assertThat(grant.fence().seq()).isEqualTo(7);
    assertThat(grant.expiresAt()).isEqualTo(Instant.parse("2026-08-20T10:00:00Z"));
  }
}
