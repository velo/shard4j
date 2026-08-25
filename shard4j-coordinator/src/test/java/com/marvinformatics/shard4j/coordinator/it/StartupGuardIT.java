package com.marvinformatics.shard4j.coordinator.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.ToStringConsumer;

/**
 * The service never runs open: with no accepted secret it must refuse to start naming the
 * variable, and with one every mutating or reading call without the right bearer is a 401.
 */
class StartupGuardIT {

  @Test
  void givenNoAcceptedSecret_whenStarting_thenRefusesToStartNamingTheVariable() throws Exception {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "no-secret-data");
    ToStringConsumer logs = new ToStringConsumer();
    GenericContainer<?> unsecured =
        CoordinatorContainers.bareCoordinator(
                dataDir,
                Map.of(
                    "COORDINATOR_TENANT_KEY", CoordinatorContainers.TENANT_KEY,
                    "COORDINATOR_TENANT_SLUG", CoordinatorContainers.TENANT_SLUG,
                    "COORDINATOR_DATA_DIR", "/data"))
            .withLogConsumer(logs);
    try {
      assertThatThrownBy(unsecured::start).isInstanceOf(RuntimeException.class);
    } finally {
      unsecured.stop();
    }
    assertThat(logs.toUtf8String()).contains("COORDINATOR_SECRETS");
  }

  @Test
  void givenASecuredCoordinator_whenCalledWithoutTheSecret_then401ExceptTheHealthProbes() throws Exception {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "auth-data");
    GenericContainer<?> secured = CoordinatorContainers.coordinator(dataDir, Map.of());
    try {
      secured.start();
      CoordinatorClient anonymous = new CoordinatorClient(secured, null);
      CoordinatorClient wrongSecret = new CoordinatorClient(secured, "not-the-value");
      CoordinatorClient authorised = new CoordinatorClient(secured);
      String sessionId = UUID.randomUUID().toString();

      assertThat(anonymous.viewRaw(sessionId).status()).isEqualTo(401);
      assertThat(wrongSecret.viewRaw(sessionId).status()).isEqualTo(401);
      assertThat(wrongSecret.departRaw(sessionId, Map.of("shard", 0)).status())
          .isEqualTo(401);
      assertThat(anonymous.probe("healthz").status()).isEqualTo(200);
      assertThat(anonymous.probe("readyz").status()).isEqualTo(200);
      // The right secret gets through the filter and reaches the real 404.
      assertThat(authorised.viewRaw(sessionId).status()).isEqualTo(404);
    } finally {
      secured.stop();
    }
  }

  @Test
  void givenPublicReadOptIn_whenAnonymousCallsArrive_thenOnlyTheReadSurfaceIsOpen() throws Exception {
    Path dataDir = Files.createTempDirectory(Path.of("target"), "public-read-data");
    GenericContainer<?> publicRead =
        CoordinatorContainers.coordinator(dataDir, Map.of("COORDINATOR_PUBLIC_READ", "true"));
    try {
      publicRead.start();
      CoordinatorClient anonymous = new CoordinatorClient(publicRead, null);
      String sessionId = UUID.randomUUID().toString();

      assertThat(anonymous.viewRaw(sessionId).status())
          .as("read is open, so the answer is the session-level 404, not a 401")
          .isEqualTo(404);
      assertThat(anonymous.departRaw(sessionId, Map.of("shard", 0)).status())
          .as("mutating calls stay authenticated regardless")
          .isEqualTo(401);
    } finally {
      publicRead.stop();
    }
  }
}
