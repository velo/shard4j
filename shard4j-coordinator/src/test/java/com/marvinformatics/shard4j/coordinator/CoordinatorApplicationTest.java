package com.marvinformatics.shard4j.coordinator;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(
    properties = {
      "coordinator.secrets=placeholder-not-a-credential",
      "coordinator.tenant-key=example/orders-service",
      "coordinator.tenant-slug=orders-service"
    })
class CoordinatorApplicationTest {

  @TempDir static Path dataDir;

  @DynamicPropertySource
  static void dataDirectory(DynamicPropertyRegistry registry) {
    registry.add("coordinator.data-dir", () -> dataDir.toString());
  }

  @Autowired private CoordinatorSettings settings;

  @Test
  void bindsItsConfigurationSurface() {
    assertThat(settings.tenantKey()).isEqualTo("example/orders-service");
    assertThat(settings.tenantSlug()).isEqualTo("orders-service");
    assertThat(settings.secrets()).hasSize(1);
    assertThat(settings.leaseTtl().toMinutes()).isEqualTo(20);
    assertThat(settings.maxClaimBatch()).isEqualTo(8);
  }
}
