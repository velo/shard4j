package com.marvinformatics.shard4j.coordinator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    properties = {
      "coordinator.secrets=placeholder-not-a-credential",
      "coordinator.tenant-key=example/orders-service",
      "coordinator.tenant-slug=orders-service"
    })
class CoordinatorApplicationTest {

  @Autowired private CoordinatorSettings settings;

  @Test
  void bindsItsConfigurationSurface() {
    assertThat(settings.tenantKey()).isEqualTo("example/orders-service");
    assertThat(settings.tenantSlug()).isEqualTo("orders-service");
    assertThat(settings.secrets()).hasSize(1);
  }
}
