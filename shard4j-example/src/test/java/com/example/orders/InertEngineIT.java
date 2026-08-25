package com.example.orders;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.discovery.DiscoverySelectors;

/**
 * The inertness guarantee: with {@code shard.enabled} absent, both engines are registered
 * -- exactly a consumer's test classpath outside the CI profile -- and the suite must run
 * exactly once, under Jupiter, with the shard4j engine contributing nothing. A delegating
 * "inert" engine would run everything twice; a claiming one would need a coordinator.
 */
@Tag("shard4j-harness")
class InertEngineIT {

  @Test
  void givenBothEnginesAndNoShardConfiguration_whenTheSuiteRuns_thenEveryTestRunsExactlyOnce() {
    LauncherDiscoveryRequest request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(
                DiscoverySelectors.selectClass(PingResourceIT.class),
                DiscoverySelectors.selectClass(InventoryAuditIT.class),
                DiscoverySelectors.selectClass(CatalogSearchIT.class))
            .build();

    List<String> started = new ArrayList<>();
    TestExecutionListener listener =
        new TestExecutionListener() {
          @Override
          public void executionStarted(TestIdentifier identifier) {
            if (identifier.isTest()) {
              started.add(identifier.getUniqueId());
            }
          }
        };
    Launcher launcher = LauncherFactory.create();
    launcher.execute(request, listener);

    // hello + 3 rows of each() + countsStock + needsLocalWarehouse + 3 rows of
    // findsProducts: 9 test starts, every one under Jupiter, no duplicates.
    assertThat(started).hasSize(9);
    assertThat(started).doesNotHaveDuplicates();
    assertThat(started).allMatch(id -> id.startsWith("[engine:junit-jupiter]"));
  }
}
