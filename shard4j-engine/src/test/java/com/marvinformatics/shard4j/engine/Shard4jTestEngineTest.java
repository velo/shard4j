package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.EngineFilter;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;

class Shard4jTestEngineTest {

  private static final UniqueId ENGINE_ROOT = UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID);

  @Test
  void givenTheServiceLoader_whenLoadingEngines_thenShard4jIsRegistered() {
    boolean registered =
        StreamSupport.stream(ServiceLoader.load(TestEngine.class).spliterator(), false)
            .anyMatch(engine -> Shard4jTestEngine.ENGINE_ID.equals(engine.getId()));

    assertThat(registered).as("shard4j must be registered as a TestEngine service").isTrue();
  }

  @Test
  void givenShardingDisabled_whenDiscovering_thenTheDescriptorIsEmptyNotADelegation() {
    // Outside the coordinated profile junit-jupiter is still registered: delegating here
    // would run the whole suite twice. Empty is what "inert" means.
    LauncherDiscoveryRequest request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(DiscoverySelectors.selectClass(PlainShapesFixture.class))
            .build();

    TestDescriptor descriptor = new Shard4jTestEngine().discover(request, ENGINE_ROOT);

    assertThat(descriptor.getChildren()).isEmpty();
  }

  @Test
  void givenTheBuildToolsOwnEngineExcludeFilter_whenDiscovering_thenJupiterStillResolves() {
    // The filter excluding junit-jupiter is exactly how this engine got selected; handing
    // it onward would make the nested discovery resolve nothing and the suite would run
    // nowhere behind a green build. The stripped delegation must still find the leaves.
    LauncherDiscoveryRequest request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(DiscoverySelectors.selectClass(PlainShapesFixture.class))
            .filters(EngineFilter.excludeEngines(JupiterDelegate.JUPITER_ENGINE_ID))
            .configurationParameter(ShardConfiguration.ENABLED, "true")
            .configurationParameter(ShardConfiguration.COORDINATOR_URL, "http://localhost:1")
            .configurationParameter(ShardConfiguration.SESSION_ID, "7f3a")
            .configurationParameter(ShardConfiguration.SHARD_INDEX, "0")
            .configurationParameter(ShardConfiguration.PASS, "main")
            .build();

    TestDescriptor descriptor = new Shard4jTestEngine().discover(request, ENGINE_ROOT);

    assertThat(descriptor.getDescendants())
        .anyMatch(child -> child.getUniqueId().toString().endsWith("[method:passes()]"));
  }

  @Test
  void givenAUniqueIdOnlyRequest_whenExecuting_thenExactlyThoseIdsRunAndNoCoordinatorIsCalled() {
    // The build tool's rerun path. The coordinator URL points at a closed port, so any
    // network call would fail this test loudly.
    String target =
        ENGINE_ROOT.toString()
            + "/[engine:junit-jupiter]/[class:"
            + PlainShapesFixture.class.getName()
            + "]/[method:passes()]";
    LauncherDiscoveryRequest request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(DiscoverySelectors.selectUniqueId(target))
            .configurationParameter(ShardConfiguration.ENABLED, "true")
            .configurationParameter(ShardConfiguration.COORDINATOR_URL, "http://localhost:1")
            .configurationParameter(ShardConfiguration.SESSION_ID, "7f3a")
            .configurationParameter(ShardConfiguration.SHARD_INDEX, "0")
            .configurationParameter(ShardConfiguration.PASS, "main")
            .build();
    Shard4jTestEngine engine = new Shard4jTestEngine();
    TestDescriptor root = engine.discover(request, ENGINE_ROOT);

    List<String> finished = new ArrayList<>();
    EngineExecutionListener listener =
        new EngineExecutionListener() {
          @Override
          public void executionFinished(TestDescriptor descriptor, TestExecutionResult result) {
            if (descriptor.getUniqueId().toString().contains("[method:")) {
              assertThat(result.getStatus()).isEqualTo(TestExecutionResult.Status.SUCCESSFUL);
              finished.add(descriptor.getUniqueId().toString());
            }
          }
        };
    engine.execute(
        ExecutionRequest.create(
            root,
            listener,
            request.getConfigurationParameters(),
            EngineTestHarness.outerRequest(listener).getOutputDirectoryProvider(),
            EngineTestHarness.outerRequest(listener).getStore()));

    assertThat(finished).containsExactly(target);
  }
}
