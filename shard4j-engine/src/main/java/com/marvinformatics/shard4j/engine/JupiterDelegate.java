package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.DiscoveryFilter;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.reporting.OutputDirectoryProvider;
import org.junit.platform.engine.support.store.Namespace;
import org.junit.platform.engine.support.store.NamespacedHierarchicalStore;

/**
 * The whole delegation surface, in one place: Jupiter is resolved through
 * {@code ServiceLoader} (no compile dependency on its engine class), discovery goes
 * through {@code TestEngine.discover} -- the stable interface method -- and execution
 * through the 5-arg {@code ExecutionRequest.create}, the design's one INTERNAL symbol.
 * Nothing from {@code org.junit.platform.launcher.core} may ever appear here; a
 * forbiddenapis rule fails the build if it does.
 */
final class JupiterDelegate {

  static final String JUPITER_ENGINE_ID = "junit-jupiter";

  private final TestEngine jupiter;
  private final UniqueId nestedRootId;

  JupiterDelegate(UniqueId engineRootId) {
    this.jupiter =
        StreamSupport.stream(ServiceLoader.load(TestEngine.class).spliterator(), false)
            .filter(engine -> JUPITER_ENGINE_ID.equals(engine.getId()))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "junit-jupiter is not on the classpath; the shard4j engine has nothing"
                            + " to delegate to"));
    this.nestedRootId = engineRootId.append("engine", JUPITER_ENGINE_ID);
  }

  UniqueId nestedRootId() {
    return nestedRootId;
  }

  /**
   * Delegates the consumer's own discovery request. The request is re-wrapped in a plain
   * {@code EngineDiscoveryRequest} facade first, which is what strips the build tool's
   * {@code EngineFilter}: the filter excluding junit-jupiter is exactly how this engine got
   * selected, and handing it onward re-applies it to the nested discovery -- which then
   * resolves nothing, and the observed result is a green build that ran the whole suite
   * nowhere. The facade cannot carry engine filters by construction.
   */
  TestDescriptor discover(EngineDiscoveryRequest request) {
    return jupiter.discover(new EngineFacingRequest(request), nestedRootId);
  }

  /** Discovers exactly the given ids, re-rooted under this engine. */
  TestDescriptor discoverIds(
      List<ExecutionId> ids,
      ConfigurationParameters parameters,
      OutputDirectoryProvider outputDirectoryProvider) {
    UniqueId engineRootId = nestedRootId.removeLastSegment();
    List<DiscoverySelector> selectors =
        ids.stream()
            .map(id -> ExecutionIdentity.underEngineRoot(engineRootId, id))
            .<DiscoverySelector>map(DiscoverySelectors::selectUniqueId)
            .toList();
    return jupiter.discover(
        new SelectorsRequest(selectors, parameters, outputDirectoryProvider), nestedRootId);
  }

  /**
   * One nested execution. The store is a child of the outer request's, closed when the
   * batch ends, so batch-scoped resources are released without touching the launcher's own
   * store lifecycle.
   */
  void execute(TestDescriptor descriptor, ExecutionRequest outer, EngineExecutionListener listener) {
    try (NamespacedHierarchicalStore<Namespace> store =
        new NamespacedHierarchicalStore<>(outer.getStore())) {
      jupiter.execute(
          ExecutionRequest.create(
              descriptor,
              listener,
              outer.getConfigurationParameters(),
              outer.getOutputDirectoryProvider(),
              store));
    }
  }

  /**
   * A plain {@code EngineDiscoveryRequest} view of the consumer's request: selectors,
   * discovery filters and configuration parameters pass through untouched; anything a
   * richer launcher-side request type carries -- engine filters above all -- structurally
   * does not exist here.
   */
  private record EngineFacingRequest(EngineDiscoveryRequest delegate)
      implements EngineDiscoveryRequest {

    @Override
    public <T extends DiscoverySelector> List<T> getSelectorsByType(Class<T> selectorType) {
      return delegate.getSelectorsByType(selectorType);
    }

    @Override
    public <T extends DiscoveryFilter<?>> List<T> getFiltersByType(Class<T> filterType) {
      return delegate.getFiltersByType(filterType);
    }

    @Override
    public ConfigurationParameters getConfigurationParameters() {
      return delegate.getConfigurationParameters();
    }

    @Override
    public OutputDirectoryProvider getOutputDirectoryProvider() {
      return delegate.getOutputDirectoryProvider();
    }
  }

  private record SelectorsRequest(
      List<DiscoverySelector> selectors,
      ConfigurationParameters parameters,
      OutputDirectoryProvider outputDirectoryProvider)
      implements EngineDiscoveryRequest {

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DiscoverySelector> List<T> getSelectorsByType(Class<T> selectorType) {
      return selectors.stream()
          .filter(selectorType::isInstance)
          .map(selector -> (T) selector)
          .toList();
    }

    @Override
    public <T extends DiscoveryFilter<?>> List<T> getFiltersByType(Class<T> filterType) {
      return List.of();
    }

    @Override
    public ConfigurationParameters getConfigurationParameters() {
      return parameters;
    }

    @Override
    public OutputDirectoryProvider getOutputDirectoryProvider() {
      return outputDirectoryProvider;
    }
  }
}
