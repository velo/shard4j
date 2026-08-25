package com.marvinformatics.shard4j.engine;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import java.util.List;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.discovery.UniqueIdSelector;
import org.junit.platform.engine.support.descriptor.EngineDescriptor;

/**
 * Delegating engine, selected by a CI-only profile through failsafe's
 * {@code <excludeJUnit5Engines>junit-jupiter</excludeJUnit5Engines>}.
 *
 * <p>It delegates through exactly two symbols: {@code TestEngine.discover(request,
 * uniqueId)}, which is {@code @API(STABLE)}, and the 5-arg {@code ExecutionRequest.create},
 * which is the one {@code @API(INTERNAL)} symbol in the design. Nothing in
 * {@code org.junit.platform.launcher.core} may ever appear here; a forbiddenapis rule
 * fails the build if it does.
 *
 * <p>With {@code shard.enabled} absent or false, {@code discover()} returns an empty
 * engine descriptor -- not a delegation. Outside the coordinated profile junit-jupiter is
 * still registered, so an inert engine that delegated everything would run the whole suite
 * twice; empty is what makes a consumer with the dependency but no CI context see Jupiter
 * behave exactly as if this engine were absent.
 *
 * <p>{@code discover()} is side-effect-free and never contacts the coordinator: it runs
 * once per candidate class in the scanner plus once for real, in every fork JVM. All
 * coordinator contact happens in {@code execute()}.
 */
public class Shard4jTestEngine implements TestEngine {

  private static final System.Logger log = System.getLogger(Shard4jTestEngine.class.getName());

  public static final String ENGINE_ID = "shard4j";

  @Override
  public String getId() {
    return ENGINE_ID;
  }

  @Override
  public TestDescriptor discover(EngineDiscoveryRequest request, UniqueId uniqueId) {
    ShardConfiguration configuration =
        ShardConfiguration.resolve(request.getConfigurationParameters());
    if (!configuration.enabled()) {
      return new EngineDescriptor(uniqueId, "shard4j");
    }
    JupiterDelegate jupiter = new JupiterDelegate(uniqueId);
    if (isUniqueIdOnly(request)) {
      // The build tool's rerun path. The ids are normalised to the wire form first, so a
      // rerun works whether they were recorded under this engine or under plain Jupiter.
      Shard4jEngineDescriptor root =
          new Shard4jEngineDescriptor(uniqueId, Shard4jEngineDescriptor.Mode.DIRECT, configuration);
      List<ExecutionId> ids =
          request.getSelectorsByType(UniqueIdSelector.class).stream()
              .map(selector -> ExecutionIdentity.executionId(selector.getUniqueId()))
              .toList();
      root.addChild(
          jupiter.discoverIds(
              ids, request.getConfigurationParameters(), request.getOutputDirectoryProvider()));
      return root;
    }
    Shard4jEngineDescriptor root =
        new Shard4jEngineDescriptor(
            uniqueId, Shard4jEngineDescriptor.Mode.COORDINATED, configuration);
    root.addChild(jupiter.discover(request));
    return root;
  }

  @Override
  public void execute(ExecutionRequest request) {
    EngineExecutionListener listener = request.getEngineExecutionListener();
    TestDescriptor root = request.getRootTestDescriptor();
    listener.executionStarted(root);
    try {
      if (root instanceof Shard4jEngineDescriptor descriptor && !root.getChildren().isEmpty()) {
        JupiterDelegate jupiter = new JupiterDelegate(root.getUniqueId());
        switch (descriptor.mode()) {
          case DIRECT -> runDirect(descriptor, jupiter, request);
          case COORDINATED -> runCoordinated(descriptor, jupiter, request);
          case INERT -> {
            // Unreachable today -- a disabled discovery returns a plain EngineDescriptor --
            // kept so a future mode addition cannot fall through silently.
          }
        }
      }
      listener.executionFinished(root, TestExecutionResult.successful());
    } catch (RuntimeException e) {
      log.log(System.Logger.Level.ERROR, "Shard execution failed", e);
      listener.executionFinished(root, TestExecutionResult.failed(e));
    } catch (Error e) {
      // An Error must still propagate -- the JVM may be dying -- but never past an
      // unfinished root: a started-and-never-finished engine node is how a crashed shard
      // would masquerade as a hung one in the build tool's report.
      log.log(System.Logger.Level.ERROR, "Shard execution failed", e);
      listener.executionFinished(root, TestExecutionResult.failed(e));
      throw e;
    }
  }

  /**
   * The build tool's own rerun path hands a unique-id-only request; those ids are executed
   * exactly, with no coordinator call -- rerunning a named failure locally must not need a
   * live session.
   */
  private void runDirect(
      Shard4jEngineDescriptor root, JupiterDelegate jupiter, ExecutionRequest request) {
    jupiter.execute(
        root.jupiterRoot(), request, new ForwardEverything(request.getEngineExecutionListener()));
  }

  private void runCoordinated(
      Shard4jEngineDescriptor root, JupiterDelegate jupiter, ExecutionRequest request) {
    DiscoveredCensus census = DiscoveredCensus.of(root.jupiterRoot());
    EngineExecutionListener listener = request.getEngineExecutionListener();
    TestDescriptor jupiterNode = root.jupiterRoot();
    listener.executionStarted(jupiterNode);
    try {
      if (census.isEmpty()) {
        // Everything was filtered away before execution; there is nothing to register and
        // a census of zero units is refused by the coordinator on purpose.
        log.log(
            System.Logger.Level.WARNING,
            "The pruned discovery contains no lease units; nothing to coordinate");
        return;
      }
      CoordinatorGateway gateway =
          new CoordinatorGateway(root.configuration(), census.unitIds());
      new ShardLoop(root.configuration(), jupiter, gateway, request).run(census);
    } finally {
      listener.executionFinished(jupiterNode, TestExecutionResult.successful());
    }
  }

  private static boolean isUniqueIdOnly(EngineDiscoveryRequest request) {
    List<UniqueIdSelector> uniqueIds = request.getSelectorsByType(UniqueIdSelector.class);
    return !uniqueIds.isEmpty()
        && uniqueIds.size() == request.getSelectorsByType(DiscoverySelector.class).size();
  }

  /** The direct path forwards the nested engine node too: it is the only execution there is. */
  private record ForwardEverything(EngineExecutionListener downstream)
      implements EngineExecutionListener {

    @Override
    public void dynamicTestRegistered(TestDescriptor descriptor) {
      downstream.dynamicTestRegistered(descriptor);
    }

    @Override
    public void executionStarted(TestDescriptor descriptor) {
      downstream.executionStarted(descriptor);
    }

    @Override
    public void executionSkipped(TestDescriptor descriptor, String reason) {
      downstream.executionSkipped(descriptor, reason);
    }

    @Override
    public void executionFinished(TestDescriptor descriptor, TestExecutionResult result) {
      downstream.executionFinished(descriptor, result);
    }
  }
}
