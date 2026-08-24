package com.marvinformatics.shard4j.engine;

import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.UniqueId;
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
 * <p>Skeleton: discovery yields an empty descriptor, so the engine is inert and a
 * consumer that merely has the jar on its classpath sees no behaviour change.
 */
public class Shard4jTestEngine implements TestEngine {

  public static final String ENGINE_ID = "shard4j";

  @Override
  public String getId() {
    return ENGINE_ID;
  }

  @Override
  public TestDescriptor discover(EngineDiscoveryRequest request, UniqueId uniqueId) {
    return new EngineDescriptor(uniqueId, "shard4j");
  }

  @Override
  public void execute(ExecutionRequest request) {
    // No-op while the engine is a skeleton. All coordinator contact belongs here and
    // never in discover(), which runs many times per fork group and in every fork JVM.
  }
}
