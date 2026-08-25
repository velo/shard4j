package com.marvinformatics.shard4j.engine;

import java.nio.file.Path;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.OutputDirectoryCreator;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.EngineDescriptor;
import org.junit.platform.engine.support.store.Namespace;
import org.junit.platform.engine.support.store.NamespacedHierarchicalStore;

/** Builds the outer execution plumbing the engine normally receives from the launcher. */
@UtilityClass
class EngineTestHarness {

  ExecutionRequest outerRequest(EngineExecutionListener listener) {
    return ExecutionRequest.create(
        new EngineDescriptor(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID), "outer"),
        listener,
        new MapConfigurationParameters(Map.of()),
        outputDirectoryCreator(),
        new NamespacedHierarchicalStore<>(
            new NamespacedHierarchicalStore<Namespace>(null)));
  }

  OutputDirectoryCreator outputDirectoryCreator() {
    return new OutputDirectoryCreator() {
      @Override
      public Path getRootDirectory() {
        return Path.of("target");
      }

      @Override
      public Path createOutputDirectory(TestDescriptor descriptor) {
        return Path.of("target");
      }
    };
  }
}
