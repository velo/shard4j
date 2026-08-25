package com.marvinformatics.shard4j.engine;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.EngineDescriptor;

/**
 * The engine's root, carrying what discovery decided into {@code execute()}: the resolved
 * configuration and how this run is to be handled. The delegated Jupiter tree hangs
 * beneath it, so the launcher's own post-discovery filtering prunes the same tree the
 * census is later read from.
 */
@Getter
@Accessors(fluent = true)
final class Shard4jEngineDescriptor extends EngineDescriptor {

  enum Mode {
    /** {@code shard.enabled} absent or false: an empty engine, indistinguishable from absent. */
    INERT,
    /** A unique-id-only request -- the build tool's own rerun path: no coordinator contact. */
    DIRECT,
    /** The coordinated loop: register, claim, execute, report, barrier. */
    COORDINATED
  }

  private final Mode mode;
  private final ShardConfiguration configuration;

  Shard4jEngineDescriptor(UniqueId uniqueId, Mode mode, ShardConfiguration configuration) {
    super(uniqueId, "shard4j");
    this.mode = mode;
    this.configuration = configuration;
  }

  TestDescriptor jupiterRoot() {
    return getChildren().stream()
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No delegated engine beneath " + getUniqueId()));
  }
}
