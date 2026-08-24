package com.marvinformatics.shard4j.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ServiceLoader;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.UniqueId;

class Shard4jTestEngineTest {

  @Test
  void isDiscoverableByServiceLoader() {
    boolean registered =
        StreamSupport.stream(ServiceLoader.load(TestEngine.class).spliterator(), false)
            .anyMatch(engine -> Shard4jTestEngine.ENGINE_ID.equals(engine.getId()));

    assertTrue(registered, "shard4j must be registered as a TestEngine service");
  }

  @Test
  void isInertUntilConfigured() {
    var engineId = UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID);
    var descriptor = new Shard4jTestEngine().discover(null, engineId);

    assertEquals(0, descriptor.getChildren().size());
  }
}
