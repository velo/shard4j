package com.marvinformatics.shard4j.engine;

import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/** A shape the census cannot lease; it must fail loudly, never be dropped. */
class FactoryShapeFixture {

  @TestFactory
  Stream<DynamicTest> generated() {
    return Stream.of(DynamicTest.dynamicTest("one", () -> {}));
  }
}
