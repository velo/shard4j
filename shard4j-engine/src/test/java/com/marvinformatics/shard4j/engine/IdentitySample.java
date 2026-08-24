package com.marvinformatics.shard4j.engine;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Discovered and executed by {@link ExecutionIdentityTest} through a real
 * {@code JupiterTestEngine}, never by the build directly: the name deliberately matches no
 * surefire pattern. It exists to produce real descriptors of every shape the identity
 * functions must handle.
 */
class IdentitySample {

  @Test
  void hello() {}

  @Test
  void sum(int[] numbers) {}

  @ParameterizedTest
  @ValueSource(strings = {"a", "b", "c"})
  void each(String value) {}

  @Nested
  class WhenNested {

    @Test
    void deep(String left, int right) {}
  }
}
