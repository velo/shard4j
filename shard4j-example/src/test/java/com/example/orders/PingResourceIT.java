package com.example.orders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Ordinary tests in an ordinary project. Nothing here extends a base class, carries an
 * annotation, or knows that shard4j exists -- which is the assertion this module makes.
 */
class PingResourceIT {

  @Test
  void hello() {
    assertEquals("pong", "pong");
  }

  @ParameterizedTest
  @ValueSource(strings = {"alpha", "beta", "gamma"})
  void each(String name) {
    assertTrue(name.length() > 2);
  }
}
