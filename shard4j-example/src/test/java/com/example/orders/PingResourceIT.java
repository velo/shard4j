package com.example.orders;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Ordinary tests in an ordinary project. Nothing here extends a base class, carries an
 * annotation, or knows that shard4j exists -- which is the assertion this module makes.
 */
public class PingResourceIT {

  @Test
  void hello() {
    assertThat("pong").isEqualTo("pong");
  }

  @ParameterizedTest
  @ValueSource(strings = {"alpha", "beta", "gamma"})
  void each(String name) {
    assertThat(name.length()).isGreaterThan(2);
  }
}
