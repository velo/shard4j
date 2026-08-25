package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Three passing rows, seeded into history as four: the parameter set shrank since the
 * coordinator last measured it, which is what makes a handed-out {@code #4} stale.
 */
class DriftRowsFixture {

  @ParameterizedTest
  @ValueSource(strings = {"one", "two", "three"})
  void rows(String value) {
    assertThat(value).isNotBlank();
  }
}
