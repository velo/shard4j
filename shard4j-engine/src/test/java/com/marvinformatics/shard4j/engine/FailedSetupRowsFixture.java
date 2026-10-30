package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Two rows behind a throwing {@code @BeforeAll}: no invocation of it ever materialises. */
class FailedSetupRowsFixture {

  @BeforeAll
  static void brokenSetup() {
    throw new IllegalStateException("the shared test environment could not start");
  }

  @ParameterizedTest
  @ValueSource(strings = {"one", "two"})
  void rows(String value) {
    assertThat(value).isNotBlank();
  }
}
