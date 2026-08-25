package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Four passing rows: the template whose invocations distribution spreads across shards. */
class SplitRowsFixture {

  @ParameterizedTest
  @ValueSource(strings = {"north", "east", "south", "west"})
  void rows(String value) {
    assertThat(value).isNotBlank();
  }
}
