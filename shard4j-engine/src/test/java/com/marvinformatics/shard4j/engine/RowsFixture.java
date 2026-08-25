package com.marvinformatics.shard4j.engine;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** One template unit whose middle row fails: the aggregate must be FAILED. */
class RowsFixture {

  @ParameterizedTest
  @ValueSource(strings = {"alpha", "broken", "gamma"})
  void rows(String value) {
    if (value.equals("broken")) {
      fail("row rejected: " + value);
    }
  }
}
