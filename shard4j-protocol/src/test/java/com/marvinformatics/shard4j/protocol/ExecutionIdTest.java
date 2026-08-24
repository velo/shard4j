package com.marvinformatics.shard4j.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * An execution id is opaque here: the engine builds it through JUnit's {@code UniqueId}
 * API and everything downstream carries it verbatim, so the only protocol-level contract
 * is value semantics and the refusal of a blank one.
 */
class ExecutionIdTest {

  @Test
  void carriesItsWireFormVerbatim() {
    String wire =
        "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]/[method:hello()]";

    assertEquals(wire, new ExecutionId(wire).value());
    assertEquals(new ExecutionId(wire), new ExecutionId(wire));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t"})
  void refusesABlankId(String blank) {
    assertThrows(IllegalArgumentException.class, () -> new ExecutionId(blank));
  }
}
