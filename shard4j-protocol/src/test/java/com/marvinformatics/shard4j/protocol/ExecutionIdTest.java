package com.marvinformatics.shard4j.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.marvinformatics.shard4j.protocol.ExecutionId.Shape;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class ExecutionIdTest {

  @Test
  void parsesAPlainMethod() {
    String wire = "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]/[method:hello()]";

    ExecutionId id = ExecutionId.parse(wire);

    assertEquals(wire, id.value());
    assertEquals(Shape.METHOD, id.shape());
  }

  @Test
  void parsesATestTemplateContainer() {
    String wire =
        "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]"
            + "/[test-template:each(java.lang.String)]";

    ExecutionId id = ExecutionId.parse(wire);

    assertEquals(wire, id.value());
    assertEquals(Shape.TEST_TEMPLATE, id.shape());
    assertTrue(id.isLeaseUnit());
  }

  @Test
  void parsesATestTemplateInvocationLeaf() {
    String wire =
        "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]"
            + "/[test-template:each(java.lang.String)]/[test-template-invocation:#2]";

    ExecutionId id = ExecutionId.parse(wire);

    assertEquals(wire, id.value());
    assertEquals(Shape.TEST_TEMPLATE_INVOCATION, id.shape());
    assertFalse(id.isLeaseUnit());
  }

  @Test
  void aPlainMethodIsALeaseUnit() {
    ExecutionId id =
        ExecutionId.parse(
            "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]/[method:hello()]");

    assertTrue(id.isLeaseUnit());
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @CsvSource(
      delimiter = '|',
      value = {
        "a @Nested class|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[nested-class:WhenEmpty]/[method:total()]",
        "a @TestFactory|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[test-factory:cases()]",
        "a dynamic test leaf|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[test-factory:cases()]/[dynamic-test:#1]",
        "a container with no method|[engine:junit-jupiter]/[class:com.example.orders.CartIT]",
        "another engine at the root|[engine:junit-vintage]/[class:com.example.orders.CartIT]"
            + "/[method:total()]",
        "no engine segment at all|[class:com.example.orders.CartIT]/[method:total()]",
        "an invocation under a plain method|[engine:junit-jupiter]"
            + "/[class:com.example.orders.CartIT]/[method:total()]/[test-template-invocation:#1]",
        "a trailing slash|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[method:total()]/",
        "a segment missing its brackets|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/method:total()",
        "a segment with no value|[engine:junit-jupiter]/[class:]/[method:total()]",
        "a method with no parameter list|[engine:junit-jupiter]"
            + "/[class:com.example.orders.CartIT]/[method:total]",
        "a method with unbalanced parentheses|[engine:junit-jupiter]"
            + "/[class:com.example.orders.CartIT]/[method:total(java.lang.String]",
        "an unnumbered invocation|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[test-template:each(java.lang.String)]/[test-template-invocation:2]",
        "a zero invocation index|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[test-template:each(java.lang.String)]/[test-template-invocation:#0]",
        "a class name that is not a class name|[engine:junit-jupiter]/[class:com.example..CartIT]"
            + "/[method:total()]",
        "a parameter type that is not a type|[engine:junit-jupiter]"
            + "/[class:com.example.orders.CartIT]/[method:total(java lang String)]",
        "the engine segment left on the wire form|[engine:shard4j]/[engine:junit-jupiter]"
            + "/[class:com.example.orders.CartIT]/[method:total()]",
      })
  void refusesAnIdItCannotAccountFor(String shape, String raw) {
    MalformedExecutionIdException thrown =
        assertThrows(MalformedExecutionIdException.class, () -> ExecutionId.parse(raw), shape);

    assertEquals(raw, thrown.id(), "the exception must name the offending id");
    assertTrue(thrown.getMessage().contains(raw), "the message must name the offending id");
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "/", "[]", "not an id at all"})
  void refusesAnIdThatIsNotAnIdAtAll(String raw) {
    assertThrows(MalformedExecutionIdException.class, () -> ExecutionId.parse(raw));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @CsvSource(
      delimiter = '|',
      value = {
        "a static nested test class|[engine:junit-jupiter]"
            + "/[class:com.example.orders.CartIT$WhenEmpty]/[method:total()]",
        "an array parameter|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[method:sum(int[])]",
        "an array of a nested type|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[test-template:each(com.example.orders.Cart$Line[])]",
        "several parameters|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[test-template:each(java.lang.String, int, java.util.List)]",
        "a primitive parameter|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[method:total(long)]",
        "a two-digit invocation index|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[test-template:each(java.lang.String)]/[test-template-invocation:#12]",
        "a default-package test class|[engine:junit-jupiter]/[class:CartIT]/[method:total()]",
      })
  void roundTripsAnIdVerbatim(String shape, String wire) {
    ExecutionId id = ExecutionId.parse(wire);

    assertEquals(wire, id.value(), shape);
    assertEquals(wire, ExecutionId.parse(id.value()).value(), shape);
  }

  private static final String WIRE =
      "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]/[method:hello()]";

  @Test
  void stripsTheOuterEngineSegmentOutbound() {
    String nested = "[engine:shard4j]/" + WIRE;

    assertEquals(WIRE, ExecutionId.stripOuterEngineSegment(nested));
  }

  @Test
  void leavesAWireFormIdAlone() {
    assertEquals(WIRE, ExecutionId.stripOuterEngineSegment(WIRE));
  }

  @Test
  void stripsEveryOuterEngineSegmentSoNestingDepthCannotMatter() {
    String twiceNested = "[engine:outer]/[engine:shard4j]/" + WIRE;

    assertEquals(WIRE, ExecutionId.stripOuterEngineSegment(twiceNested));
  }

  @Test
  void refusesToStripAnIdThatIsNotJupiterRootedAtAll() {
    String vintage = "[engine:shard4j]/[engine:junit-vintage]/[class:CartIT]/[method:total()]";

    MalformedExecutionIdException thrown =
        assertThrows(
            MalformedExecutionIdException.class, () -> ExecutionId.stripOuterEngineSegment(vintage));

    assertEquals(vintage, thrown.id());
  }

  @Test
  void rePrependsTheOuterEngineSegmentInbound() {
    ExecutionId id = ExecutionId.parse(WIRE);

    assertEquals("[engine:shard4j]/" + WIRE, id.withOuterEngineSegment("shard4j"));
  }

  @Test
  void survivesAStripAndRePrependUnchanged() {
    ExecutionId id = ExecutionId.parse(WIRE);

    String nested = id.withOuterEngineSegment("shard4j");

    assertEquals(WIRE, ExecutionId.stripOuterEngineSegment(nested));
    assertEquals(id, ExecutionId.parse(ExecutionId.stripOuterEngineSegment(nested)));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"has space", "has/slash", "has]bracket", "has:colon"})
  void refusesAnEngineIdThatWouldCorruptTheId(String engineId) {
    ExecutionId id = ExecutionId.parse(WIRE);

    assertThrows(IllegalArgumentException.class, () -> id.withOuterEngineSegment(engineId));
  }
}
