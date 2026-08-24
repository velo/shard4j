package com.marvinformatics.shard4j.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class HistoryKeyTest {

  @ParameterizedTest(name = "[{index}] {0}")
  @CsvSource(
      delimiter = '|',
      value = {
        "a plain method|[engine:junit-jupiter]/[class:com.example.orders.CartIT]/[method:total()]"
            + "|com.example.orders.CartIT#total()",
        "a method with parameters|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[method:total(java.lang.String, int)]"
            + "|com.example.orders.CartIT#total(java.lang.String, int)",
        "a template container|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[test-template:each(java.lang.String)]"
            + "|com.example.orders.CartIT#each(java.lang.String)",
        "a template invocation|[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[test-template:each(java.lang.String)]/[test-template-invocation:#7]"
            + "|com.example.orders.CartIT#each(java.lang.String)",
        "a static nested test class|[engine:junit-jupiter]"
            + "/[class:com.example.orders.CartIT$WhenEmpty]/[method:total(int[])]"
            + "|com.example.orders.CartIT$WhenEmpty#total(int[])",
      })
  void derivesThePrefixOfAnyShape(String shape, String wire, String expected) {
    assertEquals(expected, HistoryKey.from(ExecutionId.parse(wire)).value(), shape);
  }

  @Test
  void collapsesEveryInvocationOfOneTemplateOntoOneKey() {
    String template =
        "[engine:junit-jupiter]/[class:com.example.orders.CartIT]"
            + "/[test-template:each(java.lang.String)]";
    HistoryKey unit = HistoryKey.from(ExecutionId.parse(template));

    for (int n = 1; n <= 6; n++) {
      ExecutionId invocation = ExecutionId.parse(template + "/[test-template-invocation:#" + n + "]");

      assertEquals(unit, HistoryKey.from(invocation), "invocation #" + n);
    }
  }

  @Test
  void separatesOverloadsThatDifferOnlyInParameterTypes() {
    String prefix = "[engine:junit-jupiter]/[class:com.example.orders.CartIT]/[method:total";

    HistoryKey noArgs = HistoryKey.from(ExecutionId.parse(prefix + "()]"));
    HistoryKey oneArg = HistoryKey.from(ExecutionId.parse(prefix + "(int)]"));

    assertEquals("com.example.orders.CartIT#total()", noArgs.value());
    assertEquals("com.example.orders.CartIT#total(int)", oneArg.value());
  }
}
