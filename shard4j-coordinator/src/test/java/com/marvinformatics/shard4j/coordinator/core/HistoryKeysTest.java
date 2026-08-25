package com.marvinformatics.shard4j.coordinator.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class HistoryKeysTest {

  @Test
  void plainMethodCollapsesToClassHashMethod() {
    assertThat(
            HistoryKeys.of(
                    "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]/[method:hello()]")
                .value())
        .isEqualTo("com.example.orders.PingResourceIT#hello()");
  }

  @Test
  void templateMethodAndItsInvocationsShareOneKey() {
    String template =
        "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]"
            + "/[test-template:each(java.lang.String)]";
    assertThat(HistoryKeys.of(template))
        .isEqualTo(HistoryKeys.of(template + "/[test-template-invocation:#3]"));
    assertThat(HistoryKeys.of(template).value())
        .isEqualTo("com.example.orders.PingResourceIT#each(java.lang.String)");
  }

  @Test
  void parameterTypesSurviveBecauseTheyDisambiguateOverloads() {
    assertThat(
            HistoryKeys.of(
                    "[engine:junit-jupiter]/[class:com.example.A]/[method:run(int, java.lang.String)]")
                .value())
        .isEqualTo("com.example.A#run(int, java.lang.String)");
  }

  @Test
  void anyOtherShapeFailsLoudlyNamingTheId() {
    assertThatThrownBy(() -> HistoryKeys.of("[engine:junit-jupiter]/[class:com.example.A]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("[engine:junit-jupiter]/[class:com.example.A]");
    assertThatThrownBy(
            () ->
                HistoryKeys.of(
                    "[engine:junit-vintage]/[class:com.example.A]/[method:run()]"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
