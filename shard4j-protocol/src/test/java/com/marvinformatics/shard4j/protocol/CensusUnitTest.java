package com.marvinformatics.shard4j.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class CensusUnitTest {

  @Test
  void plainMethodCollapsesToClassHashMethod() {
    assertThat(
            CensusUnit.historyKeyOf(
                    "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]/[method:hello()]")
                .value())
        .isEqualTo("com.example.orders.PingResourceIT#hello()");
  }

  @Test
  void templateMethodAndItsInvocationsShareOneKey() {
    String template =
        "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]"
            + "/[test-template:each(java.lang.String)]";
    assertThat(CensusUnit.historyKeyOf(template))
        .isEqualTo(CensusUnit.historyKeyOf(template + "/[test-template-invocation:#3]"));
    assertThat(CensusUnit.historyKeyOf(template).value())
        .isEqualTo("com.example.orders.PingResourceIT#each(java.lang.String)");
  }

  @Test
  void parameterTypesSurviveBecauseTheyDisambiguateOverloads() {
    assertThat(
            CensusUnit.historyKeyOf(
                    "[engine:junit-jupiter]/[class:com.example.A]/[method:run(int, java.lang.String)]")
                .value())
        .isEqualTo("com.example.A#run(int, java.lang.String)");
  }

  @Test
  void parseSeparatesTemplateShapeAndInvocationPosition() {
    String template =
        "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]"
            + "/[test-template:each(java.lang.String)]";
    CensusUnit plain =
        CensusUnit.parse(
            "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]/[method:hello()]");
    assertThat(plain.template()).isFalse();
    assertThat(plain.invocation()).isNull();

    CensusUnit whole = CensusUnit.parse(template);
    assertThat(whole.template()).isTrue();
    assertThat(whole.invocation()).isNull();

    CensusUnit invocation = CensusUnit.parse(template + "/[test-template-invocation:#3]");
    assertThat(invocation.template()).isTrue();
    assertThat(invocation.invocation()).isEqualTo(3);
    assertThat(invocation.historyKey()).isEqualTo(whole.historyKey());
  }

  @Test
  void atPositionComposesTheIdTheParserReadsBack() {
    String template =
        "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]"
            + "/[test-template:each(java.lang.String)]";
    CensusUnit composed = CensusUnit.parse(template).atPosition(4);
    assertThat(composed.id()).isEqualTo(template + "/[test-template-invocation:#4]");
    assertThat(composed).isEqualTo(CensusUnit.parse(composed.id()));
  }

  @Test
  void onlyAWholeTemplateCanBeAddressedByPosition() {
    String template =
        "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]"
            + "/[test-template:each(java.lang.String)]";
    CensusUnit plain =
        CensusUnit.parse(
            "[engine:junit-jupiter]/[class:com.example.orders.PingResourceIT]/[method:hello()]");
    assertThatThrownBy(() -> plain.atPosition(1)).isInstanceOf(IllegalArgumentException.class);
    CensusUnit invocation = CensusUnit.parse(template).atPosition(1);
    assertThatThrownBy(() -> invocation.atPosition(2))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void anyOtherShapeFailsLoudlyNamingTheId() {
    assertThatThrownBy(() -> CensusUnit.parse("[engine:junit-jupiter]/[class:com.example.A]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("[engine:junit-jupiter]/[class:com.example.A]");
    assertThatThrownBy(
            () ->
                CensusUnit.parse(
                    "[engine:junit-vintage]/[class:com.example.A]/[method:run()]"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
