package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.DiscoveryFilter;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.reporting.OutputDirectoryProvider;

class DiscoveredCensusTest {

  private static TestDescriptor discover(Class<?>... fixtures) {
    JupiterDelegate jupiter =
        new JupiterDelegate(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID));
    return jupiter.discover(new ClassesRequest(List.of(fixtures)));
  }

  @Test
  void givenPlainAndTemplateShapes_whenBuildingTheCensus_thenOneUnitPerMethodNeverPerInvocation() {
    DiscoveredCensus census =
        DiscoveredCensus.of(discover(PlainShapesFixture.class, RowsFixture.class));

    String plain = PlainShapesFixture.class.getName();
    String rows = RowsFixture.class.getName();
    assertThat(census.unitIds())
        .containsExactly(
            "[engine:junit-jupiter]/[class:" + plain + "]/[method:abortsInBody()]",
            "[engine:junit-jupiter]/[class:" + plain + "]/[method:disabled()]",
            "[engine:junit-jupiter]/[class:" + plain + "]/[method:fails()]",
            "[engine:junit-jupiter]/[class:" + plain + "]/[method:passes()]",
            "[engine:junit-jupiter]/[class:"
                + rows
                + "]/[test-template:rows(java.lang.String)]");
  }

  @Test
  void givenADisabledLeaf_whenBuildingTheCensus_thenItIsIndistinguishableAndStaysIn() {
    // Disabled-ness surfaces only at execution; the census admits the leaf and it will
    // report SKIPPED, which satisfies coverage.
    DiscoveredCensus census = DiscoveredCensus.of(discover(PlainShapesFixture.class));

    assertThat(census.unitIds())
        .anyMatch(unit -> unit.endsWith("[method:disabled()]"));
  }

  @Test
  void givenATestFactory_whenBuildingTheCensus_thenItFailsLoudlyNamingTheId() {
    TestDescriptor root = discover(FactoryShapeFixture.class);

    assertThatThrownBy(() -> DiscoveredCensus.of(root))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(FactoryShapeFixture.class.getName())
        .hasMessageContaining("test-factory");
  }

  @Test
  void givenANestedClass_whenBuildingTheCensus_thenItFailsLoudlyNamingTheId() {
    TestDescriptor root = discover(IdentitySample.class);

    assertThatThrownBy(() -> DiscoveredCensus.of(root))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("nested-class");
  }

  private record ClassesRequest(List<Class<?>> classes) implements EngineDiscoveryRequest {

    @Override
    public <T extends DiscoverySelector> List<T> getSelectorsByType(Class<T> selectorType) {
      return classes.stream()
          .<DiscoverySelector>map(DiscoverySelectors::selectClass)
          .filter(selectorType::isInstance)
          .map(selectorType::cast)
          .toList();
    }

    @Override
    public <T extends DiscoveryFilter<?>> List<T> getFiltersByType(Class<T> filterType) {
      return List.of();
    }

    @Override
    public ConfigurationParameters getConfigurationParameters() {
      return new MapConfigurationParameters(Map.of());
    }

    @Override
    public OutputDirectoryProvider getOutputDirectoryProvider() {
      return EngineTestHarness.outputDirectoryProvider();
    }
  }
}
