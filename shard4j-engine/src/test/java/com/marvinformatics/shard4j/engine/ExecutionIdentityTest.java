package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.marvinformatics.shard4j.protocol.ExecutionId;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.engine.JupiterTestEngine;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.DiscoveryFilter;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.OutputDirectoryCreator;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.support.store.Namespace;
import org.junit.platform.engine.support.store.NamespacedHierarchicalStore;

/**
 * Every descriptor here is produced by the real {@code JupiterTestEngine} over
 * {@link IdentitySample}, so what these tests pin is what a consumer's suite will actually
 * hand the engine -- including the {@code [I} array rendering and the {@code ", "}
 * parameter separator the known-answer ordering hashes depend on.
 */
class ExecutionIdentityTest {

  private static final String SAMPLE = IdentitySample.class.getName();
  private static final String CLASS_PREFIX = "[engine:junit-jupiter]/[class:" + SAMPLE + "]";

  private static TestDescriptor engineRoot;
  private static List<TestDescriptor> invocations;

  @BeforeAll
  static void discoverAndExecuteTheSample() {
    JupiterTestEngine jupiter = new JupiterTestEngine();
    SampleDiscoveryRequest request = new SampleDiscoveryRequest();
    engineRoot = jupiter.discover(request, UniqueId.forEngine("junit-jupiter"));

    invocations = new ArrayList<>();
    EngineExecutionListener capture =
        new EngineExecutionListener() {
          @Override
          public void dynamicTestRegistered(TestDescriptor descriptor) {
            invocations.add(descriptor);
          }
        };
    jupiter.execute(
        ExecutionRequest.create(
            engineRoot,
            capture,
            request.getConfigurationParameters(),
            request.getOutputDirectoryCreator(),
            new NamespacedHierarchicalStore<Namespace>(
                new NamespacedHierarchicalStore<Namespace>(null))));
  }

  private static TestDescriptor descriptor(String uniqueIdSuffix) {
    UniqueId uniqueId = UniqueId.parse(CLASS_PREFIX + uniqueIdSuffix);
    return engineRoot.getDescendants().stream()
        .filter(candidate -> candidate.getUniqueId().equals(uniqueId))
        .findFirst()
        .orElseThrow(() -> new AssertionError("not discovered: " + uniqueId));
  }

  @Test
  void theExecutionIdIsTheDescriptorsUniqueIdVerbatim() {
    TestDescriptor hello = descriptor("/[method:hello()]");

    assertThat(ExecutionIdentity.executionId(hello))
        .isEqualTo(new ExecutionId(hello.getUniqueId().toString()));
    assertThat(ExecutionIdentity.executionId(hello).value())
        .isEqualTo(CLASS_PREFIX + "/[method:hello()]");
  }

  @Test
  void dropsEveryEngineSegmentAnOuterEnginePrepended() {
    UniqueId wire = descriptor("/[method:hello()]").getUniqueId();
    UniqueId nested = UniqueId.forEngine("outer").appendEngine(Shard4jTestEngine.ENGINE_ID);
    for (UniqueId.Segment segment : wire.getSegments()) {
      nested = nested.append(segment);
    }

    assertThat(ExecutionIdentity.executionId(nested).value()).isEqualTo(wire.toString());
  }

  @Test
  void refusesAUniqueIdThatIsNotJupiterRooted() {
    UniqueId vintage =
        UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID)
            .appendEngine("junit-vintage")
            .append("class", SAMPLE);

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> ExecutionIdentity.executionId(vintage))
        .withMessageContaining(vintage.toString());
  }

  @Test
  void refusesAJupiterRootedIdWhoseFirstSegmentIsNotAClass() {
    UniqueId runner = UniqueId.forEngine("junit-jupiter").append("runner", SAMPLE);

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> ExecutionIdentity.executionId(runner))
        .withMessageContaining("[class:");
  }

  @Test
  void aMethodAndATemplateAreTheirOwnLeaseUnit() {
    TestDescriptor hello = descriptor("/[method:hello()]");
    TestDescriptor template = descriptor("/[test-template:each(java.lang.String)]");

    assertThat(ExecutionIdentity.leaseId(hello)).isEqualTo(ExecutionIdentity.executionId(hello));
    assertThat(ExecutionIdentity.leaseId(template))
        .isEqualTo(ExecutionIdentity.executionId(template));
  }

  @Test
  void anInvocationsLeaseIsItsTemplate() {
    TestDescriptor template = descriptor("/[test-template:each(java.lang.String)]");

    assertThat(invocations).as("the sample template must have really run").hasSize(3);
    for (TestDescriptor invocation : invocations) {
      assertThat(ExecutionIdentity.leaseId(invocation))
          .isEqualTo(ExecutionIdentity.executionId(template));
    }
  }






  @Test
  void survivesAStripAndRePrependUnchanged() {
    TestDescriptor hello = descriptor("/[method:hello()]");
    ExecutionId wire = ExecutionIdentity.executionId(hello);

    UniqueId handedBack =
        ExecutionIdentity.underEngineRoot(
            UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID), wire);

    assertThat(handedBack.hasPrefix(UniqueId.forEngine(Shard4jTestEngine.ENGINE_ID))).isTrue();
    assertThat(ExecutionIdentity.executionId(handedBack)).isEqualTo(wire);
  }

  /**
   * The launcher normally supplies this request; building it by hand keeps the test on the
   * engine API alone, which is the only surface the shard4j engine itself may use.
   */
  private static final class SampleDiscoveryRequest implements EngineDiscoveryRequest {

    @Override
    public <T extends DiscoverySelector> List<T> getSelectorsByType(Class<T> selectorType) {
      DiscoverySelector selector = DiscoverySelectors.selectClass(IdentitySample.class);
      return selectorType.isInstance(selector) ? List.of(selectorType.cast(selector)) : List.of();
    }

    @Override
    public <T extends DiscoveryFilter<?>> List<T> getFiltersByType(Class<T> filterType) {
      return List.of();
    }

    @Override
    public ConfigurationParameters getConfigurationParameters() {
      return new ConfigurationParameters() {
        @Override
        public Optional<String> get(String key) {
          return Optional.empty();
        }

        @Override
        public Optional<Boolean> getBoolean(String key) {
          return Optional.empty();
        }

        @Override
        public Set<String> keySet() {
          return Set.of();
        }

        @Override
        public int size() {
          return 0;
        }
      };
    }

    @Override
    public OutputDirectoryCreator getOutputDirectoryCreator() {
      return new OutputDirectoryCreator() {
        @Override
        public Path getRootDirectory() {
          return Path.of("target");
        }

        @Override
        public Path createOutputDirectory(TestDescriptor descriptor) {
          return Path.of("target");
        }
      };
    }
  }
}
