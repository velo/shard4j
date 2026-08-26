package com.marvinformatics.shard4j.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ShardConfigurationTest {

  private static final Map<String, String> SECRET_ONLY =
      Map.of(ShardConfiguration.SECRET_ENVIRONMENT, "test-secret");

  private static Map<String, String> completeParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(ShardConfiguration.ENABLED, "true");
    parameters.put(ShardConfiguration.COORDINATOR_URL, "http://localhost:1");
    parameters.put(ShardConfiguration.SESSION_ID, "7f3a");
    parameters.put(ShardConfiguration.SHARD_INDEX, "3");
    return parameters;
  }

  private static ShardConfiguration resolve(
      Map<String, String> parameters, Map<String, String> environment) {
    return ShardConfiguration.resolve(new MapConfigurationParameters(parameters), environment);
  }

  @Test
  void givenNoConfiguration_whenResolving_thenTheEngineIsInert() {
    ShardConfiguration configuration = resolve(Map.of(), Map.of());

    assertThat(configuration.enabled()).isFalse();
  }

  @Test
  void givenEnabledFalse_whenResolving_thenNothingElseIsRequired() {
    ShardConfiguration configuration =
        resolve(Map.of(ShardConfiguration.ENABLED, "false"), Map.of());

    assertThat(configuration.enabled()).isFalse();
  }

  @Test
  void givenEnabledWithEverySetting_whenResolving_thenAllFieldsArrive() {
    Map<String, String> parameters = completeParameters();
    parameters.put(ShardConfiguration.ATTEMPT, "2");
    parameters.put(ShardConfiguration.CONCURRENCY, "2");
    parameters.put(ShardConfiguration.RETRY_BUDGET, "90");
    parameters.put(ShardConfiguration.DEADLINE, "2026-08-25T10:15:30Z");
    parameters.put(ShardConfiguration.ABORT_GUARD, "false");
    parameters.put(ShardConfiguration.METADATA_PREFIX + "run_id", "42");

    ShardConfiguration configuration = resolve(parameters, SECRET_ONLY);

    assertThat(configuration.enabled()).isTrue();
    assertThat(configuration.coordinatorUrl()).isEqualTo("http://localhost:1");
    assertThat(configuration.coordinatorSecret()).isEqualTo("test-secret");
    assertThat(configuration.sessionId()).isEqualTo("7f3a");
    assertThat(configuration.shardIndex()).isEqualTo(3);
    assertThat(configuration.attempt()).isEqualTo(2);
    assertThat(configuration.concurrency()).isEqualTo(2);
    assertThat(configuration.retryBudget()).isEqualTo(Duration.ofSeconds(90));
    assertThat(configuration.deadline()).isEqualTo(Instant.parse("2026-08-25T10:15:30Z"));
    assertThat(configuration.allLeasedAbortedIsFailure()).isFalse();
    assertThat(configuration.metadata()).containsEntry("run_id", "42");
  }

  @Test
  void givenNoConcurrency_whenResolving_thenOneSlotIsTheDefault() {
    ShardConfiguration configuration = resolve(completeParameters(), SECRET_ONLY);

    assertThat(configuration.concurrency()).isEqualTo(1);
  }

  @Test
  void givenAConcurrencyBelowOne_whenResolving_thenTheFailureNamesTheKey() {
    Map<String, String> parameters = completeParameters();
    parameters.put(ShardConfiguration.CONCURRENCY, "0");

    assertThatThrownBy(() -> resolve(parameters, SECRET_ONLY))
        .isInstanceOf(ShardConfigurationException.class)
        .hasMessageContaining(ShardConfiguration.CONCURRENCY);
  }

  @Test
  void givenANonNumericConcurrency_whenResolving_thenTheFailureNamesTheKey() {
    Map<String, String> parameters = completeParameters();
    parameters.put(ShardConfiguration.CONCURRENCY, "both");

    assertThatThrownBy(() -> resolve(parameters, SECRET_ONLY))
        .isInstanceOf(ShardConfigurationException.class)
        .hasMessageContaining(ShardConfiguration.CONCURRENCY);
  }

  @Test
  void givenEnabledWithAMissingKey_whenResolving_thenTheFailureNamesTheKey() {
    Map<String, String> parameters = completeParameters();
    parameters.remove(ShardConfiguration.SESSION_ID);

    assertThatThrownBy(() -> resolve(parameters, SECRET_ONLY))
        .isInstanceOf(ShardConfigurationException.class)
        .hasMessageContaining(ShardConfiguration.SESSION_ID);
  }

  @Test
  void givenASecretSuppliedAsAProperty_whenResolving_thenItIsRefusedWithTheReason() {
    Map<String, String> parameters = completeParameters();
    parameters.put(ShardConfiguration.SECRET_PROPERTY, "leaky");

    assertThatThrownBy(() -> resolve(parameters, SECRET_ONLY))
        .isInstanceOf(ShardConfigurationException.class)
        .hasMessageContaining("ps output");
  }

  @Test
  void givenNoExportedSecret_whenResolving_thenTheFailureNamesTheVariable() {
    assertThatThrownBy(() -> resolve(completeParameters(), Map.of()))
        .isInstanceOf(ShardConfigurationException.class)
        .hasMessageContaining(ShardConfiguration.SECRET_ENVIRONMENT);
  }

  @Test
  void givenValuesOnlyInTheEnvironment_whenResolving_thenTheUnderscoreMappingApplies() {
    Map<String, String> environment = new HashMap<>(SECRET_ONLY);
    environment.put("SHARD_ENABLED", "true");
    environment.put("SHARD_COORDINATOR_URL", "http://localhost:2");
    environment.put("SHARD_SESSION_ID", "9b1c");
    environment.put("SHARD_INDEX", "0");
    environment.put("SHARD_PASS", "main");
    environment.put("SHARD_METADATA_SHA", "d7f561f");

    ShardConfiguration configuration = resolve(Map.of(), environment);

    assertThat(configuration.enabled()).isTrue();
    assertThat(configuration.coordinatorUrl()).isEqualTo("http://localhost:2");
    assertThat(configuration.metadata()).containsEntry("sha", "d7f561f");
  }

  @Test
  void givenAnIsoRetryBudget_whenResolving_thenItParsesAsADuration() {
    Map<String, String> parameters = completeParameters();
    parameters.put(ShardConfiguration.RETRY_BUDGET, "PT2M30S");

    assertThat(resolve(parameters, SECRET_ONLY).retryBudget())
        .isEqualTo(Duration.ofSeconds(150));
  }
}
