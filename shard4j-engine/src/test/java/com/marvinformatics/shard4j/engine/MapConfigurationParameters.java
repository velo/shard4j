package com.marvinformatics.shard4j.engine;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.platform.engine.ConfigurationParameters;

/** A launcher-free stand-in so tests control exactly what the engine can see. */
record MapConfigurationParameters(Map<String, String> values) implements ConfigurationParameters {

  @Override
  public Optional<String> get(String key) {
    return Optional.ofNullable(values.get(key));
  }

  @Override
  public Optional<Boolean> getBoolean(String key) {
    return get(key).map(Boolean::parseBoolean);
  }

  @Override
  public Set<String> keySet() {
    return values.keySet();
  }
}
