package com.marvinformatics.shard4j.coordinator.storage;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * The storage layer's own mapper, deliberately separate from the web layer's: on-disk
 * records must keep parsing across framework upgrades, so unknown fields are tolerated and
 * timestamps stay ISO strings a human can read in the raw file.
 */
final class StorageJson {

  static final ObjectMapper MAPPER =
      JsonMapper.builder().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

  private StorageJson() {}
}
