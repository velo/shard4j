package com.marvinformatics.shard4j.coordinator.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Map;
import lombok.experimental.UtilityClass;

/**
 * Seeds duration history into a data directory before its container boots.
 *
 * <p>Plain history records, exactly as the coordinator writes them -- no import endpoint
 * and no special record type, so the seed IS the format and a change to it breaks these
 * tests rather than passing them against a shape nothing produces.
 */
@UtilityClass
class History {

  void seed(Path dataDir, Map<String, Long> durationMsByTestId) {
    try {
      Path historyDir = dataDir.resolve(CoordinatorContainers.TENANT_SLUG).resolve("history");
      Files.createDirectories(historyDir);
      StringBuilder lines = new StringBuilder();
      durationMsByTestId.forEach((testId, durationMs) -> lines.append(line(testId, durationMs)));
      Files.writeString(
          historyDir.resolve(LocalDate.now(ZoneOffset.UTC) + ".jsonl"), lines.toString());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private String line(String testId, long durationMs) {
    return "{\"type\":\"COMPLETION\",\"project\":\""
        + CoordinatorContainers.TENANT_KEY
        + "\",\"session\":\"seeded-elsewhere\",\"epoch\":1,\"testId\":\""
        + testId.replace("\\", "\\\\")
        + "\",\"unit\":true,\"shard\":0,\"pass\":\"MAIN\",\"outcome\":\"PASSED\",\"durationMs\":"
        + durationMs
        + ",\"firstOnShard\":false,\"ts\":\"2026-08-20T10:00:00Z\"}\n";
  }
}
