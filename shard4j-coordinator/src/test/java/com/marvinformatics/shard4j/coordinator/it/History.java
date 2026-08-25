package com.marvinformatics.shard4j.coordinator.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.TreeMap;
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
    StringBuilder lines = new StringBuilder();
    durationMsByTestId.forEach(
        (testId, durationMs) -> lines.append(line(testId, true, durationMs)));
    append(dataDir, lines);
  }

  /**
   * One template that ran whole and passed: the unit row plus one row per invocation,
   * which is exactly what the coordinator appends for a PASSED template -- and exactly
   * what makes its breakdown a complete, distributable plan on the next boot.
   */
  void seedTemplate(Path dataDir, String templateId, Map<Integer, Long> durationMsByPosition) {
    StringBuilder lines = new StringBuilder();
    long total = durationMsByPosition.values().stream().mapToLong(Long::longValue).sum();
    lines.append(line(templateId, true, total));
    new TreeMap<>(durationMsByPosition)
        .forEach(
            (position, durationMs) ->
                lines.append(
                    line(
                        templateId + "/[test-template-invocation:#" + position + "]",
                        false,
                        durationMs)));
    append(dataDir, lines);
  }

  private void append(Path dataDir, StringBuilder lines) {
    try {
      Path historyDir = dataDir.resolve(CoordinatorContainers.TENANT_SLUG).resolve("history");
      Files.createDirectories(historyDir);
      Files.writeString(
          historyDir.resolve(LocalDate.now(ZoneOffset.UTC) + ".jsonl"),
          lines.toString(),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private String line(String testId, boolean unit, long durationMs) {
    return "{\"type\":\"COMPLETION\",\"project\":\""
        + CoordinatorContainers.TENANT_KEY
        + "\",\"session\":\"seeded-elsewhere\",\"epoch\":1,\"testId\":\""
        + testId.replace("\\", "\\\\")
        + "\",\"unit\":"
        + unit
        + ",\"shard\":0,\"pass\":\"MAIN\",\"outcome\":\"PASSED\",\"durationMs\":"
        + durationMs
        + (unit ? ",\"firstOnShard\":false" : "")
        + ",\"ts\":\"2026-08-20T10:00:00Z\"}\n";
  }
}
