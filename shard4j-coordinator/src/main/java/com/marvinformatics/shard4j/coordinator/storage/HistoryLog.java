package com.marvinformatics.shard4j.coordinator.storage;

import tools.jackson.core.JacksonException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Per-completion duration records, written as each test completes -- never at session end,
 * which is what makes "losing timing data is acceptable" true.
 *
 * <p>Appends fail soft on purpose: hand-outs must never fail because history cannot be
 * written, but the warning is explicit because silent history loss is how an unbalanced
 * shard spread comes back unnoticed.
 */
@Slf4j
public final class HistoryLog implements AutoCloseable {

  private final Path dir;
  private final DailyJsonl out;

  public HistoryLog(Path dir) {
    this.dir = dir;
    this.out = new DailyJsonl(dir);
  }

  public void append(LogRecord record) {
    try {
      out.append(StorageJson.MAPPER.writeValueAsBytes(record));
    } catch (IOException e) {
      log.warn(
          "Duration-history append failed; balancing is degrading toward hash order: {}",
          e.toString());
    }
  }

  /**
   * The cold-start path: with no compacted snapshot yet, the store is rebuilt from the raw
   * day files -- which is also how a pre-seeded directory (plain records written before
   * first start, no import endpoint, no special record type) becomes warm.
   */
  public List<LogRecord> readWithin(Duration retention, Instant now) {
    LocalDate oldest = now.minus(retention).atZone(ZoneOffset.UTC).toLocalDate();
    List<LogRecord> records = new ArrayList<>();
    try {
      for (Path file : DailyJsonl.filesWithin(dir, oldest)) {
        for (String line : Files.readAllLines(file)) {
          if (line.isBlank()) {
            continue;
          }
          try {
            records.add(StorageJson.MAPPER.readValue(line, LogRecord.class));
          } catch (JacksonException e) {
            log.warn("Skipping unparseable line in {}: {}", file, e.getMessage());
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read duration history from " + dir, e);
    }
    return records;
  }

  public void prune(Duration retention, Instant now) {
    LocalDate oldestKept = now.minus(retention).atZone(ZoneOffset.UTC).toLocalDate();
    try {
      DailyJsonl.pruneOlderThan(dir, oldestKept);
    } catch (IOException e) {
      log.warn("History pruning failed: {}", e.toString());
    }
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}
