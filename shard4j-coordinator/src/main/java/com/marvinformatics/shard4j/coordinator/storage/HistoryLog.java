package com.marvinformatics.shard4j.coordinator.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
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

  private final DailyJsonl out;

  public HistoryLog(Path dir) {
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
    return out.readWithin(retention, now);
  }

  public void prune(Duration retention, Instant now) {
    out.prune(retention, now);
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}
