package com.marvinformatics.shard4j.coordinator.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * The append-only completion log that IS the session state: replayed on boot, so a restart
 * mid-session costs a bounded outage absorbed by the client retry budget, not a red run.
 *
 * <p>Appends here fail loud -- this file is load-bearing for restart recovery, unlike the
 * duration history whose loss merely degrades balancing.
 */
@Slf4j
public final class SessionLog implements AutoCloseable {

  private final DailyJsonl out;

  public SessionLog(Path dir) {
    this.out = new DailyJsonl(dir);
  }

  public void append(LogRecord record) {
    try {
      out.append(StorageJson.MAPPER.writeValueAsBytes(record));
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Session-state append failed; this log is load-bearing for restart recovery", e);
    }
  }

  /** For diagnostic records whose loss a restart can tolerate. */
  public void appendQuietly(LogRecord record) {
    try {
      out.append(StorageJson.MAPPER.writeValueAsBytes(record));
    } catch (IOException e) {
      log.warn("Diagnostic session record dropped: {}", e.toString());
    }
  }

  /** Reads the window that can still hold a live session, oldest first. */
  public List<LogRecord> replay(Duration gcIdle, Instant now) {
    return out.readWithin(gcIdle, now);
  }

  public void prune(Duration gcIdle, Instant now) {
    out.prune(gcIdle, now);
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}
