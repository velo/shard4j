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
 * The append-only completion log that IS the session state: replayed on boot, so a restart
 * mid-session costs a bounded outage absorbed by the client retry budget, not a red run.
 *
 * <p>Appends here fail loud -- this file is load-bearing for restart recovery, unlike the
 * duration history whose loss merely degrades balancing.
 */
@Slf4j
public final class SessionLog implements AutoCloseable {

  private final Path dir;
  private final DailyJsonl out;

  public SessionLog(Path dir) {
    this.dir = dir;
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

  /**
   * Reads the window that can still hold a live session, oldest first. A malformed line is
   * the crash-truncated tail the fsync-per-append design explicitly tolerates; it is
   * skipped with a warning, never fatal to boot.
   */
  public List<LogRecord> replay(Duration gcIdle, Instant now) {
    LocalDate oldest = now.minus(gcIdle).atZone(ZoneOffset.UTC).toLocalDate();
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
      throw new UncheckedIOException("Cannot replay session log from " + dir, e);
    }
    return records;
  }

  public void prune(Duration gcIdle, Instant now) {
    LocalDate oldestKept = now.minus(gcIdle).atZone(ZoneOffset.UTC).toLocalDate();
    try {
      DailyJsonl.pruneOlderThan(dir, oldestKept);
    } catch (IOException e) {
      log.warn("Session log pruning failed: {}", e.toString());
    }
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}
