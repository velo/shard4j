package com.marvinformatics.shard4j.coordinator.storage;

import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import com.marvinformatics.shard4j.coordinator.core.HistoryKeys;
import com.marvinformatics.shard4j.protocol.HistoryKey;
import com.marvinformatics.shard4j.protocol.Outcome;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The in-memory duration aggregate behind slowest-first ordering.
 *
 * <p>Estimate = median of a unit's PASSED durations over its last five distinct sessions.
 * Sessions, not records, so retries in one run cannot degenerate the window to "this run
 * only". First-on-shard rows are excluded because per-JVM setup lands entirely on whichever
 * unit runs first -- unless a unit has no other rows, in which case they are used rather
 * than leaving the deterministically-first slowest test permanently unknown. Values above
 * the clamp are discarded so one runaway or injected figure cannot dominate ordering.
 *
 * <p>"Is this duration known?" is the absence of the key -- no flag, no column. A seed
 * record is indistinguishable from a measured one here, which is what lets real
 * measurements displace a seed with no special-casing.
 */
public final class DurationStore {

  private static final Logger log = LoggerFactory.getLogger(DurationStore.class);

  static final int SESSION_WINDOW = 5;

  public record Entry(String session, long durationMs, boolean firstOnShard) {}

  private final Path snapshotFile;
  private final long clampMs;
  private final Map<String, List<Entry>> byKey = new HashMap<>();

  public DurationStore(Path snapshotFile, long clampMs) {
    this.snapshotFile = snapshotFile;
    this.clampMs = clampMs;
  }

  public synchronized void recordPassed(
      HistoryKey key, String session, long durationMs, boolean firstOnShard) {
    if (durationMs < 0 || durationMs > clampMs) {
      log.warn(
          "Discarding out-of-range duration {} ms for {}; the clamp protects ordering",
          durationMs,
          key.value());
      return;
    }
    List<Entry> entries = byKey.computeIfAbsent(key.value(), any -> new ArrayList<>());
    boolean sessionAlreadyPresent =
        entries.stream().anyMatch(entry -> entry.session().equals(session));
    if (sessionAlreadyPresent) {
      return;
    }
    entries.add(new Entry(session, durationMs, firstOnShard));
    while (entries.size() > SESSION_WINDOW) {
      entries.remove(0);
    }
  }

  public synchronized OptionalLong estimate(HistoryKey key) {
    List<Entry> entries = byKey.get(key.value());
    if (entries == null || entries.isEmpty()) {
      return OptionalLong.empty();
    }
    List<Long> rows =
        entries.stream().filter(entry -> !entry.firstOnShard()).map(Entry::durationMs).toList();
    if (rows.isEmpty()) {
      rows = entries.stream().map(Entry::durationMs).toList();
    }
    List<Long> sorted = new ArrayList<>(rows);
    sorted.sort(Long::compareTo);
    int size = sorted.size();
    long median =
        size % 2 == 1
            ? sorted.get(size / 2)
            : (sorted.get(size / 2 - 1) + sorted.get(size / 2)) / 2;
    return OptionalLong.of(median);
  }

  public synchronized boolean isEmpty() {
    return byKey.isEmpty();
  }

  /**
   * Atomic temp-then-rename: rewriting the snapshot is the one whole-store write, and the
   * rename is what makes a crash mid-write leave the previous snapshot intact. Failure is
   * soft -- the snapshot is a boot accelerator, never load-bearing state.
   */
  public synchronized void saveSnapshot() {
    Path temp = snapshotFile.resolveSibling(snapshotFile.getFileName() + ".tmp");
    try {
      try (FileChannel channel =
          FileChannel.open(
              temp,
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.TRUNCATE_EXISTING)) {
        channel.write(ByteBuffer.wrap(StorageJson.MAPPER.writeValueAsBytes(byKey)));
        channel.force(true);
      }
      Files.move(
          temp, snapshotFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException | JacksonException e) {
      log.warn("Duration snapshot write failed: {}", e.toString());
    }
  }

  /**
   * A missing or unparseable snapshot degrades balancing and must never delay readiness:
   * loud ERROR naming the file, empty map, and the service starts anyway.
   */
  public synchronized boolean loadSnapshot() {
    if (!Files.exists(snapshotFile)) {
      return false;
    }
    try {
      Map<String, List<Entry>> loaded =
          StorageJson.MAPPER.readValue(
              Files.readAllBytes(snapshotFile), new TypeReference<Map<String, List<Entry>>>() {});
      byKey.clear();
      byKey.putAll(loaded);
      return true;
    } catch (IOException | JacksonException e) {
      log.error(
          "Duration snapshot {} is unparseable; starting with degraded balancing: {}",
          snapshotFile,
          e.toString());
      byKey.clear();
      return true;
    }
  }

  public synchronized void coldLoad(List<LogRecord> historyRecords) {
    for (LogRecord record : historyRecords) {
      if (record.type() == LogRecord.Type.COMPLETION
          && Boolean.TRUE.equals(record.unit())
          && record.outcome() == Outcome.PASSED) {
        try {
          recordPassed(
              HistoryKeys.of(record.testId()),
              record.session(),
              record.durationMs(),
              Boolean.TRUE.equals(record.firstOnShard()));
        } catch (IllegalArgumentException e) {
          log.warn("Skipping history record with unusable id: {}", e.getMessage());
        }
      }
    }
  }
}
