package com.marvinformatics.shard4j.coordinator.storage;

import com.marvinformatics.shard4j.protocol.CensusUnit;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;

/**
 * The in-memory duration aggregate behind slowest-first ordering, and the per-invocation
 * breakdown behind invocation distribution.
 *
 * <p>Estimate = median of a unit's PASSED durations over its last five distinct sessions.
 * Sessions, not records, so retries in one run cannot degenerate the window to "this run
 * only". First-on-shard rows are excluded because per-JVM setup lands entirely on whichever
 * unit runs first -- unless a unit has no other rows, in which case they are used rather
 * than leaving the deterministically-first slowest test permanently unknown. Values above
 * the clamp are discarded so one runaway or injected figure cannot dominate ordering.
 *
 * <p>Keys stay at method level even for invocation data: an entry's {@code invocations}
 * map (position to duration) is a value inside the method-keyed record, never a key of
 * its own -- positions are unstable across commits, so they may steer distribution but
 * must never become an address. A breakdown is trusted as a distribution plan only when
 * its session is marked complete, meaning every invocation the method had was seen to
 * finish non-failing; anything less could silently drop a real row from the hand-out.
 *
 * <p>"Is this duration known?" is the absence of the key -- no flag, no column. A seed
 * record is indistinguishable from a measured one here, which is what lets real
 * measurements displace a seed with no special-casing.
 */
@Slf4j
@RequiredArgsConstructor
public final class DurationStore {

  static final int SESSION_WINDOW = 5;

  /**
   * {@code fromInvocations} says the entry was accreted from individually-reported
   * invocation results, so its {@code durationMs} is the running sum of them rather than a
   * measured whole-method time; {@code invocationsComplete} says the breakdown enumerates
   * every invocation the method had in that session.
   */
  public record Entry(
      String session,
      long durationMs,
      boolean firstOnShard,
      boolean fromInvocations,
      boolean invocationsComplete,
      Map<Integer, Long> invocations) {

    Map<Integer, Long> invocationsOrEmpty() {
      return invocations == null ? Map.of() : invocations;
    }
  }

  private final Path snapshotFile;
  private final long clampMs;
  private final Map<String, List<Entry>> byKey = new HashMap<>();
  private final Set<String> lowConfidenceFlagged = new HashSet<>();
  private boolean dirty;

  public synchronized void recordPassed(
      HistoryKey key, String session, long durationMs, boolean firstOnShard) {
    if (outOfRange(key, durationMs)) {
      return;
    }
    List<Entry> entries = byKey.computeIfAbsent(key.value(), any -> new ArrayList<>());
    boolean sessionAlreadyPresent =
        entries.stream().anyMatch(entry -> entry.session().equals(session));
    if (sessionAlreadyPresent) {
      return;
    }
    entries.add(new Entry(session, durationMs, firstOnShard, false, false, Map.of()));
    trimWindow(entries);
    dirty = true;
  }

  /**
   * One invocation's contribution to the method-keyed entry: attaches the position, and
   * only when the entry itself was accreted from invocations does it also advance the
   * running duration sum -- a whole-method measurement is never overwritten by parts.
   */
  public synchronized void recordInvocation(
      HistoryKey key, String session, int position, long durationMs) {
    if (outOfRange(key, durationMs)) {
      return;
    }
    List<Entry> entries = byKey.computeIfAbsent(key.value(), any -> new ArrayList<>());
    int index = indexOfSession(entries, session);
    if (index < 0) {
      entries.add(
          new Entry(session, durationMs, false, true, false, Map.of(position, durationMs)));
      trimWindow(entries);
    } else {
      Entry entry = entries.get(index);
      Map<Integer, Long> merged = new LinkedHashMap<>(entry.invocationsOrEmpty());
      merged.put(position, durationMs);
      long total =
          entry.fromInvocations()
              ? merged.values().stream().mapToLong(Long::longValue).sum()
              : entry.durationMs();
      entries.set(
          index,
          new Entry(
              session,
              total,
              entry.firstOnShard(),
              entry.fromInvocations(),
              entry.invocationsComplete(),
              merged));
    }
    dirty = true;
  }

  /**
   * Marks the session's breakdown as a full enumeration of the method's invocations --
   * the precondition for ever using it as a distribution plan.
   */
  public synchronized void markInvocationsComplete(HistoryKey key, String session) {
    List<Entry> entries = byKey.get(key.value());
    if (entries == null) {
      return;
    }
    int index = indexOfSession(entries, session);
    if (index < 0 || entries.get(index).invocationsOrEmpty().isEmpty()) {
      return;
    }
    Entry entry = entries.get(index);
    entries.set(
        index,
        new Entry(
            entry.session(),
            entry.durationMs(),
            entry.firstOnShard(),
            entry.fromInvocations(),
            true,
            entry.invocations()));
    dirty = true;
  }

  /**
   * The distribution plan: the newest complete breakdown's positions in numeric order, or
   * empty when no complete breakdown exists -- in which case the method leases whole,
   * exactly as a method never seen before does. Only the newest complete entry is
   * consulted: an older one is staler truth, not a fallback.
   */
  public synchronized List<Integer> invocationPlan(HistoryKey key) {
    List<Entry> entries = byKey.get(key.value());
    if (entries == null) {
      return List.of();
    }
    for (int i = entries.size() - 1; i >= 0; i--) {
      Entry entry = entries.get(i);
      if (entry.invocationsComplete()) {
        return entry.invocationsOrEmpty().keySet().stream().sorted().toList();
      }
    }
    return List.of();
  }

  /** Median of the position's recorded durations across the window, when any exist. */
  public synchronized OptionalLong invocationEstimate(HistoryKey key, int position) {
    List<Entry> entries = byKey.get(key.value());
    if (entries == null) {
      return OptionalLong.empty();
    }
    List<Long> rows =
        entries.stream()
            .map(entry -> entry.invocationsOrEmpty().get(position))
            .filter(duration -> duration != null)
            .toList();
    return rows.isEmpty() ? OptionalLong.empty() : OptionalLong.of(medianOf(rows));
  }

  /**
   * A shard proved the position no longer exists (a vanished non-probe invocation), so it
   * is dropped from every window entry: the next session must not hand it out again.
   */
  public synchronized void dropInvocation(HistoryKey key, int position) {
    List<Entry> entries = byKey.get(key.value());
    if (entries == null) {
      return;
    }
    for (int i = 0; i < entries.size(); i++) {
      Entry entry = entries.get(i);
      if (!entry.invocationsOrEmpty().containsKey(position)) {
        continue;
      }
      Map<Integer, Long> remaining = new LinkedHashMap<>(entry.invocations());
      remaining.remove(position);
      long total =
          entry.fromInvocations()
              ? remaining.values().stream().mapToLong(Long::longValue).sum()
              : entry.durationMs();
      entries.set(
          i,
          new Entry(
              entry.session(),
              total,
              entry.firstOnShard(),
              entry.fromInvocations(),
              entry.invocationsComplete(),
              remaining));
      dirty = true;
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
      // First-on-shard rows carry the per-JVM setup cost, so an estimate resting only on
      // them is usable but low-confidence; say so once per key instead of silently.
      if (lowConfidenceFlagged.add(key.value())) {
        log.warn(
            "Estimate for {} rests only on first-on-shard rows; low confidence until a"
                + " non-first measurement lands",
            key.value());
      }
      rows = entries.stream().map(Entry::durationMs).toList();
    }
    return OptionalLong.of(medianOf(rows));
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
      dirty = false;
    } catch (IOException | JacksonException e) {
      log.warn("Duration snapshot write failed: {}", e.toString());
    }
  }

  /**
   * The debounced entry point for the maintenance scheduler: a snapshot per PASSED result
   * was a whole-store rewrite plus two fsyncs inside the write lock, for a file that is
   * only a boot accelerator.
   */
  public synchronized void saveIfDirty() {
    if (dirty) {
      saveSnapshot();
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

  /**
   * Rebuilds the store from raw history lines. Whole-unit rows feed the method aggregate;
   * invocation-suffixed rows -- individually-leased invocations and the per-row records a
   * whole template reports -- feed the breakdown. A template whose aggregate PASSED
   * enumerated every row it materialised, so its breakdown is marked complete; a
   * distributed session's completeness was a live judgement over the whole session and is
   * not reconstructable from rows alone, so those sessions rebuild as duration data only
   * and the first run after such a rebuild leases the method whole once.
   */
  public synchronized void coldLoad(List<LogRecord> historyRecords) {
    Set<CompleteMark> completeMarks = new LinkedHashSet<>();
    for (LogRecord record : historyRecords) {
      if (record.type() != LogRecord.Type.COMPLETION) {
        continue;
      }
      CensusUnit unit;
      try {
        unit = CensusUnit.parse(record.testId());
      } catch (IllegalArgumentException e) {
        log.warn("Skipping history record with unusable id: {}", e.getMessage());
        continue;
      }
      boolean wholeUnit = Boolean.TRUE.equals(record.unit()) && unit.invocation() == null;
      if (wholeUnit && record.outcome() == Outcome.PASSED) {
        recordPassed(
            unit.historyKey(),
            record.session(),
            record.durationMs(),
            Boolean.TRUE.equals(record.firstOnShard()));
        if (unit.template()) {
          completeMarks.add(new CompleteMark(unit.historyKey().value(), record.session()));
        }
      } else if (unit.invocation() != null && record.outcome() == Outcome.PASSED) {
        recordInvocation(unit.historyKey(), record.session(), unit.invocation(), record.durationMs());
      } else if (unit.invocation() != null && record.outcome() == Outcome.SKIPPED) {
        recordInvocation(unit.historyKey(), record.session(), unit.invocation(), 0);
      }
    }
    for (CompleteMark mark : completeMarks) {
      markInvocationsComplete(new HistoryKey(mark.key()), mark.session());
    }
  }

  private record CompleteMark(String key, String session) {}

  private boolean outOfRange(HistoryKey key, long durationMs) {
    if (durationMs < 0 || durationMs > clampMs) {
      log.warn(
          "Discarding out-of-range duration {} ms for {}; the clamp protects ordering",
          durationMs,
          key.value());
      return true;
    }
    return false;
  }

  private static void trimWindow(List<Entry> entries) {
    while (entries.size() > SESSION_WINDOW) {
      entries.remove(0);
    }
  }

  private static int indexOfSession(List<Entry> entries, String session) {
    for (int i = 0; i < entries.size(); i++) {
      if (entries.get(i).session().equals(session)) {
        return i;
      }
    }
    return -1;
  }

  private static long medianOf(List<Long> rows) {
    List<Long> sorted = new ArrayList<>(rows);
    sorted.sort(Long::compareTo);
    int size = sorted.size();
    return size % 2 == 1
        ? sorted.get(size / 2)
        : (sorted.get(size / 2 - 1) + sorted.get(size / 2)) / 2;
  }
}
