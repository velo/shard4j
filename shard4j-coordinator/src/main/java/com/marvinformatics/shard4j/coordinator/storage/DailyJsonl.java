package com.marvinformatics.shard4j.coordinator.storage;

import tools.jackson.core.JacksonException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Append-only day files, fsynced per append. A crash mid-append truncates only the final
 * line and the reader skips it; at well under one write per second the fsync costs nothing
 * measurable and turns "losing data is acceptable" into a clause that never fires.
 */
@Slf4j
@RequiredArgsConstructor
final class DailyJsonl implements AutoCloseable {

  private final Path dir;
  private FileChannel channel;
  private LocalDate openDate;

  synchronized void append(byte[] jsonLine) throws IOException {
    LocalDate today = LocalDate.now(ZoneOffset.UTC);
    if (channel == null || !today.equals(openDate)) {
      close();
      Path file = dir.resolve(today + ".jsonl");
      sealTornTail(file);
      channel =
          FileChannel.open(
              file,
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.APPEND);
      openDate = today;
    }
    ByteBuffer buffer = ByteBuffer.allocate(jsonLine.length + 1);
    buffer.put(jsonLine).put((byte) '\n').flip();
    channel.write(buffer);
    channel.force(false);
  }

  /**
   * A crash mid-append leaves a partial line with no trailing newline. Reopened in APPEND
   * mode, the next record would be glued onto that fragment and a later replay would drop
   * both as one unparseable line -- so the fragment is sealed with a newline before any new
   * record is written after it.
   */
  private static void sealTornTail(Path file) throws IOException {
    if (!Files.exists(file)) {
      return;
    }
    try (FileChannel tail =
        FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      long size = tail.size();
      if (size == 0) {
        return;
      }
      ByteBuffer lastByte = ByteBuffer.allocate(1);
      tail.read(lastByte, size - 1);
      if (lastByte.get(0) != '\n') {
        tail.write(ByteBuffer.wrap(new byte[] {'\n'}), size);
        tail.force(false);
      }
    }
  }

  /**
   * Records from the day files inside the window, oldest file first. A malformed line is
   * the crash-truncated tail the fsync-per-append design explicitly tolerates; it is
   * skipped with a warning, never fatal.
   */
  List<LogRecord> readWithin(Duration window, Instant now) {
    LocalDate oldest = now.minus(window).atZone(ZoneOffset.UTC).toLocalDate();
    List<LogRecord> records = new ArrayList<>();
    try {
      for (Path file : filesWithin(dir, oldest)) {
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
      throw new UncheckedIOException("Cannot read day files from " + dir, e);
    }
    return records;
  }

  /** Deletion is unlink of whole day files older than the window -- no crash window. */
  void prune(Duration window, Instant now) {
    LocalDate oldestKept = now.minus(window).atZone(ZoneOffset.UTC).toLocalDate();
    try {
      pruneOlderThan(dir, oldestKept);
    } catch (IOException e) {
      log.warn("Pruning day files in {} failed: {}", dir, e.toString());
    }
  }

  /** Day files whose date falls inside the window, oldest first. */
  private static List<Path> filesWithin(Path dir, LocalDate oldestInclusive) throws IOException {
    List<Path> files = new ArrayList<>();
    if (!Files.isDirectory(dir)) {
      return files;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.jsonl")) {
      for (Path file : stream) {
        LocalDate date = dateOf(file);
        if (date != null && !date.isBefore(oldestInclusive)) {
          files.add(file);
        }
      }
    }
    files.sort(Path::compareTo);
    return files;
  }

  private static void pruneOlderThan(Path dir, LocalDate oldestKept) throws IOException {
    if (!Files.isDirectory(dir)) {
      return;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.jsonl")) {
      for (Path file : stream) {
        LocalDate date = dateOf(file);
        if (date != null && date.isBefore(oldestKept)) {
          Files.delete(file);
        }
      }
    }
  }

  private static LocalDate dateOf(Path file) {
    String name = file.getFileName().toString();
    try {
      return LocalDate.parse(name.substring(0, name.length() - ".jsonl".length()));
    } catch (DateTimeParseException | StringIndexOutOfBoundsException e) {
      return null;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (channel != null) {
      channel.close();
      channel = null;
    }
  }
}
