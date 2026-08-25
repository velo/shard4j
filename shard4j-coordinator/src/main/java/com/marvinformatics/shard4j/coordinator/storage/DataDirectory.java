package com.marvinformatics.shard4j.coordinator.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

/**
 * The one writable directory. Exactly one process may own it: single-writer is a
 * correctness invariant, not a deployment nicety, so the lock is taken at startup and held
 * for the process lifetime, and a second process refuses to start naming the holder.
 *
 * <p>The incarnation counter is bumped and fsynced here, before the first request is ever
 * served. Without it a restart would reset the fence sequence and a pre-restart zombie's
 * write would compare valid; it also survives a wall clock stepping backwards.
 */
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DataDirectory implements AutoCloseable {

  private final FileChannel lockChannel;
  private final FileLock lock;
  @Getter private final Path tenantDir;
  @Getter private final long incarnation;

  public static DataDirectory open(Path root, String tenantSlug) {
    try {
      Files.createDirectories(root);
      Path lockPath = root.resolve(".lock");
      FileChannel channel =
          FileChannel.open(
              lockPath,
              StandardOpenOption.CREATE,
              StandardOpenOption.READ,
              StandardOpenOption.WRITE);
      FileLock lock = channel.tryLock();
      if (lock == null) {
        String holder = readHolder(channel);
        channel.close();
        throw new IllegalStateException(
            "Refusing to start: "
                + lockPath
                + " is already held"
                + (holder.isBlank() ? "" : " by " + holder)
                + "; exactly one process may own a data directory.");
      }
      writeHolder(channel);
      long incarnation = bumpIncarnation(root);
      Path tenantDir = root.resolve(tenantSlug);
      Files.createDirectories(tenantDir.resolve("sessions"));
      Files.createDirectories(tenantDir.resolve("history"));
      return new DataDirectory(channel, lock, tenantDir, incarnation);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot open data directory " + root, e);
    }
  }

  public Path sessionsDir() {
    return tenantDir.resolve("sessions");
  }

  public Path historyDir() {
    return tenantDir.resolve("history");
  }

  public Path snapshotFile() {
    return tenantDir.resolve("current.json");
  }

  public boolean lockHeld() {
    return lock.isValid();
  }

  private static String readHolder(FileChannel channel) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(128);
    channel.read(buffer, 0);
    buffer.flip();
    return StandardCharsets.UTF_8.decode(buffer).toString().trim();
  }

  private static void writeHolder(FileChannel channel) throws IOException {
    channel.truncate(0);
    channel.write(
        ByteBuffer.wrap(("pid " + ProcessHandle.current().pid()).getBytes(StandardCharsets.UTF_8)),
        0);
    channel.force(true);
  }

  /**
   * Written temp-then-rename so the file can never be observed truncated: a corrupt
   * incarnation would let an old process's fences compare valid, so a parse failure here is
   * a refused start, not a silent reset to zero.
   */
  private static long bumpIncarnation(Path root) throws IOException {
    Path file = root.resolve("incarnation");
    long previous = 0;
    if (Files.exists(file)) {
      String content = Files.readString(file).trim();
      try {
        previous = Long.parseLong(content);
      } catch (NumberFormatException e) {
        throw new IllegalStateException(
            "Refusing to start: " + file + " does not hold an integer; repair or remove it.", e);
      }
    }
    long next = previous + 1;
    Path temp = root.resolve("incarnation.tmp");
    try (FileChannel channel =
        FileChannel.open(
            temp,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      channel.write(ByteBuffer.wrap(Long.toString(next).getBytes(StandardCharsets.UTF_8)));
      channel.force(true);
    }
    Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    // The rename is only durable once the directory entry itself is fsynced; without this a
    // power loss can resurrect the previous incarnation and let an old zombie's fences
    // compare valid.
    try (FileChannel dirChannel = FileChannel.open(root, StandardOpenOption.READ)) {
      dirChannel.force(true);
    }
    return next;
  }

  @Override
  public void close() {
    try {
      lock.release();
      lockChannel.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
