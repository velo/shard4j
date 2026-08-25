package com.marvinformatics.shard4j.coordinator.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionLogTest {

  @TempDir Path dir;

  @Test
  void aCrashTruncatedFinalLineIsSkippedNotFatal() throws IOException {
    // The day file is named from the real UTC clock inside the log, so the test must use
    // the same clock or it breaks at every UTC midnight.
    Instant now = Instant.now();
    try (SessionLog log = new SessionLog(dir)) {
      log.append(
          LogRecord.unitCompletion(
              "example/orders-service",
              "7f3a",
              1,
              "[engine:junit-jupiter]/[class:com.example.A]/[method:a()]",
              0,
              Pass.MAIN,
              Outcome.PASSED,
              1000,
              false,
              null,
              now));
    }
    Path file = onlyJsonlFile();
    Files.writeString(file, "{\"type\":\"COMPLETION\",\"session\":\"7f", StandardOpenOption.APPEND);

    List<LogRecord> replayed = new SessionLog(dir).replay(Duration.ofDays(7), now);
    assertThat(replayed).hasSize(1);
    assertThat(replayed.get(0).outcome()).isEqualTo(Outcome.PASSED);
  }

  @Test
  void replayReadsOnlyTheIdleWindow() throws IOException {
    Instant now = Instant.parse("2026-08-24T12:00:00Z");
    String line =
        "{\"type\":\"NACK\",\"session\":\"old\",\"shard\":0,\"testId\":\"x\",\"reason\":\"r\","
            + "\"ts\":\"2026-08-01T00:00:00Z\"}";
    Files.writeString(dir.resolve("2026-08-01.jsonl"), line + "\n");

    assertThat(new SessionLog(dir).replay(Duration.ofDays(7), now)).isEmpty();
    assertThat(new SessionLog(dir).replay(Duration.ofDays(30), now)).hasSize(1);
  }

  private Path onlyJsonlFile() throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      List<Path> jsonl = files.filter(f -> f.getFileName().toString().endsWith(".jsonl")).toList();
      assertThat(jsonl).hasSize(1);
      return jsonl.get(0);
    }
  }
}
