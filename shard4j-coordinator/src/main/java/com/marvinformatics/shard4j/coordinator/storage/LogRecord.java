package com.marvinformatics.shard4j.coordinator.storage;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.marvinformatics.shard4j.protocol.Outcome;
import com.marvinformatics.shard4j.protocol.Pass;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.Builder;

/**
 * One JSONL line. A single flat shape with nullable fields rather than a class hierarchy,
 * because the files are meant to be greppable by a human and parseable by three lines of
 * anything; {@code type} says which fields are present.
 *
 * <p>{@code unit} distinguishes a lease-unit completion (the only kind that feeds the
 * ordering aggregate) from a per-invocation record kept for humans diagnosing a slow or
 * flaky row inside a parameterized method.
 */
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public record LogRecord(
    Type type,
    String project,
    String session,
    Integer attempt,
    Long epoch,
    Map<String, String> metadata,
    String testSetHash,
    List<String> tests,
    String testId,
    Boolean unit,
    Integer shard,
    Pass pass,
    Outcome outcome,
    Long durationMs,
    Boolean firstOnShard,
    String reason,
    Instant ts) {

  public enum Type {
    REGISTERED,
    COMPLETION,
    NACK
  }

  public static LogRecord registered(
      String project,
      String session,
      int attempt,
      long epoch,
      Map<String, String> metadata,
      String testSetHash,
      List<String> tests,
      Instant ts) {
    return LogRecord.builder()
        .type(Type.REGISTERED)
        .project(project)
        .session(session)
        .attempt(attempt)
        .epoch(epoch)
        .metadata(metadata)
        .testSetHash(testSetHash)
        .tests(tests)
        .ts(ts)
        .build();
  }

  public static LogRecord unitCompletion(
      String project,
      String session,
      long epoch,
      String testId,
      int shard,
      Pass pass,
      Outcome outcome,
      long durationMs,
      boolean firstOnShard,
      String reason,
      Instant ts) {
    return LogRecord.builder()
        .type(Type.COMPLETION)
        .project(project)
        .session(session)
        .epoch(epoch)
        .testId(testId)
        .unit(true)
        .shard(shard)
        .pass(pass)
        .outcome(outcome)
        .durationMs(durationMs)
        .firstOnShard(firstOnShard)
        .reason(reason)
        .ts(ts)
        .build();
  }

  public static LogRecord invocationCompletion(
      String project,
      String session,
      long epoch,
      String testId,
      int shard,
      Pass pass,
      Outcome outcome,
      long durationMs,
      String reason,
      Instant ts) {
    return LogRecord.builder()
        .type(Type.COMPLETION)
        .project(project)
        .session(session)
        .epoch(epoch)
        .testId(testId)
        .unit(false)
        .shard(shard)
        .pass(pass)
        .outcome(outcome)
        .durationMs(durationMs)
        .reason(reason)
        .ts(ts)
        .build();
  }

  public static LogRecord nack(
      String project, String session, int shard, String testId, String reason, Instant ts) {
    return LogRecord.builder()
        .type(Type.NACK)
        .project(project)
        .session(session)
        .shard(shard)
        .testId(testId)
        .reason(reason)
        .ts(ts)
        .build();
  }
}
