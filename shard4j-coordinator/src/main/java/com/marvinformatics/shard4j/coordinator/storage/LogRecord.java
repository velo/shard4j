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
    JOINED,
    COMPLETION,
    NACK,
    PASS_COMPLETE,
    DEPARTED,
    RELEASED
  }

  public static LogRecord registered(
      String project,
      String session,
      int attempt,
      long epoch,
      Map<String, String> metadata,
      List<String> tests,
      Instant ts) {
    return LogRecord.builder()
        .type(Type.REGISTERED)
        .project(project)
        .session(session)
        .attempt(attempt)
        .epoch(epoch)
        .metadata(metadata)
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

  /** Barrier arrival: the shard's report that it finished {@code pass}. */
  public static LogRecord passComplete(
      String project, String session, long epoch, int shard, Pass pass, Instant ts) {
    return LogRecord.builder()
        .type(Type.PASS_COMPLETE)
        .project(project)
        .session(session)
        .epoch(epoch)
        .shard(shard)
        .pass(pass)
        .ts(ts)
        .build();
  }

  /**
   * A shard entering, or re-entering, the roster. Without it a shard that registered but
   * produced no completion or pass record would vanish from the replayed roster, and every
   * quorum would resolve without it.
   */
  public static LogRecord joined(
      String project, String session, long epoch, int shard, Instant ts) {
    return LogRecord.builder()
        .type(Type.JOINED)
        .project(project)
        .session(session)
        .epoch(epoch)
        .shard(shard)
        .ts(ts)
        .build();
  }

  public static LogRecord departed(String project, String session, int shard, Instant ts) {
    return LogRecord.builder()
        .type(Type.DEPARTED)
        .project(project)
        .session(session)
        .shard(shard)
        .ts(ts)
        .build();
  }

  /** Early release: the coordinator decided this shard is not needed for retry work. */
  public static LogRecord released(
      String project, String session, long epoch, int shard, Instant ts) {
    return LogRecord.builder()
        .type(Type.RELEASED)
        .project(project)
        .session(session)
        .epoch(epoch)
        .shard(shard)
        .ts(ts)
        .build();
  }
}
