package com.marvinformatics.shard4j.coordinator.stats;

import com.marvinformatics.shard4j.protocol.CensusUnit;
import com.marvinformatics.shard4j.protocol.Failure;
import com.marvinformatics.shard4j.protocol.InvocationRecord;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.sqlite.SQLiteConfig;

/**
 * Every attempt of every unit, passes included, plus each distinct failure stored once.
 *
 * <p>Like the duration history, this is never load-bearing: a write that fails is a
 * warning, and a database that cannot be opened at all -- a platform the bundled native
 * library does not cover, a full or read-only volume -- leaves the coordinator scheduling
 * with the statistics endpoints answering 503. Losing flake statistics must never cost a
 * test run.
 *
 * <p>Two connections, because WAL lets a reader proceed beside the writer: a month-wide
 * aggregation must not hold up the result path behind it.
 */
@Slf4j
public final class StatsStore implements AutoCloseable {

  static final int STACK_LIMIT = 64 * 1024;
  static final int MESSAGE_LIMIT = 4 * 1024;
  private static final int CLASS_LIMIT = 512;
  private static final List<String> MIGRATIONS = List.of("stats/001-attempts.sql");

  private final Connection writer;
  private final Connection reader;
  private final String unavailableReason;

  private StatsStore(Connection writer, Connection reader, String unavailableReason) {
    this.writer = writer;
    this.reader = reader;
    this.unavailableReason = unavailableReason;
  }

  /** Opens or creates the database; any failure yields a store that records nothing. */
  public static StatsStore open(Path file) {
    try {
      SQLiteConfig config = new SQLiteConfig();
      config.setJournalMode(SQLiteConfig.JournalMode.WAL);
      config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);
      config.enforceForeignKeys(true);
      config.setBusyTimeout(5_000);
      Connection writer = config.createConnection("jdbc:sqlite:" + file);
      migrate(writer, file);
      Connection reader = config.createConnection("jdbc:sqlite:" + file);
      return new StatsStore(writer, reader, null);
    } catch (SQLException | RuntimeException | LinkageError e) {
      log.error(
          "Failure statistics disabled: cannot open {}; scheduling is unaffected: {}",
          file,
          e.toString());
      return new StatsStore(null, null, e.toString());
    }
  }

  public boolean available() {
    return unavailableReason == null;
  }

  private static void migrate(Connection connection, Path file) throws SQLException {
    int version;
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("PRAGMA user_version")) {
      version = rows.getInt(1);
    }
    if (version > MIGRATIONS.size()) {
      throw new IllegalStateException(
          file + " is at schema version " + version + ", written by a newer coordinator");
    }
    for (int next = version; next < MIGRATIONS.size(); next++) {
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        for (String sql : resource(MIGRATIONS.get(next)).split(";\\s*\n")) {
          if (!sql.isBlank()) {
            statement.executeUpdate(sql);
          }
        }
        statement.executeUpdate("PRAGMA user_version = " + (next + 1));
        connection.commit();
      } catch (SQLException | RuntimeException e) {
        connection.rollback();
        throw e;
      } finally {
        connection.setAutoCommit(true);
      }
    }
  }

  private static String resource(String name) {
    try (InputStream in = StatsStore.class.getClassLoader().getResourceAsStream(name)) {
      if (in == null) {
        throw new IllegalStateException("Missing schema resource " + name);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** One accepted result: the unit's attempt and, for a template, each of its rows. */
  public void record(String session, ResultRequest request, int attempt, Instant at) {
    if (!available()) {
      return;
    }
    synchronized (writer) {
      try {
        writer.setAutoCommit(false);
        String kind = request.invocations() == null ? "UNIT" : "TEMPLATE";
        insertAttempt(
            session,
            request.testId(),
            kind,
            request.shard(),
            attempt,
            request.outcome().name(),
            request.durationMs(),
            request.reason(),
            request.failure(),
            at);
        if (request.invocations() != null) {
          for (InvocationRecord row : request.invocations()) {
            insertAttempt(
                session,
                row.testId(),
                "INVOCATION",
                request.shard(),
                attempt,
                row.outcome().name(),
                row.durationMs(),
                row.reason(),
                row.failure(),
                at);
          }
        }
        writer.commit();
      } catch (SQLException | RuntimeException e) {
        rollbackQuietly();
        log.warn("Failure-statistics write dropped for {}: {}", request.testId(), e.toString());
      } finally {
        autoCommitQuietly();
      }
    }
  }

  private void insertAttempt(
      String session,
      String testId,
      String kind,
      int shard,
      int attempt,
      String outcome,
      long durationMs,
      String reason,
      Failure failure,
      Instant at)
      throws SQLException {
    Long signatureId = failure == null ? null : upsertSignature(failure, at);
    try (PreparedStatement insert =
        writer.prepareStatement(
            "INSERT INTO test_attempt (session, test_id, history_key, kind, shard, attempt,"
                + " outcome, duration_ms, reason, signature_id, ts)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
      insert.setString(1, session);
      insert.setString(2, testId);
      insert.setString(3, historyKeyOf(testId));
      insert.setString(4, kind);
      insert.setInt(5, shard);
      insert.setInt(6, attempt);
      insert.setString(7, outcome);
      insert.setLong(8, durationMs);
      insert.setString(9, reason);
      insert.setObject(10, signatureId);
      insert.setLong(11, at.toEpochMilli());
      insert.executeUpdate();
    }
  }

  private long upsertSignature(Failure failure, Instant at) throws SQLException {
    String exceptionClass =
        capped(failure.exceptionClass() == null ? "unknown" : failure.exceptionClass(), CLASS_LIMIT);
    String stackTrace = capped(failure.stackTrace() == null ? "" : failure.stackTrace(), STACK_LIMIT);
    try (PreparedStatement upsert =
        writer.prepareStatement(
            "INSERT INTO failure_signature (fingerprint, exception_class, message, stack_trace,"
                + " first_seen, last_seen) VALUES (?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT (fingerprint) DO UPDATE SET last_seen = excluded.last_seen,"
                + " message = excluded.message, stack_trace = excluded.stack_trace"
                + " RETURNING id")) {
      upsert.setString(1, FailureFingerprint.of(exceptionClass, stackTrace));
      upsert.setString(2, exceptionClass);
      upsert.setString(3, capped(failure.message(), MESSAGE_LIMIT));
      upsert.setString(4, stackTrace);
      upsert.setLong(5, at.toEpochMilli());
      upsert.setLong(6, at.toEpochMilli());
      try (ResultSet rows = upsert.executeQuery()) {
        rows.next();
        return rows.getLong(1);
      }
    }
  }

  /** Newest first. {@code test} matches either the history key or the exact unit id. */
  public List<FailedAttempt> failures(Instant from, Instant to, String test, int limit) {
    requireAvailable();
    String sql =
        "SELECT t.session, t.test_id, t.history_key, t.kind, t.shard, t.attempt, t.duration_ms,"
            + " t.reason, t.ts, s.fingerprint,"
            + " EXISTS (SELECT 1 FROM test_attempt later WHERE later.session = t.session"
            + " AND later.test_id = t.test_id AND later.attempt > t.attempt) AS retried"
            + " FROM test_attempt t LEFT JOIN failure_signature s ON s.id = t.signature_id"
            + " WHERE t.outcome = 'FAILED' AND t.kind <> 'TEMPLATE' AND t.ts >= ? AND t.ts < ?"
            + (test == null ? "" : " AND (t.history_key = ? OR t.test_id = ?)")
            + " ORDER BY t.ts DESC, t.id DESC LIMIT ?";
    List<Object> params = new ArrayList<>(List.of(from.toEpochMilli(), to.toEpochMilli()));
    if (test != null) {
      params.add(test);
      params.add(test);
    }
    params.add(limit);
    return query(
        sql,
        params,
        rows ->
            new FailedAttempt(
                rows.getString(1),
                rows.getString(2),
                rows.getString(3),
                rows.getString(4),
                rows.getInt(5),
                rows.getInt(6),
                rows.getLong(7),
                rows.getString(8),
                Instant.ofEpochMilli(rows.getLong(9)),
                rows.getBoolean(11),
                rows.getString(10)));
  }

  public Map<String, FailureSignature> signatures(Collection<String> fingerprints) {
    requireAvailable();
    Map<String, FailureSignature> found = new LinkedHashMap<>();
    if (fingerprints.isEmpty()) {
      return found;
    }
    List<Object> params = new ArrayList<>(fingerprints);
    List<Map.Entry<String, FailureSignature>> rows =
        query(
        "SELECT fingerprint, exception_class, message, stack_trace, first_seen, last_seen"
            + " FROM failure_signature WHERE fingerprint IN ("
            + placeholders(params.size())
            + ")",
        params,
        row ->
            Map.entry(
                row.getString(1),
                new FailureSignature(
                    row.getString(2),
                    row.getString(3),
                    row.getString(4),
                    Instant.ofEpochMilli(row.getLong(5)),
                    Instant.ofEpochMilli(row.getLong(6)))));
    rows.forEach(entry -> found.put(entry.getKey(), entry.getValue()));
    return found;
  }

  /** Tests with at least one failed attempt in the window, worst first. */
  public List<TestStability> unstable(
      Instant from, Instant to, StabilityRanking ranking, int limit) {
    requireAvailable();
    List<Aggregate> aggregates =
        aggregate(
            "ts >= ? AND ts < ?",
            List.of(from.toEpochMilli(), to.toEpochMilli()),
            "failed > 0",
            ranking.orderBy,
            limit);
    List<TestStability> tests = new ArrayList<>();
    for (Aggregate row : aggregates) {
      tests.add(
          new TestStability(
              row.test,
              row.sessions,
              row.flakySessions,
              row.failedSessions,
              row.attempts,
              row.failed,
              row.retried,
              row.retryCostMs,
              row.sessions == 0 ? 0 : (double) row.flakySessions / row.sessions,
              row.lastFailure,
              topFailure(row.test, "ts >= ? AND ts < ?", List.of(from.toEpochMilli(), to.toEpochMilli()))));
    }
    return tests;
  }

  /**
   * Tests that flaked in one session and in none of the baseline sessions that ran before
   * it -- the question a pull request's build asks of its own run.
   */
  public Emerging emergingInSession(String session, Duration baseline) {
    requireAvailable();
    long[] span =
        query(
                "SELECT MIN(ts), MAX(ts) FROM test_attempt WHERE session = ?",
                List.of(session),
                rows ->
                    rows.getObject(1) == null
                        ? null
                        : new long[] {rows.getLong(1), rows.getLong(2)})
            .get(0);
    if (span == null) {
      return new Emerging(null, null, null, List.of());
    }
    Instant from = Instant.ofEpochMilli(span[0]);
    Instant to = Instant.ofEpochMilli(span[1] + 1);
    return emerging("session = ?", List.of(session), from, to, baseline, session);
  }

  /** As {@link #emergingInSession}, with a time window as the scope. */
  public Emerging emergingInWindow(Instant from, Instant to, Duration baseline) {
    requireAvailable();
    return emerging(
        "ts >= ? AND ts < ?",
        List.of(from.toEpochMilli(), to.toEpochMilli()),
        from,
        to,
        baseline,
        null);
  }

  /** The emerging list together with the windows it was judged over. */
  public record Emerging(Instant from, Instant to, Instant baselineFrom, List<EmergingTest> tests) {}

  private Emerging emerging(
      String scopeFilter,
      List<Object> scopeParams,
      Instant from,
      Instant to,
      Duration baseline,
      String excludedSession) {
    List<Aggregate> flaky =
        aggregate(scopeFilter, scopeParams, "flaky_sessions > 0", "retried DESC, retry_cost_ms DESC", 1_000);
    Instant baselineFrom = from.minus(baseline);
    if (flaky.isEmpty()) {
      return new Emerging(from, to, baselineFrom, List.of());
    }
    List<Object> baselineParams =
        new ArrayList<>(List.of(baselineFrom.toEpochMilli(), from.toEpochMilli()));
    String baselineFilter = "ts >= ? AND ts < ?";
    if (excludedSession != null) {
      baselineFilter += " AND session <> ?";
      baselineParams.add(excludedSession);
    }
    baselineFilter += " AND history_key IN (" + placeholders(flaky.size()) + ")";
    flaky.forEach(row -> baselineParams.add(row.test));
    Map<String, Aggregate> before = new LinkedHashMap<>();
    for (Aggregate row : aggregate(baselineFilter, baselineParams, null, "history_key", flaky.size())) {
      before.put(row.test, row);
    }
    List<EmergingTest> tests = new ArrayList<>();
    for (Aggregate row : flaky) {
      Aggregate prior = before.get(row.test);
      if (prior != null && prior.flakySessions > 0) {
        continue;
      }
      tests.add(
          new EmergingTest(
              row.test,
              row.flakySessions,
              row.retried,
              row.retryCostMs,
              prior == null ? 0 : prior.sessions,
              0,
              row.lastFailure,
              topFailure(row.test, scopeFilter, scopeParams)));
    }
    return new Emerging(from, to, baselineFrom, tests);
  }

  /** Consecutive buckets covering the window, empty ones included so a chart has no gaps. */
  public List<TrendBucket> trend(Instant from, Instant to, Duration bucket, String test) {
    requireAvailable();
    long bucketMs = bucket.toMillis();
    List<Object> params = new ArrayList<>(List.of(from.toEpochMilli(), to.toEpochMilli()));
    String filter = "ts >= ? AND ts < ?";
    if (test != null) {
      filter += " AND history_key = ?";
      params.add(test);
    }
    List<Object> all = new ArrayList<>(params);
    all.add(from.toEpochMilli());
    all.add(bucketMs);
    Map<Instant, TrendBucket> filled = new LinkedHashMap<>();
    List<TrendBucket> nonEmpty =
        query(
        unitsCte(filter)
            + " SELECT (started - ?) / ? AS bucket, COUNT(DISTINCT session),"
            + " COUNT(DISTINCT CASE WHEN flaky THEN session END),"
            + " COUNT(DISTINCT CASE WHEN flaky THEN history_key END),"
            + " SUM(attempts), SUM(failed), SUM(retried), SUM(retry_cost_ms)"
            + " FROM units GROUP BY bucket",
        all,
        rows -> {
          long index = rows.getLong(1);
          return new TrendBucket(
              from.plusMillis(index * bucketMs),
              min(from.plusMillis((index + 1) * bucketMs), to),
              rows.getInt(2),
              rows.getInt(3),
              rows.getInt(4),
              rows.getInt(5),
              rows.getInt(6),
              rows.getInt(7),
              rows.getLong(8));
        });
    nonEmpty.forEach(row -> filled.put(row.from(), row));
    List<TrendBucket> buckets = new ArrayList<>();
    for (long index = 0; from.plusMillis(index * bucketMs).isBefore(to); index++) {
      Instant start = from.plusMillis(index * bucketMs);
      buckets.add(
          filled.getOrDefault(
              start,
              new TrendBucket(start, min(start.plusMillis(bucketMs), to), 0, 0, 0, 0, 0, 0, 0)));
    }
    return buckets;
  }

  /** Deletion by age, then any fault no remaining attempt points at. */
  public void prune(Duration retention, Instant now) {
    if (!available()) {
      return;
    }
    synchronized (writer) {
      try (PreparedStatement attempts =
              writer.prepareStatement("DELETE FROM test_attempt WHERE ts < ?");
          Statement orphans = writer.createStatement()) {
        attempts.setLong(1, now.minus(retention).toEpochMilli());
        attempts.executeUpdate();
        orphans.executeUpdate(
            "DELETE FROM failure_signature WHERE NOT EXISTS"
                + " (SELECT 1 FROM test_attempt t WHERE t.signature_id = failure_signature.id)");
      } catch (SQLException e) {
        log.warn("Pruning failure statistics failed: {}", e.toString());
      }
    }
  }

  /**
   * Per unit and session first, then per test: a unit is flaky in a session when it failed
   * and its last attempt passed, and a failed attempt counts as retried when it is not the
   * last one. Invocation rows are left out -- their template's own row already counts the
   * attempt.
   */
  private static String unitsCte(String filter) {
    return "WITH attempts AS (SELECT history_key, test_id, session, outcome, duration_ms, ts,"
        + " ROW_NUMBER() OVER (PARTITION BY session, test_id ORDER BY attempt DESC, id DESC)"
        + " AS from_last FROM test_attempt WHERE kind <> 'INVOCATION' AND "
        + filter
        + "), units AS (SELECT history_key, session, COUNT(*) AS attempts,"
        + " SUM(outcome = 'FAILED') AS failed,"
        + " SUM(outcome = 'FAILED' AND from_last > 1) AS retried,"
        + " SUM(CASE WHEN outcome = 'FAILED' AND from_last > 1 THEN duration_ms ELSE 0 END)"
        + " AS retry_cost_ms,"
        + " SUM(outcome = 'FAILED') > 0 AND MAX(from_last = 1 AND outcome = 'PASSED') AS flaky,"
        + " MAX(from_last = 1 AND outcome = 'FAILED') AS ended_failed,"
        + " MAX(CASE WHEN outcome = 'FAILED' THEN ts END) AS last_failure,"
        + " MIN(ts) AS started"
        + " FROM attempts GROUP BY history_key, test_id, session)";
  }

  private record Aggregate(
      String test,
      int sessions,
      int flakySessions,
      int failedSessions,
      int attempts,
      int failed,
      int retried,
      long retryCostMs,
      Instant lastFailure) {}

  private List<Aggregate> aggregate(
      String filter, List<Object> params, String having, String orderBy, int limit) {
    List<Object> all = new ArrayList<>(params);
    all.add(limit);
    return query(
        unitsCte(filter)
            + " SELECT history_key, COUNT(DISTINCT session) AS sessions,"
            + " COUNT(DISTINCT CASE WHEN flaky THEN session END) AS flaky_sessions,"
            + " COUNT(DISTINCT CASE WHEN ended_failed THEN session END) AS failed_sessions,"
            + " SUM(attempts) AS attempts, SUM(failed) AS failed, SUM(retried) AS retried,"
            + " SUM(retry_cost_ms) AS retry_cost_ms, MAX(last_failure) AS last_failure"
            + " FROM units GROUP BY history_key"
            + (having == null ? "" : " HAVING " + having)
            + " ORDER BY "
            + orderBy
            + ", history_key LIMIT ?",
        all,
        rows ->
            new Aggregate(
                rows.getString(1),
                rows.getInt(2),
                rows.getInt(3),
                rows.getInt(4),
                rows.getInt(5),
                rows.getInt(6),
                rows.getInt(7),
                rows.getLong(8),
                rows.getObject(9) == null ? null : Instant.ofEpochMilli(rows.getLong(9))));
  }

  private TopFailure topFailure(String test, String filter, List<Object> params) {
    List<Object> all = new ArrayList<>(params);
    all.add(test);
    List<TopFailure> top =
        query(
            "SELECT s.fingerprint, s.exception_class, s.message, COUNT(*) AS occurrences"
                + " FROM (SELECT signature_id, ts FROM test_attempt WHERE "
                + filter
                + " AND history_key = ?) t JOIN failure_signature s ON s.id = t.signature_id"
                + " GROUP BY s.id ORDER BY occurrences DESC, MAX(t.ts) DESC LIMIT 1",
            all,
            rows ->
                new TopFailure(
                    rows.getString(1), rows.getString(2), rows.getString(3), rows.getInt(4)));
    return top.isEmpty() ? null : top.get(0);
  }

  @FunctionalInterface
  private interface RowMapper<T> {
    T map(ResultSet rows) throws SQLException;
  }

  private <T> List<T> query(String sql, List<Object> params, RowMapper<T> mapper) {
    synchronized (reader) {
      try (PreparedStatement statement = reader.prepareStatement(sql)) {
        for (int i = 0; i < params.size(); i++) {
          statement.setObject(i + 1, params.get(i));
        }
        List<T> results = new ArrayList<>();
        try (ResultSet rows = statement.executeQuery()) {
          while (rows.next()) {
            results.add(mapper.map(rows));
          }
        }
        return results;
      } catch (SQLException e) {
        throw new IllegalStateException("Failure-statistics query failed: " + e.getMessage(), e);
      }
    }
  }

  private void requireAvailable() {
    if (!available()) {
      throw new StatsUnavailableException(unavailableReason);
    }
  }

  private static String historyKeyOf(String testId) {
    try {
      return CensusUnit.parse(testId).historyKey().value();
    } catch (IllegalArgumentException e) {
      return testId;
    }
  }

  private static String placeholders(int count) {
    return String.join(", ", Collections.nCopies(count, "?"));
  }

  private static String capped(String text, int limit) {
    return text == null || text.length() <= limit ? text : text.substring(0, limit);
  }

  private static Instant min(Instant left, Instant right) {
    return left.isBefore(right) ? left : right;
  }

  private void rollbackQuietly() {
    try {
      writer.rollback();
    } catch (SQLException e) {
      log.warn("Rollback of a failure-statistics write failed: {}", e.toString());
    }
  }

  private void autoCommitQuietly() {
    try {
      writer.setAutoCommit(true);
    } catch (SQLException e) {
      log.warn("Restoring auto-commit on the statistics connection failed: {}", e.toString());
    }
  }

  @Override
  public void close() throws SQLException {
    if (writer != null) {
      writer.close();
      reader.close();
    }
  }
}
