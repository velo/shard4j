package com.marvinformatics.shard4j.coordinator.stats;

import com.marvinformatics.shard4j.coordinator.core.AttemptKind;
import com.marvinformatics.shard4j.coordinator.core.Truncation;
import com.marvinformatics.shard4j.coordinator.core.UnknownSessionException;
import com.marvinformatics.shard4j.protocol.CensusUnit;
import com.marvinformatics.shard4j.protocol.Failure;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.sqlite.SQLiteConfig;

/**
 * Every attempt of every unit, passes included, plus each distinct failure stored once.
 *
 * <p>Two connections, because WAL lets a reader run beside the writer: a month-wide aggregation
 * must not hold up the result path.
 */
@Slf4j
@RequiredArgsConstructor
final class SqliteStatsStore implements StatsStore {

  static final int STACK_TRACE_MAX_CHARS = 64 * 1024;
  static final int MESSAGE_MAX_CHARS = 4 * 1024;
  private static final int EXCEPTION_CLASS_MAX_CHARS = 512;

  private final Connection writeConnection;
  private final Connection readConnection;

  static StatsStore openOrCreateElseDisabled(Path databaseFile) {
    try {
      SQLiteConfig config = new SQLiteConfig();
      config.setJournalMode(SQLiteConfig.JournalMode.WAL);
      config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);
      config.enforceForeignKeys(true);
      config.setBusyTimeout(5_000);
      Connection writeConnection = config.createConnection("jdbc:sqlite:" + databaseFile);
      try {
        StatsSchema.migrateSchemaToLatest(writeConnection, databaseFile);
      } catch (SQLException | RuntimeException e) {
        writeConnection.close();
        throw e;
      }
      return new SqliteStatsStore(
          writeConnection, config.createConnection("jdbc:sqlite:" + databaseFile));
    } catch (SQLException | RuntimeException | LinkageError e) {
      log.error(
          "Failure statistics disabled: cannot open {}; scheduling is unaffected: {}",
          databaseFile,
          e.toString());
      return new DisabledStatsStore(e.toString());
    }
  }

  @Override
  public void recordAttemptsQuietly(
      String sessionId, int shard, int attemptNumber, List<Attempt> attempts, Instant acceptedAt) {
    synchronized (writeConnection) {
      try {
        writeConnection.setAutoCommit(false);
        for (Attempt attempt : attempts) {
          insertAttemptAndUpsertDistinctFailure(
              sessionId, shard, attemptNumber, attempt, acceptedAt);
        }
        writeConnection.commit();
      } catch (SQLException | RuntimeException e) {
        rollbackWriterQuietly();
        log.warn(
            "Failure-statistics write dropped for {}: {}",
            attempts.isEmpty() ? sessionId : attempts.get(0).testId(),
            e.toString());
      } finally {
        restoreWriterAutoCommitQuietly();
      }
    }
  }

  private void insertAttemptAndUpsertDistinctFailure(
      String sessionId, int shard, int attemptNumber, Attempt attempt, Instant acceptedAt)
      throws SQLException {
    Long distinctFailureId =
        attempt.failure() == null ? null : upsertDistinctFailureId(attempt.failure(), acceptedAt);
    try (PreparedStatement insert =
        writeConnection.prepareStatement(
            "INSERT INTO test_attempt (session_id, test_id, history_key, unit_kind, shard,"
                + " attempt_number, outcome, duration_ms, reason, distinct_failure_id,"
                + " recorded_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
      insert.setString(1, sessionId);
      insert.setString(2, attempt.testId());
      insert.setString(3, deriveHistoryKeyOrTestId(attempt.testId()));
      insert.setString(4, attempt.unitKind().name());
      insert.setInt(5, shard);
      insert.setInt(6, attemptNumber);
      insert.setString(7, attempt.outcome().name());
      insert.setLong(8, attempt.durationMs());
      insert.setString(9, attempt.reason());
      insert.setObject(10, distinctFailureId);
      insert.setLong(11, acceptedAt.toEpochMilli());
      insert.executeUpdate();
    }
  }

  private long upsertDistinctFailureId(Failure failure, Instant seenAt) throws SQLException {
    String exceptionClass =
        Truncation.truncate(
            Objects.requireNonNullElse(failure.exceptionClass(), "unknown"),
            EXCEPTION_CLASS_MAX_CHARS);
    String stackTrace =
        Truncation.truncate(
            Objects.requireNonNullElse(failure.stackTrace(), ""), STACK_TRACE_MAX_CHARS);
    try (PreparedStatement upsert =
        writeConnection.prepareStatement(
            "INSERT INTO distinct_failure (fingerprint, exception_class, message, stack_trace,"
                + " first_seen_ms, last_seen_ms) VALUES (?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT (fingerprint) DO UPDATE SET last_seen_ms = excluded.last_seen_ms,"
                + " message = excluded.message, stack_trace = excluded.stack_trace"
                + " RETURNING id")) {
      upsert.setString(1, FailureFingerprint.hashNormalizedTrace(exceptionClass, stackTrace));
      upsert.setString(2, exceptionClass);
      upsert.setString(3, Truncation.truncate(failure.message(), MESSAGE_MAX_CHARS));
      upsert.setString(4, stackTrace);
      upsert.setLong(5, seenAt.toEpochMilli());
      upsert.setLong(6, seenAt.toEpochMilli());
      try (ResultSet row = upsert.executeQuery()) {
        row.next();
        return row.getLong(1);
      }
    }
  }

  @Override
  public FailureReport findLatestFailedAttempts(
      Instant from, Instant to, String testIdOrHistoryKey, int limit) {
    Filter filter =
        Filter.window(from, to)
            .and("outcome = 'FAILED'")
            .and("(unit_kind <> 'TEMPLATE' OR distinct_failure_id IS NOT NULL)");
    if (testIdOrHistoryKey != null) {
      filter =
          filter.and("(history_key = ? OR test_id = ?)", testIdOrHistoryKey, testIdOrHistoryKey);
    }
    List<Object> queryParams = new ArrayList<>(filter.params());
    queryParams.add(limit);
    List<FailedAttempt> failedAttempts =
        query(
            "SELECT t.session_id, t.test_id, t.history_key, t.unit_kind, t.shard, t.attempt_number,"
                + " t.duration_ms, t.reason, t.recorded_at_ms, d.fingerprint, EXISTS (SELECT 1 FROM"
                + " test_attempt later WHERE later.session_id = t.session_id AND later.test_id ="
                + " t.test_id AND later.attempt_number > t.attempt_number) AS retried FROM"
                + " test_attempt t LEFT JOIN distinct_failure d ON d.id = t.distinct_failure_id"
                + " WHERE "
                + filter.sql()
                + " ORDER BY t.recorded_at_ms DESC, t.id DESC LIMIT ?",
            queryParams,
            row ->
                new FailedAttempt(
                    row.getString("session_id"),
                    row.getString("test_id"),
                    row.getString("history_key"),
                    AttemptKind.valueOf(row.getString("unit_kind")),
                    row.getInt("shard"),
                    row.getInt("attempt_number"),
                    row.getLong("duration_ms"),
                    row.getString("reason"),
                    Instant.ofEpochMilli(row.getLong("recorded_at_ms")),
                    row.getBoolean("retried"),
                    row.getString("fingerprint")));
    Set<String> fingerprints = new LinkedHashSet<>();
    failedAttempts.stream()
        .map(FailedAttempt::fingerprint)
        .filter(Objects::nonNull)
        .forEach(fingerprints::add);
    return new FailureReport(failedAttempts, findDistinctFailuresByFingerprint(fingerprints));
  }

  private Map<String, DistinctFailure> findDistinctFailuresByFingerprint(Set<String> fingerprints) {
    Map<String, DistinctFailure> distinctFailuresByFingerprint = new LinkedHashMap<>();
    if (fingerprints.isEmpty()) {
      return distinctFailuresByFingerprint;
    }
    query(
            "SELECT fingerprint, exception_class, message, stack_trace, first_seen_ms,"
                + " last_seen_ms FROM distinct_failure WHERE fingerprint IN ("
                + buildInClausePlaceholders(fingerprints.size())
                + ")",
            new ArrayList<>(fingerprints),
            row ->
                Map.entry(
                    row.getString("fingerprint"),
                    new DistinctFailure(
                        row.getString("exception_class"),
                        row.getString("message"),
                        row.getString("stack_trace"),
                        Instant.ofEpochMilli(row.getLong("first_seen_ms")),
                        Instant.ofEpochMilli(row.getLong("last_seen_ms")))))
        .forEach(entry -> distinctFailuresByFingerprint.put(entry.getKey(), entry.getValue()));
    return distinctFailuresByFingerprint;
  }

  @Override
  public List<TestStability> findTestsWithFailures(
      Instant from, Instant to, UnstableTestOrder order, int limit) {
    Filter window = Filter.window(from, to);
    List<Object> queryParams = new ArrayList<>();
    String sql =
        "WITH "
            + buildUnitSessionsCte("scope", window, queryParams)
            + ", "
            + buildTotalsCte("scope")
            + ", "
            + buildMostFrequentFailureCte("scope", window, queryParams)
            + " SELECT scope_totals.*,"
            + " CAST(flaky_sessions AS REAL) / sessions AS flake_rate,"
            + " d.fingerprint, d.exception_class, d.message, scope_top.occurrences"
            + " FROM scope_totals"
            + " LEFT JOIN scope_top ON scope_top.history_key = scope_totals.history_key"
            + " AND scope_top.fault_rank = 1"
            + " LEFT JOIN distinct_failure d ON d.id = scope_top.distinct_failure_id"
            + " WHERE failed_attempts > 0"
            + " ORDER BY "
            + order.orderByClause
            + ", scope_totals.history_key LIMIT ?";
    queryParams.add(limit);
    return query(
        sql,
        queryParams,
        row ->
            new TestStability(
                row.getString("history_key"),
                row.getInt("sessions"),
                row.getInt("flaky_sessions"),
                row.getInt("failed_sessions"),
                row.getInt("attempts"),
                row.getInt("failed_attempts"),
                row.getInt("retried_attempts"),
                row.getLong("retry_cost_ms"),
                row.getDouble("flake_rate"),
                instantOrNull(row, "last_failure_at_ms"),
                mostFrequentFailureOrNull(row)));
  }

  /** The scope is the session's own span, so the baseline is everything recorded before it. */
  @Override
  public NewlyFlakyReport findNewlyFlakyTestsInSession(String sessionId, Duration baselineSpan) {
    long[] firstAndLastAttemptMillis =
        query(
                "SELECT MIN(recorded_at_ms) AS first_ms, MAX(recorded_at_ms) AS last_ms"
                    + " FROM test_attempt WHERE session_id = ?",
                List.of(sessionId),
                row ->
                    row.getObject("first_ms") == null
                        ? null
                        : new long[] {row.getLong("first_ms"), row.getLong("last_ms")})
            .get(0);
    if (firstAndLastAttemptMillis == null) {
      throw new UnknownSessionException(sessionId);
    }
    return findNewlyFlakyTests(
        Filter.session(sessionId),
        Instant.ofEpochMilli(firstAndLastAttemptMillis[0]),
        Instant.ofEpochMilli(firstAndLastAttemptMillis[1] + 1),
        baselineSpan);
  }

  @Override
  public NewlyFlakyReport findNewlyFlakyTestsInWindow(
      Instant from, Instant to, Duration baselineSpan) {
    return findNewlyFlakyTests(Filter.window(from, to), from, to, baselineSpan);
  }

  private NewlyFlakyReport findNewlyFlakyTests(
      Filter scope, Instant from, Instant to, Duration baselineSpan) {
    Instant baselineFrom = from.minus(baselineSpan);
    Filter baseline =
        Filter.window(baselineFrom, from)
            .and("history_key IN (SELECT history_key FROM scope_totals WHERE flaky_sessions > 0)");
    List<Object> queryParams = new ArrayList<>();
    String sql =
        "WITH "
            + buildUnitSessionsCte("scope", scope, queryParams)
            + ", "
            + buildTotalsCte("scope")
            + ", "
            + buildUnitSessionsCte("baseline", baseline, queryParams)
            + ", "
            + buildTotalsCte("baseline")
            + ", "
            + buildMostFrequentFailureCte("scope", scope, queryParams)
            + " SELECT s.history_key, s.flaky_sessions, s.retried_attempts, s.retry_cost_ms,"
            + " s.last_failure_at_ms, COALESCE(b.sessions, 0) AS baseline_sessions,"
            + " d.fingerprint, d.exception_class, d.message, scope_top.occurrences"
            + " FROM scope_totals s"
            + " LEFT JOIN baseline_totals b ON b.history_key = s.history_key"
            + " LEFT JOIN scope_top ON scope_top.history_key = s.history_key"
            + " AND scope_top.fault_rank = 1"
            + " LEFT JOIN distinct_failure d ON d.id = scope_top.distinct_failure_id"
            + " WHERE s.flaky_sessions > 0 AND COALESCE(b.flaky_sessions, 0) = 0"
            + " ORDER BY s.retried_attempts DESC, s.retry_cost_ms DESC, s.history_key";
    List<NewlyFlakyTest> tests =
        query(
            sql,
            queryParams,
            row ->
                new NewlyFlakyTest(
                    row.getString("history_key"),
                    row.getInt("flaky_sessions"),
                    row.getInt("retried_attempts"),
                    row.getLong("retry_cost_ms"),
                    row.getInt("baseline_sessions"),
                    instantOrNull(row, "last_failure_at_ms"),
                    mostFrequentFailureOrNull(row)));
    return new NewlyFlakyReport(from, to, baselineFrom, tests);
  }

  @Override
  public List<TrendBucket> findFlakinessPerBucket(
      Instant from, Instant to, Duration bucketSpan, String historyKey) {
    long bucketSpanMs = bucketSpan.toMillis();
    Filter filter = Filter.window(from, to);
    if (historyKey != null) {
      filter = filter.and("history_key = ?", historyKey);
    }
    List<Object> queryParams = new ArrayList<>();
    String sql =
        "WITH "
            + buildUnitSessionsCte("trend", filter, queryParams)
            + " SELECT (first_attempt_at_ms - ?) / ? AS bucket_index,"
            + " COUNT(DISTINCT session_id) AS sessions,"
            + " COUNT(DISTINCT CASE WHEN flaky THEN session_id END) AS flaky_sessions,"
            + " COUNT(DISTINCT CASE WHEN flaky THEN history_key END) AS flaky_tests,"
            + " SUM(attempts) AS attempts, SUM(failed_attempts) AS failed_attempts,"
            + " SUM(retried_attempts) AS retried_attempts, SUM(retry_cost_ms) AS retry_cost_ms"
            + " FROM trend_units GROUP BY bucket_index";
    queryParams.add(from.toEpochMilli());
    queryParams.add(bucketSpanMs);
    Map<Instant, TrendBucket> nonEmptyBucketsByStart = new LinkedHashMap<>();
    query(
            sql,
            queryParams,
            row -> {
              Instant bucketStart = from.plusMillis(row.getLong("bucket_index") * bucketSpanMs);
              return new TrendBucket(
                  bucketStart,
                  earlierOf(bucketStart.plusMillis(bucketSpanMs), to),
                  row.getInt("sessions"),
                  row.getInt("flaky_sessions"),
                  row.getInt("flaky_tests"),
                  row.getInt("attempts"),
                  row.getInt("failed_attempts"),
                  row.getInt("retried_attempts"),
                  row.getLong("retry_cost_ms"));
            })
        .forEach(bucket -> nonEmptyBucketsByStart.put(bucket.from(), bucket));
    List<TrendBucket> buckets = new ArrayList<>();
    for (Instant bucketStart = from;
        bucketStart.isBefore(to);
        bucketStart = bucketStart.plusMillis(bucketSpanMs)) {
      Instant bucketEnd = earlierOf(bucketStart.plusMillis(bucketSpanMs), to);
      buckets.add(
          nonEmptyBucketsByStart.getOrDefault(
              bucketStart, new TrendBucket(bucketStart, bucketEnd, 0, 0, 0, 0, 0, 0, 0)));
    }
    return buckets;
  }

  /** Age first, then any distinct failure no remaining attempt points at. */
  @Override
  public void prune(Duration retention, Instant now) {
    synchronized (writeConnection) {
      try (PreparedStatement deleteExpiredAttempts =
              writeConnection.prepareStatement(
                  "DELETE FROM test_attempt WHERE recorded_at_ms < ?");
          Statement deleteOrphanDistinctFailures = writeConnection.createStatement()) {
        deleteExpiredAttempts.setLong(1, now.minus(retention).toEpochMilli());
        deleteExpiredAttempts.executeUpdate();
        deleteOrphanDistinctFailures.executeUpdate(
            "DELETE FROM distinct_failure WHERE NOT EXISTS (SELECT 1 FROM test_attempt t"
                + " WHERE t.distinct_failure_id = distinct_failure.id)");
      } catch (SQLException e) {
        log.warn("Pruning failure statistics failed: {}", e.toString());
      }
    }
  }

  /**
   * Per unit and session first: a unit is flaky in a session when it failed and its last attempt
   * passed, and a failed attempt is retried when it is not the last one. Invocation rows are left
   * out, because their template's own row already counts the attempt.
   */
  private static String buildUnitSessionsCte(
      String prefix, Filter filter, List<Object> queryParams) {
    queryParams.addAll(filter.params());
    return prefix
        + "_ranked AS (SELECT history_key, test_id, session_id, outcome, duration_ms,"
        + " recorded_at_ms, ROW_NUMBER() OVER (PARTITION BY session_id, test_id"
        + " ORDER BY attempt_number DESC, id DESC) AS position_from_last"
        + " FROM test_attempt WHERE unit_kind <> 'INVOCATION' AND "
        + filter.sql()
        + "), "
        + prefix
        + "_units AS (SELECT history_key, session_id, COUNT(*) AS attempts,"
        + " SUM(outcome = 'FAILED') AS failed_attempts,"
        + " SUM(outcome = 'FAILED' AND position_from_last > 1) AS retried_attempts,"
        + " SUM(CASE WHEN outcome = 'FAILED' AND position_from_last > 1 THEN duration_ms"
        + " ELSE 0 END) AS retry_cost_ms,"
        + " SUM(outcome = 'FAILED') > 0 AND MAX(position_from_last = 1 AND outcome = 'PASSED')"
        + " AS flaky,"
        + " MAX(position_from_last = 1 AND outcome = 'FAILED') AS ended_failed,"
        + " MAX(CASE WHEN outcome = 'FAILED' THEN recorded_at_ms END) AS last_failure_at_ms,"
        + " MIN(recorded_at_ms) AS first_attempt_at_ms"
        + " FROM "
        + prefix
        + "_ranked GROUP BY history_key, test_id, session_id)";
  }

  private static String buildTotalsCte(String prefix) {
    return prefix
        + "_totals AS (SELECT history_key, COUNT(DISTINCT session_id) AS sessions,"
        + " COUNT(DISTINCT CASE WHEN flaky THEN session_id END) AS flaky_sessions,"
        + " COUNT(DISTINCT CASE WHEN ended_failed THEN session_id END) AS failed_sessions,"
        + " SUM(attempts) AS attempts, SUM(failed_attempts) AS failed_attempts,"
        + " SUM(retried_attempts) AS retried_attempts, SUM(retry_cost_ms) AS retry_cost_ms,"
        + " MAX(last_failure_at_ms) AS last_failure_at_ms FROM "
        + prefix
        + "_units GROUP BY history_key)";
  }

  private static String buildMostFrequentFailureCte(
      String prefix, Filter filter, List<Object> queryParams) {
    queryParams.addAll(filter.params());
    return prefix
        + "_top AS (SELECT history_key, distinct_failure_id, COUNT(*) AS occurrences,"
        + " ROW_NUMBER() OVER (PARTITION BY history_key"
        + " ORDER BY COUNT(*) DESC, MAX(recorded_at_ms) DESC) AS fault_rank"
        + " FROM test_attempt WHERE distinct_failure_id IS NOT NULL AND "
        + filter.sql()
        + " GROUP BY history_key, distinct_failure_id)";
  }

  private static MostFrequentFailure mostFrequentFailureOrNull(ResultSet row) throws SQLException {
    String fingerprint = row.getString("fingerprint");
    return fingerprint == null
        ? null
        : new MostFrequentFailure(
            fingerprint,
            row.getString("exception_class"),
            row.getString("message"),
            row.getInt("occurrences"));
  }

  private static Instant instantOrNull(ResultSet row, String column) throws SQLException {
    return row.getObject(column) == null ? null : Instant.ofEpochMilli(row.getLong(column));
  }

  @FunctionalInterface
  private interface RowMapper<T> {
    T mapRow(ResultSet row) throws SQLException;
  }

  private <T> List<T> query(String sql, List<Object> params, RowMapper<T> mapper) {
    synchronized (readConnection) {
      try (PreparedStatement statement = readConnection.prepareStatement(sql)) {
        for (int i = 0; i < params.size(); i++) {
          statement.setObject(i + 1, params.get(i));
        }
        List<T> mappedRows = new ArrayList<>();
        try (ResultSet rows = statement.executeQuery()) {
          while (rows.next()) {
            mappedRows.add(mapper.mapRow(rows));
          }
        }
        return mappedRows;
      } catch (SQLException e) {
        throw new IllegalStateException("Failure-statistics query failed: " + e.getMessage(), e);
      }
    }
  }

  /** Invocation-row ids reach here unvalidated; a malformed one is kept under its raw id. */
  private static String deriveHistoryKeyOrTestId(String testId) {
    try {
      return CensusUnit.historyKeyOf(testId).value();
    } catch (IllegalArgumentException e) {
      log.warn("Recording {} under its raw id: {}", testId, e.getMessage());
      return testId;
    }
  }

  private static String buildInClausePlaceholders(int count) {
    return String.join(", ", Collections.nCopies(count, "?"));
  }

  private static Instant earlierOf(Instant left, Instant right) {
    return left.isBefore(right) ? left : right;
  }

  private void rollbackWriterQuietly() {
    try {
      writeConnection.rollback();
    } catch (SQLException e) {
      log.warn("Rollback of a failure-statistics write failed: {}", e.toString());
    }
  }

  private void restoreWriterAutoCommitQuietly() {
    try {
      writeConnection.setAutoCommit(true);
    } catch (SQLException e) {
      log.warn("Restoring auto-commit on the statistics connection failed: {}", e.toString());
    }
  }

  @Override
  public void close() {
    try {
      writeConnection.close();
      readConnection.close();
    } catch (SQLException e) {
      log.warn("Closing the statistics database failed: {}", e.toString());
    }
  }
}
