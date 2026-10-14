package com.marvinformatics.shard4j.coordinator.stats;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.experimental.UtilityClass;

/** Ordered SQL resources, applied once each and tracked in {@code PRAGMA user_version}. */
@UtilityClass
class StatsSchema {

  private final List<String> MIGRATIONS = List.of("stats/001-attempts.sql");

  void migrateSchemaToLatest(Connection connection, Path databaseFile) throws SQLException {
    int currentSchemaVersion;
    try (Statement statement = connection.createStatement();
        ResultSet row = statement.executeQuery("PRAGMA user_version")) {
      currentSchemaVersion = row.getInt(1);
    }
    if (currentSchemaVersion > MIGRATIONS.size()) {
      throw new IllegalStateException(
          databaseFile
              + " is at schema version "
              + currentSchemaVersion
              + ", written by a newer coordinator");
    }
    for (int migrationIndex = currentSchemaVersion;
        migrationIndex < MIGRATIONS.size();
        migrationIndex++) {
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        for (String sql : readMigrationSql(MIGRATIONS.get(migrationIndex)).split(";\\s*\n")) {
          if (!sql.isBlank()) {
            statement.executeUpdate(sql);
          }
        }
        statement.executeUpdate("PRAGMA user_version = " + (migrationIndex + 1));
        connection.commit();
      } catch (SQLException | RuntimeException e) {
        connection.rollback();
        throw e;
      } finally {
        connection.setAutoCommit(true);
      }
    }
  }

  private String readMigrationSql(String resourceName) {
    try (InputStream in = StatsSchema.class.getClassLoader().getResourceAsStream(resourceName)) {
      if (in == null) {
        throw new IllegalStateException("Missing schema resource " + resourceName);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
