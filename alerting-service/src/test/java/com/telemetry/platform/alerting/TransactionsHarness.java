package com.telemetry.platform.alerting;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TransactionsHarness {

    private static final String URL = "jdbc:postgresql://localhost:5433/telemetry";
    private static final String USER = "telemetry_app";
    private static final String PASSWORD = "localdevpassword";

    private static final String INSERT_SQL =
            "INSERT INTO anomaly_alerts (sensor_id, reading_timestamp, value, baseline_mean, baseline_std_dev, z_score, consecutive_anomalies, severity) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    public static void main(String[] args) throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername(USER);
        config.setPassword(PASSWORD);
        config.setMaximumPoolSize(3);
        config.setPoolName("transactions-harness-pool");

        try (HikariDataSource dataSource = new HikariDataSource(config)) {

            System.out.println("=== Section 1: default autoCommit=true - each statement is its OWN implicit transaction ===");
            System.out.println("sensor S-7777 rows before: " + countRows(dataSource, "S-7777"));
            try (Connection conn = dataSource.getConnection()) {
                insertRow(conn, "S-7777", "LOW");      // valid
                insertRow(conn, "S-7777", "INVALID");  // violates CHECK constraint
            } catch (SQLException e) {
                System.out.println("  Second insert failed as expected: " + e.getMessage().split("\n")[0]);
            }
            System.out.println("sensor S-7777 rows after: " + countRows(dataSource, "S-7777")
                    + " (the FIRST insert already committed on its own - autoCommit gave the pair no atomicity)");
            cleanup(dataSource, "S-7777");

            System.out.println();
            System.out.println("=== Section 2: explicit transaction, rolled back on failure - atomicity in action ===");
            System.out.println("sensor S-7778 rows before: " + countRows(dataSource, "S-7778"));
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    insertRow(conn, "S-7778", "LOW");      // valid
                    insertRow(conn, "S-7778", "INVALID");  // violates CHECK constraint
                    conn.commit();
                } catch (SQLException e) {
                    System.out.println("  Second insert failed, rolling back the WHOLE transaction: " + e.getMessage().split("\n")[0]);
                    conn.rollback();
                } finally {
                    conn.setAutoCommit(true);
                }
            }
            System.out.println("sensor S-7778 rows after: " + countRows(dataSource, "S-7778")
                    + " (rollback undid the first insert too - all or nothing)");

            System.out.println();
            System.out.println("=== Section 3: explicit transaction, both inserts valid - committed together ===");
            System.out.println("sensor S-7779 rows before: " + countRows(dataSource, "S-7779"));
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    insertRow(conn, "S-7779", "LOW");
                    insertRow(conn, "S-7779", "MEDIUM");
                    conn.commit();
                    System.out.println("  Both inserts committed together.");
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                } finally {
                    conn.setAutoCommit(true);
                }
            }
            System.out.println("sensor S-7779 rows after: " + countRows(dataSource, "S-7779"));
            cleanup(dataSource, "S-7779");
        }
    }

    private static void insertRow(Connection conn, String sensorId, String severity) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
            stmt.setString(1, sensorId);
            stmt.setObject(2, OffsetDateTime.now());
            stmt.setDouble(3, 50.0);
            stmt.setDouble(4, 10.0);
            stmt.setDouble(5, 2.0);
            stmt.setDouble(6, 4.0);
            stmt.setLong(7, 1);
            stmt.setString(8, severity);
            stmt.executeUpdate();
        }
    }

    private static int countRows(HikariDataSource dataSource, String sensorId) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) FROM anomaly_alerts WHERE sensor_id = ?")) {
            stmt.setString(1, sensorId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private static void cleanup(HikariDataSource dataSource, String sensorId) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("DELETE FROM anomaly_alerts WHERE sensor_id = ?")) {
            stmt.setString(1, sensorId);
            stmt.executeUpdate();
        }
    }
}