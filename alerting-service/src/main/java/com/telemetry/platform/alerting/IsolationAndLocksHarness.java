package com.telemetry.platform.alerting;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IsolationAndLocksHarness {

    private static final String URL = "jdbc:postgresql://localhost:5433/telemetry";
    private static final String USER = "telemetry_app";
    private static final String PASSWORD = "localdevpassword";

    public static void main(String[] args) throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername(USER);
        config.setPassword(PASSWORD);
        config.setMaximumPoolSize(5);
        config.setPoolName("isolation-harness-pool");

        try (HikariDataSource dataSource = new HikariDataSource(config)) {

            long testRowId = insertTestRow(dataSource, "S-6001", 10.0);
            System.out.println("Inserted test row id=" + testRowId + " with z_score=10.0");

            System.out.println();
            System.out.println("=== Section 1: non-repeatable read under READ COMMITTED (Postgres's default) ===");
            demonstrateReadCommitted(dataSource, testRowId);

            resetZScore(dataSource, testRowId, 10.0);

            System.out.println();
            System.out.println("=== Section 2: REPEATABLE READ prevents the same non-repeatable read ===");
            demonstrateRepeatableRead(dataSource, testRowId);

            System.out.println();
            System.out.println("=== Section 3: SELECT ... FOR UPDATE - a real row lock blocking a second transaction ===");
            demonstrateRowLock(dataSource, testRowId);

            deleteTestRow(dataSource, testRowId);
            System.out.println();
            System.out.println("Test row deleted. Harness complete.");
        }
    }

    private static void demonstrateReadCommitted(HikariDataSource dataSource, long testRowId) throws SQLException {
        try (Connection conn1 = dataSource.getConnection()) {
            conn1.setAutoCommit(false);
            conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            double firstRead = readZScore(conn1, testRowId);
            System.out.println("  conn1 (open transaction) first read: z_score = " + firstRead);

            try (Connection conn2 = dataSource.getConnection()) {
                updateZScore(conn2, testRowId, 99.0);
                System.out.println("  conn2 updated and committed z_score = 99.0 (separate, already-committed transaction)");
            }

            double secondRead = readZScore(conn1, testRowId);
            System.out.println("  conn1 (SAME open transaction) second read: z_score = " + secondRead);
            System.out.println("  " + (firstRead == secondRead ? "UNCHANGED" : "CHANGED")
                    + " within one transaction - this is the non-repeatable read.");

            conn1.commit();
            conn1.setAutoCommit(true);
        }
    }

    private static void demonstrateRepeatableRead(HikariDataSource dataSource, long testRowId) throws SQLException {
        try (Connection conn1 = dataSource.getConnection()) {
            conn1.setAutoCommit(false);
            conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            double firstRead = readZScore(conn1, testRowId);
            System.out.println("  conn1 (open REPEATABLE READ transaction) first read: z_score = " + firstRead);

            try (Connection conn2 = dataSource.getConnection()) {
                updateZScore(conn2, testRowId, 77.0);
                System.out.println("  conn2 updated and committed z_score = 77.0 (separate, already-committed transaction)");
            }

            double secondRead = readZScore(conn1, testRowId);
            System.out.println("  conn1 (SAME open REPEATABLE READ transaction) second read: z_score = " + secondRead);
            System.out.println("  " + (firstRead == secondRead ? "UNCHANGED" : "CHANGED")
                    + " within one transaction - REPEATABLE READ's snapshot held.");

            conn1.commit();
            conn1.setAutoCommit(true);
        }
    }

    private static void demonstrateRowLock(HikariDataSource dataSource, long testRowId) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection conn1 = dataSource.getConnection()) {
            conn1.setAutoCommit(false);
            try (PreparedStatement stmt = conn1.prepareStatement(
                    "SELECT z_score FROM anomaly_alerts WHERE id = ? FOR UPDATE")) {
                stmt.setLong(1, testRowId);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    System.out.println("  conn1 acquired the row lock via SELECT ... FOR UPDATE (z_score=" + rs.getDouble(1) + ") and is holding it.");
                }
            }

            System.out.println("  Launching conn2's SELECT ... FOR UPDATE on a background thread - it should block.");
            long start = System.nanoTime();
            CompletableFuture<Long> conn2Attempt = CompletableFuture.supplyAsync(() -> {
                try (Connection conn2 = dataSource.getConnection()) {
                    conn2.setAutoCommit(false);
                    try (PreparedStatement stmt = conn2.prepareStatement(
                            "SELECT z_score FROM anomaly_alerts WHERE id = ? FOR UPDATE")) {
                        stmt.setLong(1, testRowId);
                        try (ResultSet rs = stmt.executeQuery()) {
                            rs.next();
                        }
                    }
                    conn2.commit();
                    conn2.setAutoCommit(true);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return (System.nanoTime() - start) / 1_000_000;
            }, executor);

            System.out.println("  Main thread holding conn1's lock for 3000ms before releasing it...");
            Thread.sleep(3000);
            conn1.commit();
            conn1.setAutoCommit(true);
            System.out.println("  conn1 committed - lock released.");

            long conn2WaitedMs = conn2Attempt.get();
            System.out.println("  conn2's SELECT ... FOR UPDATE unblocked after " + conn2WaitedMs
                    + "ms - proving it was genuinely waiting on conn1's row lock.");
        } finally {
            executor.shutdown();
        }
    }

    private static double readZScore(Connection conn, long id) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement("SELECT z_score FROM anomaly_alerts WHERE id = ?")) {
            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getDouble(1);
            }
        }
    }

    private static void updateZScore(Connection conn, long id, double newValue) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement("UPDATE anomaly_alerts SET z_score = ? WHERE id = ?")) {
            stmt.setDouble(1, newValue);
            stmt.setLong(2, id);
            stmt.executeUpdate();
        }
    }

    private static void resetZScore(HikariDataSource dataSource, long id, double value) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            updateZScore(conn, id, value);
        }
    }

    private static long insertTestRow(HikariDataSource dataSource, String sensorId, double zScore) throws SQLException {
        String sql = "INSERT INTO anomaly_alerts (sensor_id, reading_timestamp, value, baseline_mean, baseline_std_dev, z_score, consecutive_anomalies, severity) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, sensorId);
            stmt.setObject(2, OffsetDateTime.now());
            stmt.setDouble(3, 50.0);
            stmt.setDouble(4, 10.0);
            stmt.setDouble(5, 2.0);
            stmt.setDouble(6, zScore);
            stmt.setLong(7, 1);
            stmt.setString(8, "LOW");
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static void deleteTestRow(HikariDataSource dataSource, long id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("DELETE FROM anomaly_alerts WHERE id = ?")) {
            stmt.setLong(1, id);
            stmt.executeUpdate();
        }
    }
}
