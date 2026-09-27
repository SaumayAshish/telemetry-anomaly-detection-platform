package com.telemetry.platform.alerting;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.OffsetDateTime;

public class BatchInsertHarness {

    private static final String URL = "jdbc:postgresql://localhost:5433/telemetry";
    private static final String USER = "telemetry_app";
    private static final String PASSWORD = "localdevpassword";
    private static final int ROW_COUNT = 2000;
    private static final String SENSOR_ID_PREFIX = "S-PERFTEST-";

    private static final String INSERT_SQL =
            "INSERT INTO anomaly_alerts (sensor_id, reading_timestamp, value, baseline_mean, baseline_std_dev, z_score, consecutive_anomalies, severity) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    public static void main(String[] args) throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername(USER);
        config.setPassword(PASSWORD);
        config.setMaximumPoolSize(5);
        config.setPoolName("batch-insert-harness-pool");

        try (HikariDataSource dataSource = new HikariDataSource(config)) {

            System.out.println("=== Section 1: " + ROW_COUNT + " individual inserts, autoCommit=true (default) ===");
            long time1 = insertIndividuallyAutoCommit(dataSource);
            System.out.println("  Elapsed: " + time1 + "ms");
            deleteTestRows(dataSource);

            System.out.println();
            System.out.println("=== Section 2: " + ROW_COUNT + " individual inserts, one explicit transaction ===");
            long time2 = insertIndividuallyOneTransaction(dataSource);
            System.out.println("  Elapsed: " + time2 + "ms");
            deleteTestRows(dataSource);

            System.out.println();
            System.out.println("=== Section 3: " + ROW_COUNT + " inserts via addBatch()/executeBatch(), one transaction ===");
            long time3 = insertViaBatch(dataSource);
            System.out.println("  Elapsed: " + time3 + "ms");
            deleteTestRows(dataSource);

            System.out.println();
            System.out.println("Summary: individual/autoCommit=" + time1 + "ms, individual/one-transaction=" + time2 + "ms, batched=" + time3 + "ms");
            System.out.println("Test rows cleaned up. Harness complete.");
        }
    }

    private static long insertIndividuallyAutoCommit(HikariDataSource dataSource) throws Exception {
        long start = System.nanoTime();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
            for (int i = 0; i < ROW_COUNT; i++) {
                bindRow(stmt, i);
                stmt.executeUpdate();
            }
        }
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static long insertIndividuallyOneTransaction(HikariDataSource dataSource) throws Exception {
        long start = System.nanoTime();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
                for (int i = 0; i < ROW_COUNT; i++) {
                    bindRow(stmt, i);
                    stmt.executeUpdate();
                }
                conn.commit();
            } finally {
                conn.setAutoCommit(true);
            }
        }
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static long insertViaBatch(HikariDataSource dataSource) throws Exception {
        long start = System.nanoTime();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
                for (int i = 0; i < ROW_COUNT; i++) {
                    bindRow(stmt, i);
                    stmt.addBatch();
                }
                stmt.executeBatch();
                conn.commit();
            } finally {
                conn.setAutoCommit(true);
            }
        }
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static void bindRow(PreparedStatement stmt, int i) throws Exception {
        stmt.setString(1, SENSOR_ID_PREFIX + i);
        stmt.setObject(2, OffsetDateTime.now());
        stmt.setDouble(3, 50.0);
        stmt.setDouble(4, 10.0);
        stmt.setDouble(5, 2.0);
        stmt.setDouble(6, 1.0);
        stmt.setLong(7, 1);
        stmt.setString(8, "LOW");
    }

    private static void deleteTestRows(HikariDataSource dataSource) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("DELETE FROM anomaly_alerts WHERE sensor_id LIKE ?")) {
            stmt.setString(1, SENSOR_ID_PREFIX + "%");
            stmt.executeUpdate();
        }
    }
}