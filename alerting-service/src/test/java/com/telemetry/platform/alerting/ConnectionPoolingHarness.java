package com.telemetry.platform.alerting;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionPoolingHarness {

    private static final String URL = "jdbc:postgresql://localhost:5433/telemetry";
    private static final String USER = "telemetry_app";
    private static final String PASSWORD = "localdevpassword";
    private static final int ITERATIONS = 20;

    public static void main(String[] args) throws Exception {

        System.out.println("=== Section 1: cost contrast - raw DriverManager connections vs a warmed-up pool ===");
        timeRawConnections();
        timePooledConnections();

        System.out.println();
        System.out.println("=== Section 2: pool exhaustion - what happens when every connection is checked out ===");
        demonstratePoolExhaustion();

        System.out.println();
        System.out.println("=== Section 3: proving close() returns a connection to the pool instead of destroying it ===");
        demonstrateCloseReturnsToPool();
    }

    private static void timeRawConnections() throws SQLException {
        long start = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
                // immediately close - simulating one query's worth of connection use
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        System.out.println("  " + ITERATIONS + " raw DriverManager connections (open+close each time): "
                + elapsedMs + "ms total, " + (elapsedMs / (double) ITERATIONS) + "ms average per connection");
    }

    private static void timePooledConnections() throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername(USER);
        config.setPassword(PASSWORD);
        config.setMaximumPoolSize(5);
        config.setPoolName("warmup-pool");

        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            try (Connection warmup = dataSource.getConnection()) {
                // establishes the pool's real connections before we start timing
            }

            long start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                try (Connection conn = dataSource.getConnection()) {
                    // immediately close - returns to the pool, does NOT close the socket
                }
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            System.out.println("  " + ITERATIONS + " pooled connections (borrow+return each time): "
                    + elapsedMs + "ms total, " + (elapsedMs / (double) ITERATIONS) + "ms average per connection");
        }
    }

    private static void demonstratePoolExhaustion() throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername(USER);
        config.setPassword(PASSWORD);
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(2000);
        config.setPoolName("small-pool");

        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            try (Connection conn1 = dataSource.getConnection();
                 Connection conn2 = dataSource.getConnection()) {

                System.out.println("  Both of the pool's 2 connections are now checked out and held open.");
                System.out.println("  Attempting a 3rd getConnection() with connectionTimeout=2000ms...");

                long start = System.nanoTime();
                try (Connection conn3 = dataSource.getConnection()) {
                    System.out.println("  Unexpected: 3rd connection was acquired.");
                } catch (SQLException e) {
                    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    System.out.println("  Failed after " + elapsedMs + "ms: " + e.getClass().getSimpleName() + " - " + e.getMessage());
                }
            }
        }
    }

    private static void demonstrateCloseReturnsToPool() throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername(USER);
        config.setPassword(PASSWORD);
        config.setMaximumPoolSize(3);
        config.setPoolName("inspect-pool");

        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            HikariPoolMXBean poolStats = dataSource.getHikariPoolMXBean();

            Connection conn = dataSource.getConnection();
            System.out.println("  After borrowing 1 connection: active=" + poolStats.getActiveConnections()
                    + " idle=" + poolStats.getIdleConnections() + " total=" + poolStats.getTotalConnections());

            conn.close();
            System.out.println("  After calling close() on it: active=" + poolStats.getActiveConnections()
                    + " idle=" + poolStats.getIdleConnections() + " total=" + poolStats.getTotalConnections());
        }
    }
}