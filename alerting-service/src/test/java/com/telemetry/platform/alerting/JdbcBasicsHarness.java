package com.telemetry.platform.alerting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcBasicsHarness {

    private static final String URL = "jdbc:postgresql://localhost:5433/telemetry";
    private static final String USER = "telemetry_app";
    private static final String PASSWORD = "localdevpassword";

    public static void main(String[] args) throws SQLException {

        System.out.println("=== Connecting to TimescaleDB via raw JDBC (Connection -> Statement -> ResultSet) ===");

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, sensor_id, severity, z_score FROM anomaly_alerts WHERE severity = 'CRITICAL' ORDER BY id")) {

            System.out.println("Connection open. Cursor is positioned BEFORE the first row - calling next() to advance it.");

            int rowCount = 0;
            while (rs.next()) {
                long id = rs.getLong("id");
                String sensorId = rs.getString("sensor_id");
                String severity = rs.getString("severity");
                double zScore = rs.getDouble("z_score");
                System.out.printf("  id=%d sensor=%s severity=%s z=%.2f%n", id, sensorId, severity, zScore);
                rowCount++;
            }

            System.out.println("Rows read: " + rowCount);
            System.out.println("(Note: this Statement is hardcoded and NOT parameterized - that gap is deliberate, see Step 3.)");

        }

        System.out.println("try-with-resources closed ResultSet, then Statement, then Connection, in that order.");
    }
}