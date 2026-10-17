package com.telemetry.platform.alerting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PreparedStatementHarness {

    private static final String URL = "jdbc:postgresql://localhost:5433/telemetry";
    private static final String USER = "telemetry_app";
    private static final String PASSWORD = "localdevpassword";

    public static void main(String[] args) throws SQLException {

        System.out.println("=== Section 1: the vulnerable version - string-concatenated Statement ===");
        System.out.println("Legitimate lookup for a sensor that does NOT exist, sensor_id = 'S-9999':");
        vulnerableLookupBySensorId("S-9999");

        System.out.println();
        System.out.println("Now the SAME lookup, but with a malicious string standing in for the sensor_id:");
        vulnerableLookupBySensorId("S-9999' OR '1'='1");

        System.out.println();
        System.out.println("=== Section 2: the fixed version - PreparedStatement with a bound parameter ===");
        System.out.println("Same non-existent sensor, via PreparedStatement:");
        safeLookupBySensorId("S-9999");

        System.out.println();
        System.out.println("Same malicious string, now passed as a BOUND PARAMETER instead of concatenated SQL text:");
        safeLookupBySensorId("S-9999' OR '1'='1");

        System.out.println();
        System.out.println("=== Section 3: debugging exercise - UNION-based injection ===");
        vulnerableLookupBySensorIdVerbose(
                "S-9999' UNION SELECT id, sensor_id, error_message FROM alert_dlq --"
        );
    }

    private static void vulnerableLookupBySensorId(String sensorId) throws SQLException {
        String sql =
                "SELECT id, sensor_id, severity " +
                        "FROM anomaly_alerts " +
                        "WHERE sensor_id = '" + sensorId + "'";

        System.out.println("  Actual SQL sent to the database: " + sql);

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            int count = 0;

            while (rs.next()) {
                count++;
            }

            System.out.println("  Rows returned: " + count);
        }
    }

    private static void safeLookupBySensorId(String sensorId) throws SQLException {
        String sql =
                "SELECT id, sensor_id, severity " +
                        "FROM anomaly_alerts " +
                        "WHERE sensor_id = ?";

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, sensorId);

            System.out.println(
                    "  Parameter bound (never concatenated into SQL text): "
                            + sensorId
            );

            try (ResultSet rs = stmt.executeQuery()) {
                int count = 0;

                while (rs.next()) {
                    count++;
                }

                System.out.println("  Rows returned: " + count);
            }
        }
    }

    private static void vulnerableLookupBySensorIdVerbose(String sensorId)
            throws SQLException {

        String sql =
                "SELECT id, sensor_id, severity " +
                        "FROM anomaly_alerts " +
                        "WHERE sensor_id = '" + sensorId + "'";

        System.out.println("  Actual SQL sent to the database: " + sql);

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            int count = 0;

            while (rs.next()) {
                System.out.printf(
                        "  row: col1=%s col2=%s col3=%s%n",
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3)
                );

                count++;
            }

            System.out.println("  Rows returned: " + count);
        }
    }
}