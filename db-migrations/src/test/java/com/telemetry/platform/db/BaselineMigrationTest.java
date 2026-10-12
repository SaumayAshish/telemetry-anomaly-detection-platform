package com.telemetry.platform.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class BaselineMigrationTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16");

    private static Flyway flyway;

    @BeforeAll
    static void migrate() {
        flyway = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .load();
        flyway.migrate();
    }

    @BeforeEach
    void emptyTables() throws SQLException {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("TRUNCATE alert_dlq, anomaly_alerts, sensors RESTART IDENTITY");
        }
    }

    @Test
    void baselineCreatesExactlyTheExpectedTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        assertThat(tables)
                .containsExactlyInAnyOrder("sensors", "anomaly_alerts", "alert_dlq", "flyway_schema_history");
    }

    @Test
    void migratingAgainAppliesNothing() {
        MigrateResult result = flyway.migrate();
        assertThat(result.migrationsExecuted).isZero();
    }

    @Test
    void duplicateKafkaPositionInsertsOnlyOneAlert() throws SQLException {
        insertSensor("s-1");

        int first = insertAlert("s-1", "HIGH", 0, 42L);
        int second = insertAlert("s-1", "HIGH", 0, 42L);

        assertThat(first).isEqualTo(1);
        assertThat(second).isZero();
        assertThat(count("anomaly_alerts")).isEqualTo(1);
    }

    @Test
    void unknownSeverityIsRejectedByTheCheckConstraint() throws SQLException {
        insertSensor("s-1");

        assertThatThrownBy(() -> insertAlert("s-1", "BANANA", 0, 1L))
                .isInstanceOfSatisfying(SQLException.class,
                        e -> assertThat(e.getSQLState()).isEqualTo("23514"));
    }

    @Test
    void alertForUnknownSensorIsRejectedByTheForeignKey() {
        assertThatThrownBy(() -> insertAlert("no-such-sensor", "LOW", 0, 1L))
                .isInstanceOfSatisfying(SQLException.class,
                        e -> assertThat(e.getSQLState()).isEqualTo("23503"));
    }

    @Test
    void duplicateDeadLetterPositionInsertsOnlyOneRow() throws SQLException {
        int first = insertDeadLetter("alerts", 0, 7L);
        int second = insertDeadLetter("alerts", 0, 7L);

        assertThat(first).isEqualTo(1);
        assertThat(second).isZero();
        assertThat(count("alert_dlq")).isEqualTo(1);
    }

    @Test
    void idColumnsAreGeneratedByTheDatabase() throws SQLException {
        insertSensor("s-1");
        insertAlert("s-1", "LOW", 0, 1L);

        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM anomaly_alerts")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(1L);
        }
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static void insertSensor(String sensorId) throws SQLException {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO sensors (sensor_id, location, install_date, model) "
                             + "VALUES (?, 'lab', current_date, 'M1')")) {
            ps.setString(1, sensorId);
            ps.executeUpdate();
        }
    }

    // Same ON CONFLICT clause as AlertConsumer, so this proves the production SQL has a constraint to hit.
    private static int insertAlert(String sensorId, String severity, int partition, long offset)
            throws SQLException {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO anomaly_alerts (sensor_id, reading_timestamp, value, baseline_mean, "
                             + "baseline_std_dev, z_score, consecutive_anomalies, severity, "
                             + "kafka_partition, kafka_offset) "
                             + "VALUES (?, now(), 1.0, 0.0, 1.0, 5.0, 1, ?, ?, ?) "
                             + "ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING")) {
            ps.setString(1, sensorId);
            ps.setString(2, severity);
            ps.setInt(3, partition);
            ps.setLong(4, offset);
            return ps.executeUpdate();
        }
    }

    private static int insertDeadLetter(String topic, int partition, long offset) throws SQLException {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO alert_dlq (kafka_topic, kafka_partition, kafka_offset, payload) "
                             + "VALUES (?, ?, ?, '{}') "
                             + "ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING")) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setLong(3, offset);
            return ps.executeUpdate();
        }
    }

    private static int count(String table) throws SQLException {
        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }
}