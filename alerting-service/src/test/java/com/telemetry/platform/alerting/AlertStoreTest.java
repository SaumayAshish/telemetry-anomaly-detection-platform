package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class AlertStoreTest {

    private static final Instant READING_TIME = Instant.parse("2026-10-12T10:15:30Z");

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16");

    @BeforeAll
    static void migrate() {
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .load()
                .migrate();
    }

    @BeforeEach
    void emptyTables() throws SQLException {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("TRUNCATE alert_dlq, anomaly_alerts, sensors RESTART IDENTITY");
        }
    }

    @Test
    void persistStoresEveryValueInTheRightColumn() throws SQLException {
        insertSensor("s-1");

        boolean inserted = persist(event("s-1"), 3, 42L);

        assertThat(inserted).isTrue();
        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM anomaly_alerts")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("sensor_id")).isEqualTo("s-1");
            assertThat(rs.getObject("reading_timestamp", OffsetDateTime.class).toInstant())
                    .isEqualTo(READING_TIME);
            assertThat(rs.getDouble("value")).isEqualTo(9.5);
            assertThat(rs.getDouble("baseline_mean")).isEqualTo(2.0);
            assertThat(rs.getDouble("baseline_std_dev")).isEqualTo(1.5);
            assertThat(rs.getDouble("z_score")).isEqualTo(5.0);
            assertThat(rs.getLong("consecutive_anomalies")).isEqualTo(3L);
            assertThat(rs.getString("severity")).isEqualTo("HIGH");
            assertThat(rs.getInt("kafka_partition")).isEqualTo(3);
            assertThat(rs.getLong("kafka_offset")).isEqualTo(42L);
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void duplicateDeliveryReturnsFalseAndKeepsOneRow() throws SQLException {
        insertSensor("s-1");

        boolean first = persist(event("s-1"), 0, 42L);
        boolean second = persist(event("s-1"), 0, 42L);

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(count("anomaly_alerts")).isEqualTo(1);
    }

    @Test
    void sameOffsetOnDifferentPartitionsAreDifferentAlerts() throws SQLException {
        insertSensor("s-1");

        assertThat(persist(event("s-1"), 0, 7L)).isTrue();
        assertThat(persist(event("s-1"), 1, 7L)).isTrue();

        assertThat(count("anomaly_alerts")).isEqualTo(2);
    }

    @Test
    void alertForUnknownSensorFailsWithNonRetryableForeignKeyViolation() {
        assertThatThrownBy(() -> persist(event("no-such-sensor"), 0, 1L))
                .isInstanceOfSatisfying(SQLException.class, e -> {
                    assertThat(e.getSQLState()).isEqualTo("23503");
                    assertThat(SqlFailureClassifier.isRetryable(e)).isFalse();
                });
    }

    @Test
    void deadLetterStoresTheFailureDetails() throws SQLException {
        boolean stored = deadLetter("s-1", "alerts", 2, 7L, "23503", "fk violated");

        assertThat(stored).isTrue();
        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM alert_dlq")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("kafka_topic")).isEqualTo("alerts");
            assertThat(rs.getInt("kafka_partition")).isEqualTo(2);
            assertThat(rs.getLong("kafka_offset")).isEqualTo(7L);
            assertThat(rs.getString("sensor_id")).isEqualTo("s-1");
            assertThat(rs.getString("payload")).isEqualTo("{\"raw\":true}");
            assertThat(rs.getString("sql_state")).isEqualTo("23503");
            assertThat(rs.getString("error_message")).isEqualTo("fk violated");
            assertThat(rs.getTimestamp("failed_at")).isNotNull();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void duplicateDeadLetterReturnsFalseAndKeepsOneRow() throws SQLException {
        boolean first = deadLetter("s-1", "alerts", 0, 7L, "VALIDATION", "bad");
        boolean second = deadLetter("s-1", "alerts", 0, 7L, "VALIDATION", "bad");

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(count("alert_dlq")).isEqualTo(1);
    }

    // The consumer dead-letters unparseable messages with this placeholder sensor id,
    // which must not be rejected for not being a registered sensor.
    @Test
    void deadLetterAcceptsTheUnknownSensorPlaceholder() throws SQLException {
        boolean stored = deadLetter("UNKNOWN", "alerts", 0, 1L, "DESERIALIZATION", "bad json");

        assertThat(stored).isTrue();
        assertThat(count("alert_dlq")).isEqualTo(1);
    }

    private static AnomalyEvent event(String sensorId) {
        return new AnomalyEvent(sensorId, READING_TIME, 9.5, 2.0, 1.5, 5.0, 3L, AnomalySeverity.HIGH);
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

    private static boolean persist(AnomalyEvent event, int partition, long offset) throws SQLException {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(AlertStore.INSERT_ALERT_SQL)) {
            return AlertStore.persistAlert(ps, event, partition, offset);
        }
    }

    private static boolean deadLetter(String sensorId, String topic, int partition, long offset,
                                      String sqlState, String message) throws SQLException {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(AlertStore.INSERT_ALERT_DLQ_SQL)) {
            return AlertStore.deadLetter(
                    ps, sensorId, "{\"raw\":true}", topic, partition, offset, sqlState, message);
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