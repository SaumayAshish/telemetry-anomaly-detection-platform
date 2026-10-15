package com.telemetry.platform.api.alert;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.hibernate.Hibernate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Runs the repository against a real PostgreSQL migrated by the db-migrations module, with
 * ddl-auto=validate from application.properties, so entity mappings that drift from the
 * schema fail here and not in production. Rows are inserted through JDBC inside the test
 * transaction, which Spring rolls back after each test.
 */
@DataJpaTest(properties = "spring.test.database.replace=none")
class AnomalyAlertRepositoryTest {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16");

    static {
        POSTGRES.start();
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .load()
                .migrate();
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    private static final Instant T1 = Instant.parse("2026-10-14T10:00:00Z");
    private static final Instant T2 = Instant.parse("2026-10-14T11:00:00Z");
    private static final Instant T3 = Instant.parse("2026-10-14T12:00:00Z");

    @Autowired
    private AnomalyAlertRepository repository;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private Environment environment;

    private JdbcTemplate jdbc;

    @BeforeEach
    void seedSensors() {
        jdbc = new JdbcTemplate(dataSource);
        sensor("S-A");
        sensor("S-B");
    }

    @Test
    void schemaIsValidatedAgainstTheMigratedDatabase() {
        assertThat(environment.getProperty("spring.jpa.hibernate.ddl-auto")).isEqualTo("validate");
    }

    @Test
    void mapsEveryColumnOfTheMigratedTable() {
        alert("S-A", "HIGH", T1);

        List<AnomalyAlert> all = repository.findAllWithSensor();

        assertThat(all).hasSize(1);
        AnomalyAlert alert = all.get(0);
        assertThat(alert.getSensorId()).isEqualTo("S-A");
        assertThat(alert.getReadingTimestamp()).isEqualTo(T1);
        assertThat(alert.getValue()).isEqualTo(9.5);
        assertThat(alert.getBaselineMean()).isEqualTo(2.0);
        assertThat(alert.getBaselineStdDev()).isEqualTo(1.5);
        assertThat(alert.getZScore()).isEqualTo(5.0);
        assertThat(alert.getConsecutiveAnomalies()).isEqualTo(3L);
        assertThat(alert.getSeverity()).isEqualTo("HIGH");
        assertThat(alert.getIngestedAt()).isNotNull();
        assertThat(Hibernate.isInitialized(alert.getSensor())).isTrue();
    }

    @Test
    void findBySensorIdReturnsOnlyThatSensorsAlertsWithTheSensorLoaded() {
        alert("S-A", "HIGH", T1);
        alert("S-A", "LOW", T2);
        alert("S-B", "HIGH", T3);

        List<AnomalyAlert> found = repository.findBySensorId("S-A");

        assertThat(found).hasSize(2);
        assertThat(found).allSatisfy(a -> {
            assertThat(a.getSensorId()).isEqualTo("S-A");
            assertThat(Hibernate.isInitialized(a.getSensor())).isTrue();
        });
    }

    @Test
    void findBySensorIdAndSeverityAppliesBothFilters() {
        alert("S-A", "HIGH", T1);
        alert("S-A", "LOW", T2);
        alert("S-B", "HIGH", T3);

        List<AnomalyAlert> found = repository.findBySensorIdAndSeverity("S-A", "HIGH");

        assertThat(found).extracting(AnomalyAlert::getReadingTimestamp).containsExactly(T1);
    }

    @Test
    void findByReadingTimestampBetweenIncludesBothEnds() {
        alert("S-A", "HIGH", T1);
        alert("S-A", "HIGH", T2);
        alert("S-A", "HIGH", T3);

        List<AnomalyAlert> found = repository.findByReadingTimestampBetween(T1, T2);

        assertThat(found)
                .extracting(AnomalyAlert::getReadingTimestamp)
                .containsExactlyInAnyOrder(T1, T2);
    }

    private void sensor(String sensorId) {
        jdbc.update(
                "INSERT INTO sensors (sensor_id, location, install_date, model) "
                        + "VALUES (?, 'lab', DATE '2026-01-01', 'm1')",
                sensorId);
    }

    private void alert(String sensorId, String severity, Instant readingTime) {
        jdbc.update(
                "INSERT INTO anomaly_alerts (sensor_id, reading_timestamp, value, baseline_mean, "
                        + "baseline_std_dev, z_score, consecutive_anomalies, severity) "
                        + "VALUES (?, ?, 9.5, 2.0, 1.5, 5.0, 3, ?)",
                sensorId, Timestamp.from(readingTime), severity);
    }
}