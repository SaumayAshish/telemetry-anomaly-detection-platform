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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
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
    private static final Pageable PAGE = PageRequest.of(0, 50);

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

        List<AnomalyAlert> all = repository.findAllWithSensor(PAGE).getContent();

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

        List<AnomalyAlert> found = repository.findBySensorId("S-A", PAGE).getContent();

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

        List<AnomalyAlert> found = repository.findBySensorIdAndSeverity("S-A", "HIGH", PAGE).getContent();

        assertThat(found).extracting(AnomalyAlert::getReadingTimestamp).containsExactly(T1);
    }

    @Test
    void findByReadingTimestampBetweenIncludesBothEnds() {
        alert("S-A", "HIGH", T1);
        alert("S-A", "HIGH", T2);
        alert("S-A", "HIGH", T3);

        List<AnomalyAlert> found = repository.findByReadingTimestampBetween(T1, T2, PAGE).getContent();

        assertThat(found)
                .extracting(AnomalyAlert::getReadingTimestamp)
                .containsExactlyInAnyOrder(T1, T2);
    }

    @Test
    void pagesAreOrderedNewestFirstWithTheIdAsTiebreakerAndNeverOverlap() {
        for (int i = 0; i < 7; i++) {
            alert("S-A", "HIGH", T1.plusSeconds(i));
        }
        for (int i = 0; i < 5; i++) {
            alert("S-A", "HIGH", T3);
        }
        Sort newestFirst = Sort.by(Sort.Order.desc("readingTimestamp"), Sort.Order.desc("id"));

        Page<AnomalyAlert> first = repository.findBySensorId("S-A", PageRequest.of(0, 5, newestFirst));
        Page<AnomalyAlert> second = repository.findBySensorId("S-A", PageRequest.of(1, 5, newestFirst));
        Page<AnomalyAlert> third = repository.findBySensorId("S-A", PageRequest.of(2, 5, newestFirst));

        assertThat(first.getTotalElements()).isEqualTo(12);
        assertThat(first.getTotalPages()).isEqualTo(3);
        assertThat(first.getContent()).hasSize(5);
        assertThat(second.getContent()).hasSize(5);
        assertThat(third.getContent()).hasSize(2);

        List<AnomalyAlert> all = new java.util.ArrayList<>(first.getContent());
        all.addAll(second.getContent());
        all.addAll(third.getContent());
        assertThat(all).extracting(AnomalyAlert::getId).doesNotHaveDuplicates();
        assertThat(all).extracting(AnomalyAlert::getReadingTimestamp)
                .isSortedAccordingTo(java.util.Comparator.reverseOrder());
        assertThat(first.getContent()).extracting(AnomalyAlert::getId)
                .isSortedAccordingTo(java.util.Comparator.reverseOrder());
        assertThat(all).allSatisfy(a -> assertThat(Hibernate.isInitialized(a.getSensor())).isTrue());
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