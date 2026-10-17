package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;

import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Drives the real AlertConsumer.run wiring (KafkaConsumer, BatchLoop, RecordProcessor,
 * AlertWriter) against a real Kafka broker and a real, migrated PostgreSQL.
 */
@Testcontainers
class AlertConsumerEndToEndTest {

    private static final String TOPIC = "telemetry.sensor.anomalies.v1";
    private static final String GROUP = "alerting-service-group";
    private static final String SENSOR = "S-E2E-1";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16");

    // Not static: every test gets a fresh broker, so topic contents and committed offsets
    // from one test can never leak into another.
    @Container
    KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    private final AtomicReference<Throwable> consumerFailure = new AtomicReference<>();

    @BeforeAll
    static void migrate() {
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .load()
                .migrate();
    }

    @BeforeEach
    void prepare() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("TRUNCATE alert_dlq, anomaly_alerts, sensors RESTART IDENTITY");
            s.execute("INSERT INTO sensors (sensor_id, location, install_date, model) "
                    + "VALUES ('" + SENSOR + "', 'lab', DATE '2026-01-01', 'm1')");
        }

        try (Admin admin = Admin.create(Map.<String, Object>of(
                "bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
    }

    @AfterEach
    void clearStopRequest() {
        AlertConsumer.stop();
        AlertConsumer.resetForNextRun();
    }

    @Test
    @Timeout(180)
    void aValidEventIsPersistedWithItsKafkaPosition() throws Exception {
        send(validEventJson());

        Thread consumer = startConsumer();
        try {
            assertThat(awaitCount("anomaly_alerts", 1)).isEqualTo(1);
        } finally {
            stopConsumer(consumer);
        }

        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT sensor_id, severity, kafka_partition, kafka_offset FROM anomaly_alerts")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("sensor_id")).isEqualTo(SENSOR);
            assertThat(rs.getString("severity")).isEqualTo("HIGH");
            assertThat(rs.getInt("kafka_partition")).isZero();
            assertThat(rs.getLong("kafka_offset")).isZero();
        }
        assertThat(count("alert_dlq")).isZero();
    }

    @Test
    @Timeout(180)
    void aMalformedRecordIsDeadLetteredAndDoesNotBlockTheNextOne() throws Exception {
        send("{not json");
        send(validEventJson());

        Thread consumer = startConsumer();
        try {
            assertThat(awaitCount("anomaly_alerts", 1)).isEqualTo(1);
            assertThat(awaitCount("alert_dlq", 1)).isEqualTo(1);
        } finally {
            stopConsumer(consumer);
        }

        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT sql_state, payload, kafka_offset FROM alert_dlq")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("sql_state")).isEqualTo("DESERIALIZATION");
            assertThat(rs.getString("payload")).isEqualTo("{not json");
            assertThat(rs.getLong("kafka_offset")).isZero();
        }
        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT kafka_offset FROM anomaly_alerts")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("kafka_offset")).isEqualTo(1L);
        }
    }

    @Test
    @Timeout(240)
    void aStoppedConsumerCommitsItsBatchAndARestartContinuesWithoutDuplicates() throws Exception {
        send(validEventJson());

        Thread first = startConsumer();
        try {
            assertThat(awaitCount("anomaly_alerts", 1)).isEqualTo(1);
        } finally {
            stopConsumer(first);
        }

        assertThat(committedOffset()).as("offset committed by the first run").isEqualTo(1L);

        AlertConsumer.resetForNextRun();
        send(validEventJson());

        Thread second = startConsumer();
        try {
            assertThat(awaitCount("anomaly_alerts", 2)).isEqualTo(2);
        } finally {
            stopConsumer(second);
        }

        assertThat(count("anomaly_alerts")).isEqualTo(2);
        assertThat(committedOffset()).as("offset committed by the second run").isEqualTo(2L);
    }

    @Test
    @Timeout(180)
    void theMetricsEndpointCountsWhatTheConsumerDid() throws Exception {
        send("{not json");
        send(validEventJson());

        Thread consumer = startConsumer(0);
        int boundPort = -1;
        try {
            assertThat(awaitCount("anomaly_alerts", 1)).isEqualTo(1);
            assertThat(awaitCount("alert_dlq", 1)).isEqualTo(1);

            String scrape = awaitScrape(
                    "alerting_alerts_persisted_total{severity=\"HIGH\"} 1.0",
                    "alerting_dead_letters_total{reason=\"DESERIALIZATION\"} 1.0");
            assertThat(scrape)
                    .contains("alerting_alerts_persisted_total{severity=\"HIGH\"} 1.0")
                    .contains("alerting_dead_letters_total{reason=\"DESERIALIZATION\"} 1.0");
            boundPort = AlertConsumer.metricsPort();
        } finally {
            stopConsumer(consumer);
        }

        assertThat(AlertConsumer.metricsPort()).isEqualTo(-1);
        assertThat(awaitNothingListensOn(boundPort)).isTrue();
    }

    // Once run returns, the metrics server must be closed and its port must refuse connections.
    private static boolean awaitNothingListensOn(int port) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                HttpRequest request = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + "/metrics")).build();
                client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException refused) {
                return true;
            }
            Thread.sleep(200);
        }
        return false;
    }

    // The counters move just after the row is written, so poll until they show up.
    private static String awaitScrape(String... expectedLines) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        String last = "";
        while (System.nanoTime() < deadline) {
            int port = AlertConsumer.metricsPort();
            if (port > 0) {
                try {
                    HttpRequest request = HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + port + "/metrics")).build();
                    last = client.send(request, HttpResponse.BodyHandlers.ofString()).body();
                    if (Arrays.stream(expectedLines).allMatch(last::contains)) {
                        return last;
                    }
                } catch (IOException notUpYet) {
                    // The server may still be starting; try again.
                }
            }
            Thread.sleep(200);
        }
        return last;
    }

    private Thread startConsumer() {
        return startConsumer(AlertConsumerConfig.METRICS_DISABLED);
    }

    private Thread startConsumer(int metricsPort) {
        AlertConsumer.resetForNextRun();

        AlertConsumerConfig config = new AlertConsumerConfig(
                kafka.getBootstrapServers(),
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword(),
                AlertConsumerConfig.DEFAULT_DLQ_FALLBACK_PATH,
                metricsPort);

        Thread thread = new Thread(() -> {
            try {
                AlertConsumer.run(config);
            } catch (Throwable failure) {
                consumerFailure.set(failure);
            }
        }, "consumer-under-test");
        thread.start();
        return thread;
    }

    private void stopConsumer(Thread thread) throws InterruptedException {
        AlertConsumer.stop();
        thread.join(30_000);

        assertThat(thread.isAlive()).as("consumer thread stopped").isFalse();
        assertThat(consumerFailure.get()).as("consumer failure").isNull();
    }

    private void send(String value) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC, SENSOR, value)).get();
        }
    }

    private static String validEventJson() throws Exception {
        AnomalyEvent event = new AnomalyEvent(
                SENSOR, Instant.parse("2026-10-14T10:15:30Z"), 9.5, 2.0, 1.5, 5.0, 3L,
                AnomalySeverity.HIGH);
        return RecordProcessor.newObjectMapper().writeValueAsString(event);
    }

    private long committedOffset() throws Exception {
        try (Admin admin = Admin.create(Map.<String, Object>of(
                "bootstrap.servers", kafka.getBootstrapServers()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin
                    .listConsumerGroupOffsets(GROUP)
                    .partitionsToOffsetAndMetadata()
                    .get();
            OffsetAndMetadata committed = offsets.get(new TopicPartition(TOPIC, 0));
            return committed == null ? -1L : committed.offset();
        }
    }

    private static int awaitCount(String table, int expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        int last = -1;
        while (System.nanoTime() < deadline) {
            last = count(table);
            if (last >= expected) {
                return last;
            }
            Thread.sleep(200);
        }
        return last;
    }

    private static int count(String table) throws SQLException {
        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
