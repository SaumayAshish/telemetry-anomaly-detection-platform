package com.telemetry.platform.alerting;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.telemetry.platform.events.AnomalyEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class AlertConsumer {

    private static final String ANOMALY_TOPIC = "telemetry.sensor.anomalies.v1";
    private static final String GROUP_ID = "alerting-service-group";

    // Hardcoded for local-dev simplicity.
    // Not production practice - these belong in environment variables
    // or a secrets manager. Revisit before Phase 10.
    private static final String DB_URL =
            "jdbc:postgresql://localhost:5433/telemetry";
    private static final String DB_USER = "telemetry_app";
    private static final String DB_PASSWORD = "localdevpassword";

    private static final long INITIAL_BACKOFF_MS = 1_000;
    private static final int BACKOFF_MULTIPLIER = 2;
    private static final long MAX_BACKOFF_MS = 30_000;

    private static final Duration POLL_TIMEOUT =
            Duration.ofSeconds(1);

    /*
     * SQLState classes considered retryable:
     *
     * 08 - Connection Exception
     * 53 - Insufficient Resources
     * 57 - Operator Intervention
     */
    private static final Set<String> RETRYABLE_SQLSTATE_CLASSES =
            Set.of("08", "53", "57");

    private static final String INSERT_ALERT_SQL = """
            INSERT INTO anomaly_alerts
                (sensor_id, reading_timestamp, value, baseline_mean, baseline_std_dev, z_score, consecutive_anomalies, severity, kafka_partition, kafka_offset)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
            """;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.disable(
                SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
        );
        OBJECT_MAPPER.configure(
                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false
        );
    }

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                GROUP_ID
        );
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );
        props.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        Deserializer<AnomalyEvent> valueDeserializer =
                (topic, bytes) -> {
                    try {
                        return OBJECT_MAPPER.readValue(
                                bytes,
                                AnomalyEvent.class
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to deserialize AnomalyEvent",
                                e
                        );
                    }
                };

        KafkaConsumer<String, AnomalyEvent> consumer =
                new KafkaConsumer<>(
                        props,
                        new StringDeserializer(),
                        valueDeserializer
                );

        consumer.subscribe(List.of(ANOMALY_TOPIC));

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.out.println(
                            "Shutdown signal received - waking up consumer..."
                    );

                    consumer.wakeup();

                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                })
        );

        Connection dbConnection = null;
        PreparedStatement insertAlertStatement = null;

        try {
            /*
             * Initial DB connection.
             *
             * If DB is unavailable when the consumer starts,
             * connectWithRetry() keeps retrying with exponential backoff
             * until the database becomes available.
             */
            dbConnection = connectWithRetry();

            insertAlertStatement =
                    dbConnection.prepareStatement(INSERT_ALERT_SQL);

            while (true) {

                ConsumerRecords<String, AnomalyEvent> records =
                        consumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<String, AnomalyEvent> record
                        : records) {

                    AnomalyEvent event = record.value();
                    boolean persisted = false;
                    boolean wasNewInsert = false;

                    /*
                     * Keep retrying until the alert is persisted,
                     * or we determine the failure is permanent.
                     */
                    while (!persisted) {

                        try {

                            if (!dbConnection.isValid(2)) {

                                closeQuietly(insertAlertStatement);
                                closeQuietly(dbConnection);

                                dbConnection = connectWithRetry();

                                insertAlertStatement =
                                        dbConnection.prepareStatement(
                                                INSERT_ALERT_SQL
                                        );
                            }

                            wasNewInsert = persistAlert(
                                    insertAlertStatement,
                                    event,
                                    record.partition(),
                                    record.offset()
                            );

                            persisted = true;

                        } catch (SQLException e) {

                            if (!isRetryable(e)) {
                                System.err.println(
                                        "PERMANENT failure persisting alert for "
                                                + event.sensorId()
                                                + " (SQLState=" + e.getSQLState()
                                                + "): " + e.getMessage()
                                                + ". Skipping this alert — dead-letter"
                                                + " handling not yet implemented (Phase 6 Step 5)."
                                );
                                break;
                            }

                            System.err.println(
                                    "Transient DB error (SQLState=" + e.getSQLState()
                                            + ") persisting alert for " + event.sensorId()
                                            + ": " + e.getMessage() + ". Reconnecting..."
                            );

                            closeQuietly(insertAlertStatement);
                            closeQuietly(dbConnection);

                            dbConnection = connectWithRetry();

                            insertAlertStatement =
                                    dbConnection.prepareStatement(
                                            INSERT_ALERT_SQL
                                    );
                        }
                    }

                    if (persisted && wasNewInsert) {
                        System.out.println(
                                "ALERT | sensor=" + event.sensorId()
                                        + " | severity=" + event.severity()
                                        + " | value=" + event.value()
                                        + " | baselineMean=" + event.baselineMean()
                                        + " | baselineStdDev=" + event.baselineStdDev()
                                        + " | zScore=" + event.zScore()
                                        + " | consecutiveAnomalies="
                                        + event.consecutiveAnomalies()
                                        + " | at=" + event.readingTimestamp()
                                        + " | partition=" + record.partition()
                                        + " | offset=" + record.offset()
                        );
                    } else if (persisted) {
                        System.out.println(
                                "DUPLICATE | sensor=" + event.sensorId()
                                        + " | partition=" + record.partition()
                                        + " | offset=" + record.offset()
                                        + " | already persisted — skipping (idempotent no-op)."
                        );
                    }
                }

                /*
                 * Commit only after the entire polled batch has been
                 * successfully persisted.
                 */
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (WakeupException e) {

            System.out.println(
                    "Consumer loop interrupted for shutdown, as expected."
            );

        } catch (SQLException e) {

            throw new RuntimeException(
                    "Database error while persisting alerts",
                    e
            );

        } finally {

            closeQuietly(insertAlertStatement);
            closeQuietly(dbConnection);

            consumer.close();

            System.out.println(
                    "Consumer closed cleanly."
            );
        }
    }

    private static boolean persistAlert(
            PreparedStatement insertAlertStatement,
            AnomalyEvent event,
            int kafkaPartition,
            long kafkaOffset
    ) throws SQLException {

        insertAlertStatement.setString(1, event.sensorId());
        insertAlertStatement.setTimestamp(2, Timestamp.from(event.readingTimestamp()));
        insertAlertStatement.setDouble(3, event.value());
        insertAlertStatement.setDouble(4, event.baselineMean());
        insertAlertStatement.setDouble(5, event.baselineStdDev());
        insertAlertStatement.setDouble(6, event.zScore());
        insertAlertStatement.setLong(7, event.consecutiveAnomalies());
        insertAlertStatement.setString(8, event.severity().name());
        insertAlertStatement.setInt(9, kafkaPartition);
        insertAlertStatement.setLong(10, kafkaOffset);

        int rowsInserted = insertAlertStatement.executeUpdate();

        if (rowsInserted > 1) {
            throw new SQLException(
                    "Expected to insert at most 1 row, but inserted " + rowsInserted
            );
        }

        /*
         * true  = this was a new alert, actually inserted.
         * false = ON CONFLICT DO NOTHING fired: this exact
         *         (kafka_partition, kafka_offset) was already
         *         persisted by an earlier delivery of this record.
         */
        return rowsInserted == 1;
    }

    private static boolean isRetryable(SQLException e) {

        String sqlState = e.getSQLState();

        if (sqlState == null || sqlState.length() < 2) {
            return false;
        }

        return RETRYABLE_SQLSTATE_CLASSES.contains(sqlState.substring(0, 2));
    }

    private static void closeQuietly(AutoCloseable resource) {

        if (resource == null) {
            return;
        }

        try {
            resource.close();
        } catch (Exception e) {
            System.err.println(
                    "Error closing resource: "
                            + e.getMessage()
            );
        }
    }

    private static Connection connectWithRetry()
            throws InterruptedException {

        long backoffMs = INITIAL_BACKOFF_MS;

        while (true) {

            try {

                Connection connection =
                        DriverManager.getConnection(
                                DB_URL,
                                DB_USER,
                                DB_PASSWORD
                        );

                System.out.println(
                        "Connected to database."
                );

                return connection;

            } catch (SQLException e) {

                System.err.println(
                        "DB connection failed: "
                                + e.getMessage()
                                + ". Retrying in "
                                + backoffMs
                                + "ms..."
                );

                Thread.sleep(backoffMs);

                /*
                 * Exponential backoff:
                 *
                 * 1s → 2s → 4s → 8s → 16s → 30s → 30s...
                 */
                backoffMs = Math.min(
                        backoffMs * BACKOFF_MULTIPLIER,
                        MAX_BACKOFF_MS
                );
            }
        }
    }
}