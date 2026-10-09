package com.telemetry.platform.alerting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.telemetry.platform.events.AnomalyEvent;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class AlertConsumer {

    private static final String ANOMALY_TOPIC = "telemetry.sensor.anomalies.v1";
    private static final String GROUP_ID = "alerting-service-group";
    private static final Logger log = LoggerFactory.getLogger(AlertConsumer.class);

    private static final long INITIAL_BACKOFF_MS = 1_000;
    private static final int BACKOFF_MULTIPLIER = 2;
    private static final long MAX_BACKOFF_MS = 30_000;
    private static AlertConsumerConfig config;

    // How often connectWithRetry()'s backoff sleep checks for a shutdown
    // signal. Smaller = faster shutdown response, more wakeups. 200ms is a
    // reasonable balance - worst-case shutdown latency during a reconnect
    // backoff drops from up to MAX_BACKOFF_MS (30s) to ~200ms.
    private static final long SHUTDOWN_CHECK_INTERVAL_MS = 200;

    // Set by the shutdown hook thread, read by the main thread inside
    // sleepUnlessShuttingDown(). Declared volatile so the main thread is
    // guaranteed to observe the write made by the shutdown hook thread -
    // without it, the JIT/CPU would be free to cache a stale read and the
    // main thread might never notice the flag changed.
    private static volatile boolean shuttingDown = false;

    private static final ValidatorFactory VALIDATOR_FACTORY =
            Validation.buildDefaultValidatorFactory();
    private static final Validator VALIDATOR =
            VALIDATOR_FACTORY.getValidator();

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

    //
    // This is the last-resort sink for failures that survive a non-retryable
    // DLQ-table write failure: if even this file write fails, there is no
    // further fallback and the consumer is allowed to crash loudly.
    private static final Path DLQ_FALLBACK_FILE = Paths.get("dlq-fallback.jsonl");

    private static final String INSERT_ALERT_SQL = """
            INSERT INTO anomaly_alerts
                (sensor_id, reading_timestamp, value, baseline_mean, baseline_std_dev, z_score, consecutive_anomalies, severity, kafka_partition, kafka_offset)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
            """;

    private static final String INSERT_ALERT_DLQ_SQL = """
            INSERT INTO alert_dlq
                (kafka_topic, kafka_partition, kafka_offset, sensor_id, payload, sql_state, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
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

    public static void main(String[] args) {

        config = AlertConsumerConfig.fromEnvironment();
        log.info("Configuration loaded: {}", config);

        Properties props = new Properties();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.kafkaBootstrapServers()
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

        /*
         * Both key and value are consumed as plain String. Kafka's client
         * calls the deserializer INSIDE consumer.poll() itself, before our
         * loop below even starts - an exception thrown there would escape
         * every try/catch in this method and crash the process (this is
         * exactly what happened with a Deserializer<AnomalyEvent> that let
         * Jackson exceptions propagate). StringDeserializer can't fail in
         * that way, so parsing AnomalyEvent out of the raw String is done
         * explicitly inside the loop instead, where it can be caught and
         * routed to the dead-letter path like any other failure.
         */
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(
                        props,
                        new StringDeserializer(),
                        new StringDeserializer()
                );

        consumer.subscribe(List.of(ANOMALY_TOPIC));

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    log.info("Shutdown signal received, waking up consumer");

                    /*
                     * Set BEFORE wakeup(), not after: sleepUnlessShuttingDown()
                     * checks this flag at the top of each 200ms increment, so
                     * it needs to already be true by the time the main thread
                     * next checks it, however it's currently blocked.
                     */
                    shuttingDown = true;

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
        PreparedStatement alertDlqStatement = null;

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

            alertDlqStatement =
                    dbConnection.prepareStatement(INSERT_ALERT_DLQ_SQL);

            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<String, String> record
                        : records) {

                    String rawPayload = record.value();
                    AnomalyEvent event;
                    boolean persisted = false;
                    boolean wasNewInsert = false;

                    /*
                     * Deserialization boundary.
                     *
                     * See the comment on the StringDeserializer setup
                     * above for why this is explicit rather than wired
                     * into Kafka's Deserializer plugin interface. A
                     * message that fails here never reached a valid
                     * AnomalyEvent, so there is no sensorId to report -
                     * it's dead-lettered as "UNKNOWN" and the raw text
                     * is preserved as-is for later inspection.
                     */
                    try {

                        event = OBJECT_MAPPER.readValue(
                                rawPayload,
                                AnomalyEvent.class
                        );

                        if (event == null) {
                            throw new IllegalArgumentException(
                                    "Payload deserialized to null (JSON null literal)"
                            );
                        }

                    } catch (JsonProcessingException | IllegalArgumentException deserializationFailure) {

                        log.warn("Deserialization failed topic={} partition={} offset={} exception={}",
                                record.topic(), record.partition(), record.offset(),
                                deserializationFailure.getClass().getSimpleName());

                        boolean deadLettered = false;

                        while (!deadLettered) {

                            try {

                                if (!dbConnection.isValid(2)) {

                                    closeQuietly(insertAlertStatement);
                                    closeQuietly(alertDlqStatement);
                                    closeQuietly(dbConnection);

                                    dbConnection = connectWithRetry();

                                    insertAlertStatement =
                                            dbConnection.prepareStatement(
                                                    INSERT_ALERT_SQL
                                            );

                                    alertDlqStatement =
                                            dbConnection.prepareStatement(
                                                    INSERT_ALERT_DLQ_SQL
                                            );
                                }

                                boolean wasNewDeadLetter = deadLetter(
                                        alertDlqStatement,
                                        "UNKNOWN",
                                        rawPayload,
                                        record.topic(),
                                        record.partition(),
                                        record.offset(),
                                        "DESERIALIZATION",
                                        deserializationFailure.getMessage()
                                );

                                logDeadLetterOutcome(wasNewDeadLetter, record, "UNKNOWN", "DESERIALIZATION");

                                deadLettered = true;

                            } catch (SQLException dlqEx) {

                                if (!isRetryable(dlqEx)) {

                                    writeFallbackRecord(
                                            "UNKNOWN",
                                            rawPayload,
                                            record.topic(),
                                            record.partition(),
                                            record.offset(),
                                            "DESERIALIZATION",
                                            deserializationFailure.getMessage(),
                                            dlqEx
                                    );

                                    deadLettered = true;

                                } else {

                                    System.err.println(
                                            "Transient DB error (SQLState="
                                                    + dlqEx.getSQLState()
                                                    + ") persisting to dead-letter table: "
                                                    + dlqEx.getMessage()
                                                    + ". Reconnecting..."
                                    );

                                    closeQuietly(insertAlertStatement);
                                    closeQuietly(alertDlqStatement);
                                    closeQuietly(dbConnection);

                                    dbConnection = connectWithRetry();

                                    insertAlertStatement =
                                            dbConnection.prepareStatement(
                                                    INSERT_ALERT_SQL
                                            );

                                    alertDlqStatement =
                                            dbConnection.prepareStatement(
                                                    INSERT_ALERT_DLQ_SQL
                                            );
                                }
                            }
                        }

                        /*
                         * This record could not even be parsed into an
                         * AnomalyEvent. It has either been dead-lettered
                         * or could not be captured. Either way, it must
                         * never reach Bean Validation or persistAlert().
                         */
                        continue;
                    }

                    /*
                     * Bean Validation boundary.
                     *
                     * AnomalyEvent was successfully deserialized (it's
                     * structurally valid JSON matching the record shape),
                     * but nothing has yet checked whether its VALUES make
                     * business sense. There is no Spring/@KafkaListener
                     * machinery in this module to do this automatically,
                     * so it's invoked explicitly, right here, before the
                     * event is allowed anywhere near persistAlert().
                     */
                    Set<ConstraintViolation<AnomalyEvent>> violations =
                            VALIDATOR.validate(event);

                    if (!violations.isEmpty()) {

                        String violationSummary = violations.stream()
                                .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                                .collect(Collectors.joining("; "));

                        log.warn("Validation failed topic={} partition={} offset={} sensorId={} violations=[{}]",
                                record.topic(), record.partition(), record.offset(),
                                safe(event.sensorId()), violationSummary);

                        String payload = rawPayload;

                        boolean deadLettered = false;

                        while (!deadLettered) {

                            try {

                                if (!dbConnection.isValid(2)) {

                                    closeQuietly(insertAlertStatement);
                                    closeQuietly(alertDlqStatement);
                                    closeQuietly(dbConnection);

                                    dbConnection = connectWithRetry();

                                    insertAlertStatement =
                                            dbConnection.prepareStatement(
                                                    INSERT_ALERT_SQL
                                            );

                                    alertDlqStatement =
                                            dbConnection.prepareStatement(
                                                    INSERT_ALERT_DLQ_SQL
                                            );
                                }

                                boolean wasNewDeadLetter = deadLetter(
                                        alertDlqStatement,
                                        event.sensorId(),
                                        payload,
                                        record.topic(),
                                        record.partition(),
                                        record.offset(),
                                        "VALIDATION",
                                        violationSummary
                                );

                                logDeadLetterOutcome(wasNewDeadLetter, record, event.sensorId(), "VALIDATION");

                                deadLettered = true;

                            } catch (SQLException dlqEx) {

                                if (!isRetryable(dlqEx)) {

                                    writeFallbackRecord(
                                            event.sensorId(),
                                            payload,
                                            record.topic(),
                                            record.partition(),
                                            record.offset(),
                                            "VALIDATION",
                                            violationSummary,
                                            dlqEx
                                    );

                                    deadLettered = true;

                                } else {

                                    System.err.println(
                                            "Transient DB error (SQLState="
                                                    + dlqEx.getSQLState()
                                                    + ") persisting to dead-letter table: "
                                                    + dlqEx.getMessage() + ". Reconnecting..."
                                    );

                                    closeQuietly(insertAlertStatement);
                                    closeQuietly(alertDlqStatement);
                                    closeQuietly(dbConnection);

                                    dbConnection = connectWithRetry();

                                    insertAlertStatement =
                                            dbConnection.prepareStatement(
                                                    INSERT_ALERT_SQL
                                            );

                                    alertDlqStatement =
                                            dbConnection.prepareStatement(
                                                    INSERT_ALERT_DLQ_SQL
                                            );
                                }
                            }
                        }

                        /*
                         * This record failed validation and has either
                         * been dead-lettered or could not be captured.
                         * Either way, it must never reach persistAlert().
                         */
                        continue;
                    }

                    /*
                     * Keep retrying until the alert is persisted,
                     * or we determine the failure is permanent.
                     */
                    while (!persisted) {

                        try {

                            if (!dbConnection.isValid(2)) {

                                closeQuietly(insertAlertStatement);
                                closeQuietly(alertDlqStatement);
                                closeQuietly(dbConnection);

                                dbConnection = connectWithRetry();

                                insertAlertStatement =
                                        dbConnection.prepareStatement(
                                                INSERT_ALERT_SQL
                                        );

                                alertDlqStatement =
                                        dbConnection.prepareStatement(
                                                INSERT_ALERT_DLQ_SQL
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

                                log.warn("Insert rejected permanently topic={} partition={} offset={} sensorId={} sqlState={}",
                                        record.topic(), record.partition(), record.offset(),
                                        safe(event.sensorId()), e.getSQLState());

                                String failureSqlState = e.getSQLState();
                                String failureMessage = e.getMessage();
                                String failurePayload;

                                try {
                                    failurePayload = OBJECT_MAPPER.writeValueAsString(event);
                                } catch (JsonProcessingException je) {

                                    System.err.println(
                                            "FATAL: could not serialize event for"
                                                    + " dead-letter capture, sensor="
                                                    + event.sensorId()
                                                    + ", partition=" + record.partition()
                                                    + ", offset=" + record.offset()
                                                    + ": " + je.getMessage()
                                                    + ". This alert is permanently lost."
                                    );

                                    break;
                                }

                                boolean deadLettered = false;

                                /*
                                 * Retry the DLQ write with the same
                                 * reconnect-on-transient-failure logic
                                 * used for the primary insert.
                                 */
                                while (!deadLettered) {

                                    try {

                                        if (!dbConnection.isValid(2)) {

                                            closeQuietly(insertAlertStatement);
                                            closeQuietly(alertDlqStatement);
                                            closeQuietly(dbConnection);

                                            dbConnection = connectWithRetry();

                                            insertAlertStatement =
                                                    dbConnection.prepareStatement(
                                                            INSERT_ALERT_SQL
                                                    );

                                            alertDlqStatement =
                                                    dbConnection.prepareStatement(
                                                            INSERT_ALERT_DLQ_SQL
                                                    );
                                        }

                                        boolean wasNewDeadLetter = deadLetter(
                                                alertDlqStatement,
                                                event.sensorId(),
                                                failurePayload,
                                                record.topic(),
                                                record.partition(),
                                                record.offset(),
                                                failureSqlState,
                                                failureMessage
                                        );

                                        logDeadLetterOutcome(wasNewDeadLetter, record, event.sensorId(), "INSERT_REJECTED");

                                        deadLettered = true;

                                    } catch (SQLException dlqEx) {

                                        if (!isRetryable(dlqEx)) {

                                            writeFallbackRecord(
                                                    event.sensorId(),
                                                    failurePayload,
                                                    record.topic(),
                                                    record.partition(),
                                                    record.offset(),
                                                    failureSqlState,
                                                    failureMessage,
                                                    dlqEx
                                            );

                                            deadLettered = true;

                                        } else {

                                            System.err.println(
                                                    "Transient DB error (SQLState="
                                                            + dlqEx.getSQLState()
                                                            + ") persisting to dead-letter table: "
                                                            + dlqEx.getMessage() + ". Reconnecting..."
                                            );

                                            closeQuietly(insertAlertStatement);
                                            closeQuietly(alertDlqStatement);
                                            closeQuietly(dbConnection);

                                            dbConnection = connectWithRetry();

                                            insertAlertStatement =
                                                    dbConnection.prepareStatement(
                                                            INSERT_ALERT_SQL
                                                    );

                                            alertDlqStatement =
                                                    dbConnection.prepareStatement(
                                                            INSERT_ALERT_DLQ_SQL
                                                    );
                                        }
                                    }
                                }

                                /*
                                 * The record has either been successfully
                                 * dead-lettered or could not be captured.
                                 */
                                break;
                            }

                            System.err.println(
                                    "Transient DB error (SQLState=" + e.getSQLState()
                                            + ") persisting alert for " + event.sensorId()
                                            + ": " + e.getMessage() + ". Reconnecting..."
                            );

                            closeQuietly(insertAlertStatement);
                            closeQuietly(alertDlqStatement);
                            closeQuietly(dbConnection);

                            dbConnection = connectWithRetry();

                            insertAlertStatement =
                                    dbConnection.prepareStatement(
                                            INSERT_ALERT_SQL
                                    );

                            alertDlqStatement =
                                    dbConnection.prepareStatement(
                                            INSERT_ALERT_DLQ_SQL
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
                 * processed successfully or routed to the DLQ.
                 */
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (WakeupException e) {

            log.info("Consumer loop interrupted for shutdown, as expected");
        } catch (InterruptedException e) {

            log.info("Consumer interrupted during reconnect backoff for shutdown, as expected");

        } catch (SQLException e) {

            throw new RuntimeException(
                    "Database error while persisting alerts",
                    e
            );

        } finally {

            closeQuietly(insertAlertStatement);
            closeQuietly(alertDlqStatement);
            closeQuietly(dbConnection);

            consumer.close();

            log.info("Consumer closed cleanly");;
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

    private static boolean deadLetter(
            PreparedStatement alertDlqStatement,
            String sensorId,
            String payload,
            String kafkaTopic,
            int kafkaPartition,
            long kafkaOffset,
            String sqlState,
            String errorMessage
    ) throws SQLException {

        alertDlqStatement.setString(1, kafkaTopic);
        alertDlqStatement.setInt(2, kafkaPartition);
        alertDlqStatement.setLong(3, kafkaOffset);
        alertDlqStatement.setString(4, sensorId);
        alertDlqStatement.setString(5, payload);
        alertDlqStatement.setString(6, sqlState);
        alertDlqStatement.setString(7, errorMessage);

        int rowsInserted = alertDlqStatement.executeUpdate();

        if (rowsInserted > 1) {
            throw new SQLException(
                    "Expected to insert at most 1 dead-letter row, but inserted " + rowsInserted
            );
        }

        /*
         * true  = this failure was captured as a new dead-letter row.
         * false = ON CONFLICT DO NOTHING fired: this exact Kafka
         *         record was already dead-lettered by an earlier
         *         delivery attempt.
         */
        return rowsInserted == 1;
    }

    /**
     * Last-resort persistence when the dead-letter TABLE write itself fails
     * for a non-retryable reason (e.g. a constraint violation unrelated to
     * connectivity - retrying won't fix it). Appends the failed record to a
     * local JSON-Lines file so it is not silently lost even though the
     * database rejected it.
     * <p>
     * If even this file write fails (disk full, permissions, etc.), there
     * is no further fallback: this is deliberately allowed to propagate as
     * an unchecked exception so the consumer crashes loudly rather than
     * quietly losing data.
     */
    private static void writeFallbackRecord(
            String sensorId,
            String payload,
            String kafkaTopic,
            int kafkaPartition,
            long kafkaOffset,
            String sqlState,
            String errorMessage,
            SQLException dlqWriteFailure
    ) {
        try {
            Map<String, Object> fallbackRecord = new LinkedHashMap<>();
            fallbackRecord.put("kafkaTopic", kafkaTopic);
            fallbackRecord.put("kafkaPartition", kafkaPartition);
            fallbackRecord.put("kafkaOffset", kafkaOffset);
            fallbackRecord.put("sensorId", sensorId);
            fallbackRecord.put("payload", payload);
            fallbackRecord.put("sqlState", sqlState);
            fallbackRecord.put("errorMessage", errorMessage);
            fallbackRecord.put("dlqWriteFailureSqlState", dlqWriteFailure.getSQLState());
            fallbackRecord.put("dlqWriteFailureReason", dlqWriteFailure.getMessage());
            fallbackRecord.put("failedAt", Instant.now().toString());

            String line = OBJECT_MAPPER.writeValueAsString(fallbackRecord) + System.lineSeparator();

            try (FileChannel channel = FileChannel.open(
                    DLQ_FALLBACK_FILE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE
            )) {
                channel.write(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
                channel.force(true); // fsync - survive a JVM/OS crash, not just an unflushed buffer
            }

            System.err.println(
                    "DLQ WRITE FAILED | sensor=" + sensorId
                            + " | partition=" + kafkaPartition
                            + " | offset=" + kafkaOffset
                            + " | cause=" + dlqWriteFailure.getMessage()
                            + " | fell back to local file " + DLQ_FALLBACK_FILE
            );

        } catch (IOException | RuntimeException fallbackFailure) {

            System.err.println(
                    "FATAL: DLQ table write AND local file fallback both failed"
                            + " for sensor=" + sensorId
                            + ", partition=" + kafkaPartition
                            + ", offset=" + kafkaOffset
                            + ". dlqCause=" + dlqWriteFailure.getMessage()
                            + ", fallbackCause=" + fallbackFailure.getMessage()
                            + ". This alert is permanently lost."
            );

            throw new UncheckedIOException(
                    "Unable to persist failed message anywhere - DB and local fallback both failed",
                    fallbackFailure instanceof IOException ioFailure
                            ? ioFailure
                            : new IOException(fallbackFailure)
            );
        }
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
                                config.dbUrl(),
                                config.dbUser(),
                                config.dbPassword()
                        );

                log.info("Connected to database");

                return connection;

            } catch (SQLException e) {
                String sqlState = e.getSQLState();

                if (sqlState != null && sqlState.startsWith("28")) {
                    throw new IllegalStateException(
                            "Database rejected the configured credentials (SQLState=" + sqlState
                                    + "). Not retrying: " + e.getMessage(), e);
                }

                System.err.println(
                        "DB connection failed: "
                                + e.getMessage()
                                + ". Retrying in "
                                + backoffMs
                                + "ms..."
                );

                sleepUnlessShuttingDown(backoffMs);

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

    /**
     * Sleeps for up to durationMs, checking the shutdown flag every
     * SHUTDOWN_CHECK_INTERVAL_MS and returning early - by throwing
     * InterruptedException - if a shutdown signal arrived mid-backoff.
     * <p>
     * Plain Thread.sleep(durationMs) would block for the full duration,
     * up to MAX_BACKOFF_MS (30s), regardless of a shutdown request:
     * consumer.wakeup() only affects a thread blocked inside
     * consumer.poll(), not one blocked in Thread.sleep(). Without this,
     * SIGTERM/Ctrl+C during a reconnect backoff would leave the process
     * unresponsive for up to 30 seconds - long enough for an orchestrator
     * like Kubernetes to send SIGKILL before a clean shutdown completes.
     */
    private static void sleepUnlessShuttingDown(long durationMs)
            throws InterruptedException {

        long remainingMs = durationMs;

        while (remainingMs > 0) {

            if (shuttingDown) {
                throw new InterruptedException(
                        "Shutdown requested during reconnect backoff"
                );
            }

            long thisSleepMs = Math.min(remainingMs, SHUTDOWN_CHECK_INTERVAL_MS);

            Thread.sleep(thisSleepMs);

            remainingMs -= thisSleepMs;
        }
    }

    private static void logDeadLetterOutcome(
            boolean isNew,
            ConsumerRecord<String, String> record,
            String sensorId,
            String reason) {

        if (isNew) {
            log.warn("DEAD_LETTERED topic={} partition={} offset={} sensorId={} reason={}",
                    record.topic(), record.partition(), record.offset(), safe(sensorId), reason);
        } else {
            log.info("DEAD_LETTER_DUPLICATE topic={} partition={} offset={} sensorId={} reason={}",
                    record.topic(), record.partition(), record.offset(), safe(sensorId), reason);
        }
    }

    // Values from Kafka messages are untrusted: strip control characters (a newline
    // could forge a fake log line) and cap the length.
    private static String safe(String value) {
        if (value == null) {
            return "null";
        }
        String cleaned = value.replaceAll("\\p{Cntrl}", "_");
        return cleaned.length() > 64 ? cleaned.substring(0, 64) + "..." : cleaned;
    }

}
