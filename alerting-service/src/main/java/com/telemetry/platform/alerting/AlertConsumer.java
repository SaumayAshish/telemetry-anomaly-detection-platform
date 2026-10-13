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

    private static final long SHUTDOWN_CHECK_INTERVAL_MS = 200;
    private static volatile boolean shuttingDown = false;

    private static final ValidatorFactory VALIDATOR_FACTORY =
            Validation.buildDefaultValidatorFactory();
    private static final Validator VALIDATOR =
            VALIDATOR_FACTORY.getValidator();

    private static final Duration POLL_TIMEOUT =
            Duration.ofSeconds(1);

    private static final Set<String> RETRYABLE_SQLSTATE_CLASSES =
            Set.of("08", "53", "57");

    private static final Path DLQ_FALLBACK_FILE =
            Paths.get("dlq-fallback.jsonl");

    private static final String INSERT_ALERT_SQL = AlertStore.INSERT_ALERT_SQL;
    private static final String INSERT_ALERT_DLQ_SQL = AlertStore.INSERT_ALERT_DLQ_SQL;

    private static final DatabaseConnector CONNECTOR = new DatabaseConnector(
            () -> DriverManager.getConnection(
                    AlertConsumer.config.dbUrl(),
                    AlertConsumer.config.dbUser(),
                    AlertConsumer.config.dbPassword()),
            AlertConsumer::sleepUnlessShuttingDown,
            INITIAL_BACKOFF_MS,
            MAX_BACKOFF_MS,
            BACKOFF_MULTIPLIER);

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
         * Consume both key and value as plain Strings.
         * Parse the event explicitly inside the processing loop so
         * malformed messages can be routed to the dead-letter path.
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
            dbConnection = connectWithRetry();

            insertAlertStatement =
                    dbConnection.prepareStatement(INSERT_ALERT_SQL);

            alertDlqStatement =
                    dbConnection.prepareStatement(INSERT_ALERT_DLQ_SQL);

            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<String, String> record : records) {

                    String rawPayload = record.value();
                    AnomalyEvent event;
                    boolean persisted = false;
                    boolean wasNewInsert = false;

                    // JSON deserialization boundary.
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

                    } catch (
                            JsonProcessingException
                            | IllegalArgumentException deserializationFailure
                    ) {

                        log.warn(
                                "Deserialization failed topic={} partition={} offset={} exception={}",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                deserializationFailure.getClass().getSimpleName()
                        );

                        boolean deadLettered = false;

                        while (!deadLettered) {
                            try {
                                if (!dbConnection.isValid(2)) {
                                    log.warn(
                                            "Database connection no longer valid, reconnecting topic={} partition={} offset={}",
                                            record.topic(),
                                            record.partition(),
                                            record.offset()
                                    );

                                    closeQuietly(insertAlertStatement);
                                    closeQuietly(alertDlqStatement);
                                    closeQuietly(dbConnection);

                                    dbConnection = connectWithRetry();

                                    insertAlertStatement =
                                            dbConnection.prepareStatement(INSERT_ALERT_SQL);

                                    alertDlqStatement =
                                            dbConnection.prepareStatement(INSERT_ALERT_DLQ_SQL);
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

                                logDeadLetterOutcome(
                                        wasNewDeadLetter,
                                        record,
                                        "UNKNOWN",
                                        "DESERIALIZATION"
                                );

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
                                    log.warn(
                                            "Transient database error writing dead letter, reconnecting topic={} partition={} offset={} sqlState={}",
                                            record.topic(),
                                            record.partition(),
                                            record.offset(),
                                            dlqEx.getSQLState()
                                    );

                                    closeQuietly(insertAlertStatement);
                                    closeQuietly(alertDlqStatement);
                                    closeQuietly(dbConnection);

                                    dbConnection = connectWithRetry();

                                    insertAlertStatement =
                                            dbConnection.prepareStatement(INSERT_ALERT_SQL);

                                    alertDlqStatement =
                                            dbConnection.prepareStatement(INSERT_ALERT_DLQ_SQL);
                                }
                            }
                        }

                        // Malformed messages never reach validation or persistence.
                        continue;
                    }

                    // Bean Validation boundary.
                    Set<ConstraintViolation<AnomalyEvent>> violations =
                            VALIDATOR.validate(event);

                    if (!violations.isEmpty()) {

                        String violationSummary = violations.stream()
                                .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                                .sorted()
                                .collect(Collectors.joining("; "));

                        log.warn(
                                "Validation failed topic={} partition={} offset={} sensorId={} violations=[{}]",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                safe(event.sensorId()),
                                violationSummary
                        );

                        String payload = rawPayload;
                        boolean deadLettered = false;

                        while (!deadLettered) {
                            try {
                                if (!dbConnection.isValid(2)) {
                                    log.warn(
                                            "Database connection no longer valid, reconnecting topic={} partition={} offset={}",
                                            record.topic(),
                                            record.partition(),
                                            record.offset()
                                    );

                                    closeQuietly(insertAlertStatement);
                                    closeQuietly(alertDlqStatement);
                                    closeQuietly(dbConnection);

                                    dbConnection = connectWithRetry();

                                    insertAlertStatement =
                                            dbConnection.prepareStatement(INSERT_ALERT_SQL);

                                    alertDlqStatement =
                                            dbConnection.prepareStatement(INSERT_ALERT_DLQ_SQL);
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

                                logDeadLetterOutcome(
                                        wasNewDeadLetter,
                                        record,
                                        event.sensorId(),
                                        "VALIDATION"
                                );

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
                                    log.warn(
                                            "Transient database error writing dead letter, reconnecting topic={} partition={} offset={} sqlState={}",
                                            record.topic(),
                                            record.partition(),
                                            record.offset(),
                                            dlqEx.getSQLState()
                                    );

                                    closeQuietly(insertAlertStatement);
                                    closeQuietly(alertDlqStatement);
                                    closeQuietly(dbConnection);

                                    dbConnection = connectWithRetry();

                                    insertAlertStatement =
                                            dbConnection.prepareStatement(INSERT_ALERT_SQL);

                                    alertDlqStatement =
                                            dbConnection.prepareStatement(INSERT_ALERT_DLQ_SQL);
                                }
                            }
                        }

                        // Invalid events never reach persistAlert().
                        continue;
                    }

                    // Retry until the alert is persisted or the failure is permanent.
                    while (!persisted) {
                        try {
                            if (!dbConnection.isValid(2)) {
                                log.warn(
                                        "Database connection no longer valid, reconnecting topic={} partition={} offset={}",
                                        record.topic(),
                                        record.partition(),
                                        record.offset()
                                );

                                closeQuietly(insertAlertStatement);
                                closeQuietly(alertDlqStatement);
                                closeQuietly(dbConnection);

                                dbConnection = connectWithRetry();

                                insertAlertStatement =
                                        dbConnection.prepareStatement(INSERT_ALERT_SQL);

                                alertDlqStatement =
                                        dbConnection.prepareStatement(INSERT_ALERT_DLQ_SQL);
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
                                log.warn(
                                        "Insert rejected permanently topic={} partition={} offset={} sensorId={} sqlState={}",
                                        record.topic(),
                                        record.partition(),
                                        record.offset(),
                                        safe(event.sensorId()),
                                        e.getSQLState()
                                );

                                String failureSqlState = e.getSQLState();
                                String failureMessage = e.getMessage();
                                String failurePayload;

                                try {
                                    failurePayload =
                                            OBJECT_MAPPER.writeValueAsString(event);

                                } catch (JsonProcessingException je) {
                                    log.error(
                                            "Could not serialize event for dead-letter capture, alert permanently lost topic={} partition={} offset={} sensorId={} exception={}",
                                            record.topic(),
                                            record.partition(),
                                            record.offset(),
                                            safe(event.sensorId()),
                                            je.getClass().getSimpleName()
                                    );

                                    break;
                                }

                                boolean deadLettered = false;

                                // Retry the DLQ write when the failure is transient.
                                while (!deadLettered) {
                                    try {
                                        if (!dbConnection.isValid(2)) {
                                            log.warn(
                                                    "Database connection no longer valid, reconnecting topic={} partition={} offset={}",
                                                    record.topic(),
                                                    record.partition(),
                                                    record.offset()
                                            );

                                            closeQuietly(insertAlertStatement);
                                            closeQuietly(alertDlqStatement);
                                            closeQuietly(dbConnection);

                                            dbConnection = connectWithRetry();

                                            insertAlertStatement =
                                                    dbConnection.prepareStatement(INSERT_ALERT_SQL);

                                            alertDlqStatement =
                                                    dbConnection.prepareStatement(INSERT_ALERT_DLQ_SQL);
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

                                        logDeadLetterOutcome(
                                                wasNewDeadLetter,
                                                record,
                                                event.sensorId(),
                                                "INSERT_REJECTED"
                                        );

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
                                            log.warn(
                                                    "Transient database error writing dead letter, reconnecting topic={} partition={} offset={} sqlState={}",
                                                    record.topic(),
                                                    record.partition(),
                                                    record.offset(),
                                                    dlqEx.getSQLState()
                                            );

                                            closeQuietly(insertAlertStatement);
                                            closeQuietly(alertDlqStatement);
                                            closeQuietly(dbConnection);

                                            dbConnection = connectWithRetry();

                                            insertAlertStatement =
                                                    dbConnection.prepareStatement(INSERT_ALERT_SQL);

                                            alertDlqStatement =
                                                    dbConnection.prepareStatement(INSERT_ALERT_DLQ_SQL);
                                        }
                                    }
                                }

                                // The record was dead-lettered or captured by fallback.
                                break;
                            }

                            log.warn(
                                    "Transient database error persisting alert, reconnecting topic={} partition={} offset={} sqlState={}",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    e.getSQLState()
                            );

                            closeQuietly(insertAlertStatement);
                            closeQuietly(alertDlqStatement);
                            closeQuietly(dbConnection);

                            dbConnection = connectWithRetry();

                            insertAlertStatement =
                                    dbConnection.prepareStatement(INSERT_ALERT_SQL);

                            alertDlqStatement =
                                    dbConnection.prepareStatement(INSERT_ALERT_DLQ_SQL);
                        }
                    }

                    if (persisted && wasNewInsert) {
                        log.info(
                                "ALERT_PERSISTED topic={} partition={} offset={} sensorId={} severity={}",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                safe(event.sensorId()),
                                event.severity()
                        );

                    } else if (persisted) {
                        log.info(
                                "ALERT_DUPLICATE topic={} partition={} offset={} sensorId={}",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                safe(event.sensorId())
                        );
                    }
                }

                // Commit only after the polled batch has been processed.
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (WakeupException e) {

            log.info("Consumer loop interrupted for shutdown, as expected");

        } catch (InterruptedException e) {

            log.info(
                    "Consumer interrupted during reconnect backoff for shutdown, as expected"
            );

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

            log.info("Consumer closed cleanly");
        }
    }

    private static boolean persistAlert(
            PreparedStatement insertAlertStatement,
            AnomalyEvent event,
            int kafkaPartition,
            long kafkaOffset
    ) throws SQLException {

        return AlertStore.persistAlert(
                insertAlertStatement,
                event,
                kafkaPartition,
                kafkaOffset
        );
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

        return AlertStore.deadLetter(
                alertDlqStatement,
                sensorId,
                payload,
                kafkaTopic,
                kafkaPartition,
                kafkaOffset,
                sqlState,
                errorMessage
        );
    }

    /**
     * Last-resort persistence when the dead-letter table write fails
     * for a non-retryable reason.
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
            fallbackRecord.put(
                    "dlqWriteFailureSqlState",
                    dlqWriteFailure.getSQLState()
            );
            fallbackRecord.put(
                    "dlqWriteFailureReason",
                    dlqWriteFailure.getMessage()
            );
            fallbackRecord.put("failedAt", Instant.now().toString());

            String line =
                    OBJECT_MAPPER.writeValueAsString(fallbackRecord)
                            + System.lineSeparator();

            try (FileChannel channel = FileChannel.open(
                    DLQ_FALLBACK_FILE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE
            )) {
                channel.write(
                        ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8))
                );
                channel.force(true);
            }

            log.error(
                    "DLQ_TABLE_WRITE_FAILED, fell back to local file topic={} partition={} offset={} sensorId={} sqlState={} file={}",
                    kafkaTopic,
                    kafkaPartition,
                    kafkaOffset,
                    safe(sensorId),
                    dlqWriteFailure.getSQLState(),
                    DLQ_FALLBACK_FILE
            );

        } catch (IOException | RuntimeException fallbackFailure) {

            log.error(
                    "DLQ table write AND local file fallback both failed, record permanently lost topic={} partition={} offset={} sensorId={} dlqSqlState={} fallbackException={}",
                    kafkaTopic,
                    kafkaPartition,
                    kafkaOffset,
                    safe(sensorId),
                    dlqWriteFailure.getSQLState(),
                    fallbackFailure.getClass().getSimpleName()
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
        return SqlFailureClassifier.isRetryable(e);
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource == null) {
            return;
        }

        try {
            resource.close();
        } catch (Exception e) {
            log.warn(
                    "Error closing resource exception={}",
                    e.getClass().getSimpleName()
            );
        }
    }

    private static Connection connectWithRetry() throws InterruptedException {
        return CONNECTOR.connectWithRetry();
    }

    /**
     * Sleeps for up to durationMs, checking the shutdown flag every
     * SHUTDOWN_CHECK_INTERVAL_MS and returning early by throwing
     * InterruptedException if a shutdown signal arrives during backoff.
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

            long thisSleepMs =
                    Math.min(remainingMs, SHUTDOWN_CHECK_INTERVAL_MS);

            Thread.sleep(thisSleepMs);

            remainingMs -= thisSleepMs;
        }
    }

    private static void logDeadLetterOutcome(
            boolean isNew,
            ConsumerRecord<String, String> record,
            String sensorId,
            String reason
    ) {
        if (isNew) {
            log.warn(
                    "DEAD_LETTERED topic={} partition={} offset={} sensorId={} reason={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    safe(sensorId),
                    reason
            );
        } else {
            log.info(
                    "DEAD_LETTER_DUPLICATE topic={} partition={} offset={} sensorId={} reason={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    safe(sensorId),
                    reason
            );
        }
    }

    private static String safe(String value) {
        return LogSanitizer.safe(value);
    }
}