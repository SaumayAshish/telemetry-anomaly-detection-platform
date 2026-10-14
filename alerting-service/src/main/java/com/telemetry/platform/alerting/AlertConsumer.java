package com.telemetry.platform.alerting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.platform.alerting.AlertWriter.DeadLetter;
import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import com.telemetry.platform.alerting.BatchLoop.PolledRecord;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
    private static volatile KafkaConsumer<String, String> activeConsumer;

    private static final ValidatorFactory VALIDATOR_FACTORY =
            Validation.buildDefaultValidatorFactory();
    private static final Validator VALIDATOR =
            VALIDATOR_FACTORY.getValidator();

    private static final Duration POLL_TIMEOUT =
            Duration.ofSeconds(1);

    private static final Path DLQ_FALLBACK_FILE =
            Paths.get("dlq-fallback.jsonl");

    private static final DatabaseConnector CONNECTOR = new DatabaseConnector(
            () -> DriverManager.getConnection(
                    AlertConsumer.config.dbUrl(),
                    AlertConsumer.config.dbUser(),
                    AlertConsumer.config.dbPassword()),
            AlertConsumer::sleepUnlessShuttingDown,
            INITIAL_BACKOFF_MS,
            MAX_BACKOFF_MS,
            BACKOFF_MULTIPLIER);

    private static final ObjectMapper OBJECT_MAPPER =
            RecordProcessor.newObjectMapper();

    public static void main(String[] args) {

        AlertConsumerConfig loaded = AlertConsumerConfig.fromEnvironment();
        log.info("Configuration loaded: {}", loaded);

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    log.info("Shutdown signal received, waking up consumer");

                    stop();

                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                })
        );

        run(loaded);
    }

    /**
     * Runs the consume loop until {@link #stop()} is called. Blocks the calling thread.
     */
    static void run(AlertConsumerConfig runConfig) {

        config = runConfig;

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

        activeConsumer = consumer;

        // A stop() that arrived before the consumer existed must still end this run.
        if (shuttingDown) {
            consumer.wakeup();
        };

        AlertWriter writer = null;

        try {
            writer = new AlertWriter(
                    AlertConsumer::connectWithRetry,
                    AlertConsumer::writeFallback,
                    AlertConsumer::sleepUnlessShuttingDown
            );

            RecordProcessor processor =
                    new RecordProcessor(writer, OBJECT_MAPPER, VALIDATOR);

            BatchLoop.BatchSource source = new BatchLoop.BatchSource() {
                @Override
                public List<PolledRecord> poll() {
                    List<PolledRecord> batch = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : consumer.poll(POLL_TIMEOUT)) {
                        batch.add(new PolledRecord(
                                new RecordPosition(
                                        record.topic(),
                                        record.partition(),
                                        record.offset()
                                ),
                                record.value()
                        ));
                    }

                    return batch;
                }

                @Override
                public void commit() {
                    consumer.commitSync();
                }
            };

            BatchLoop loop = new BatchLoop(source, processor::process);

            while (true) {
                loop.runOnce();
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

            if (writer != null) {
                writer.close();
            }

            consumer.close();
            activeConsumer = null;

            log.info("Consumer closed cleanly");
        }
    }
    /** Asks a running {@link #run} to finish its current batch and return. */
    static void stop() {
        shuttingDown = true;

        KafkaConsumer<String, String> running = activeConsumer;
        if (running != null) {
            running.wakeup();
        }
    }

    /** Clears the stop request so {@link #run} can be started again in the same JVM. */
    static void resetForNextRun() {
        shuttingDown = false;
    }

    /** Adapts the writer's fallback hook to the file-based last resort below. */
    private static void writeFallback(
            RecordPosition position,
            DeadLetter letter,
            SQLException dlqWriteFailure
    ) {
        writeFallbackRecord(
                letter.sensorId(),
                letter.payload(),
                position.topic(),
                position.partition(),
                position.offset(),
                letter.sqlState(),
                letter.errorMessage(),
                dlqWriteFailure
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

    private static String safe(String value) {
        return LogSanitizer.safe(value);
    }
}
