package com.telemetry.platform.alerting;

import com.telemetry.platform.events.AnomalyEvent;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the database connection and the two prepared statements the consumer writes through,
 * and hides the policy for transient failures: when the connection is stale or a retryable
 * error occurs, close everything, reconnect, prepare again and retry the same write.
 *
 * <p>Permanent failures are not retried. A rejected alert insert is reported to the caller
 * as {@link Outcome#REJECTED}; a dead-letter write that the table refuses permanently is
 * handed to the {@link Fallback}, so no record is silently dropped.
 *
 * <p>Not thread-safe: the consumer loop is the only caller.
 */
final class AlertWriter implements AlertSink, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AlertWriter.class);

    private static final int VALIDITY_CHECK_TIMEOUT_SECONDS = 2;

    /** Where a Kafka record came from: log context, and the idempotency key of every write. */
    record RecordPosition(String topic, int partition, long offset) {
    }

    /**
     * A message that could not become an alert. {@code sqlState} is what the {@code sql_state}
     * column stores (a SQLState or a category label); {@code reason} is only for the log line.
     */
    record DeadLetter(String sensorId, String payload, String sqlState, String errorMessage, String reason) {
    }

    enum Outcome { INSERTED, DUPLICATE, REJECTED }

    /** {@code failure} is set only when the outcome is {@link Outcome#REJECTED}. */
    record PersistResult(Outcome outcome, SQLException failure) {

        static PersistResult inserted() {
            return new PersistResult(Outcome.INSERTED, null);
        }

        static PersistResult duplicate() {
            return new PersistResult(Outcome.DUPLICATE, null);
        }

        static PersistResult rejected(SQLException failure) {
            return new PersistResult(Outcome.REJECTED, failure);
        }
    }

    @FunctionalInterface
    interface ConnectionSource {
        /** Blocks until a connection is available; throws only if shutdown interrupts the wait. */
        Connection connect() throws InterruptedException;
    }

    /** Last resort for a dead letter the table permanently refuses. */
    @FunctionalInterface
    interface Fallback {
        void write(RecordPosition position, DeadLetter letter, SQLException deadLetterWriteFailure);
    }

    private final ConnectionSource connections;
    private final Fallback fallback;

    private Connection connection;
    private PreparedStatement insertAlert;
    private PreparedStatement insertDeadLetter;

    AlertWriter(ConnectionSource connections, Fallback fallback)
            throws SQLException, InterruptedException {
        this.connections = connections;
        this.fallback = fallback;
        try {
            open();
        } catch (SQLException | InterruptedException e) {
            close();
            throw e;
        }
    }

    /**
     * Persists one alert, retrying transient failures on a fresh connection.
     *
     * @return INSERTED, DUPLICATE (this Kafka position was already stored), or REJECTED
     *         (a permanent failure; the caller decides what to dead-letter)
     * @throws SQLException if reconnecting itself fails
     */
    @Override
    public PersistResult persist(RecordPosition position, AnomalyEvent event)
            throws SQLException, InterruptedException {

        while (true) {
            try {
                ensureValid(position);

                boolean inserted = AlertStore.persistAlert(
                        insertAlert, event, position.partition(), position.offset());

                return inserted ? PersistResult.inserted() : PersistResult.duplicate();

            } catch (SQLException e) {
                if (!SqlFailureClassifier.isRetryable(e)) {
                    return PersistResult.rejected(e);
                }

                log.warn(
                        "Transient database error persisting alert, reconnecting topic={} partition={} offset={} sqlState={}",
                        position.topic(), position.partition(), position.offset(), e.getSQLState());

                reconnect();
            }
        }
    }

    /**
     * Writes one dead letter, retrying transient failures on a fresh connection. A permanent
     * refusal by the table goes to the {@link Fallback} instead of being thrown.
     */
    @Override
    public void deadLetter(RecordPosition position, DeadLetter letter)
            throws SQLException, InterruptedException {

        while (true) {
            try {
                ensureValid(position);

                boolean isNew = AlertStore.deadLetter(
                        insertDeadLetter,
                        letter.sensorId(),
                        letter.payload(),
                        position.topic(),
                        position.partition(),
                        position.offset(),
                        letter.sqlState(),
                        letter.errorMessage());

                logDeadLetterOutcome(isNew, position, letter);
                return;

            } catch (SQLException e) {
                if (!SqlFailureClassifier.isRetryable(e)) {
                    fallback.write(position, letter, e);
                    return;
                }

                log.warn(
                        "Transient database error writing dead letter, reconnecting topic={} partition={} offset={} sqlState={}",
                        position.topic(), position.partition(), position.offset(), e.getSQLState());

                reconnect();
            }
        }
    }

    @Override
    public void close() {
        closeQuietly(insertAlert);
        closeQuietly(insertDeadLetter);
        closeQuietly(connection);
    }

    private void ensureValid(RecordPosition position) throws SQLException, InterruptedException {
        if (!connection.isValid(VALIDITY_CHECK_TIMEOUT_SECONDS)) {
            log.warn(
                    "Database connection no longer valid, reconnecting topic={} partition={} offset={}",
                    position.topic(), position.partition(), position.offset());

            reconnect();
        }
    }

    private void reconnect() throws SQLException, InterruptedException {
        close();
        open();
    }

    private void open() throws SQLException, InterruptedException {
        connection = connections.connect();
        insertAlert = connection.prepareStatement(AlertStore.INSERT_ALERT_SQL);
        insertDeadLetter = connection.prepareStatement(AlertStore.INSERT_ALERT_DLQ_SQL);
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource == null) {
            return;
        }

        try {
            resource.close();
        } catch (Exception e) {
            log.warn("Error closing resource exception={}", e.getClass().getSimpleName());
        }
    }

    private static void logDeadLetterOutcome(boolean isNew, RecordPosition position, DeadLetter letter) {
        if (isNew) {
            log.warn(
                    "DEAD_LETTERED topic={} partition={} offset={} sensorId={} reason={}",
                    position.topic(), position.partition(), position.offset(),
                    LogSanitizer.safe(letter.sensorId()), letter.reason());
        } else {
            log.info(
                    "DEAD_LETTER_DUPLICATE topic={} partition={} offset={} sensorId={} reason={}",
                    position.topic(), position.partition(), position.offset(),
                    LogSanitizer.safe(letter.sensorId()), letter.reason());
        }
    }
}