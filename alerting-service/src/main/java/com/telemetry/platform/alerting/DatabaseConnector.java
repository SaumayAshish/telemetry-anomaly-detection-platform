package com.telemetry.platform.alerting;

import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Opens a database connection, retrying with exponential backoff until it succeeds.
 * Credential and configuration rejections fail fast. The way to open a connection and
 * the way to sleep are injected so the retry policy can be tested without a database
 * and without waiting.
 */
final class DatabaseConnector {

    private static final Logger log = LoggerFactory.getLogger(DatabaseConnector.class);

    @FunctionalInterface
    interface ConnectionOpener {
        Connection open() throws SQLException;
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final ConnectionOpener opener;
    private final Sleeper sleeper;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final long backoffMultiplier;

    DatabaseConnector(
            ConnectionOpener opener,
            Sleeper sleeper,
            long initialBackoffMs,
            long maxBackoffMs,
            long backoffMultiplier
    ) {
        this.opener = opener;
        this.sleeper = sleeper;
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.backoffMultiplier = backoffMultiplier;
    }

    Connection connectWithRetry() throws InterruptedException {

        long backoffMs = initialBackoffMs;

        while (true) {

            try {
                Connection connection = opener.open();

                log.info("Connected to database");

                return connection;

            } catch (SQLException e) {
                String sqlState = e.getSQLState();

                if (SqlFailureClassifier.isFatalConnectFailure(sqlState)) {
                    log.error("Database rejected the connection sqlState={}, not retrying", sqlState);
                    throw new IllegalStateException(
                            "Database rejected the connection (SQLState=" + sqlState
                                    + "). Check DB_USER and DB_PASSWORD. Not retrying: " + e.getMessage(), e);
                }

                log.warn("Database connection failed, retrying in {} ms sqlState={}: {}",
                        backoffMs, sqlState, e.getMessage());

                sleeper.sleep(backoffMs);

                // 1s -> 2s -> 4s -> 8s -> 16s -> 30s -> 30s ...
                backoffMs = Math.min(backoffMs * backoffMultiplier, maxBackoffMs);
            }
        }
    }
}
