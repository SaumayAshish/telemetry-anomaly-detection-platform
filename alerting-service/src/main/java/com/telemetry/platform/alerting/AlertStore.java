package com.telemetry.platform.alerting;

import com.telemetry.platform.events.AnomalyEvent;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Database writes for alerts and dead letters, separated from the consumer loop
 * so they can be tested against a real PostgreSQL.
 */
final class AlertStore {

    static final String INSERT_ALERT_SQL = """
            INSERT INTO anomaly_alerts
                (sensor_id, reading_timestamp, value, baseline_mean, baseline_std_dev, z_score, consecutive_anomalies, severity, kafka_partition, kafka_offset)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
            """;

    static final String INSERT_ALERT_DLQ_SQL = """
            INSERT INTO alert_dlq
                (kafka_topic, kafka_partition, kafka_offset, sensor_id, payload, sql_state, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
            """;

    private AlertStore() {
    }

    /**
     * @return true if a new alert row was inserted, false if ON CONFLICT DO NOTHING
     *         fired because this (kafka_partition, kafka_offset) was already persisted.
     */
    static boolean persistAlert(
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

        return rowsInserted == 1;
    }

    /**
     * @return true if this failure was captured as a new dead-letter row, false if
     *         this exact Kafka record was already dead-lettered by an earlier attempt.
     */
    static boolean deadLetter(
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

        return rowsInserted == 1;
    }
}