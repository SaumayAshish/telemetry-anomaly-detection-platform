package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.platform.alerting.AlertWriter.DeadLetter;
import com.telemetry.platform.alerting.AlertWriter.PersistResult;
import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.sql.SQLException;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** Every routing decision of RecordProcessor leaves exactly one trace in the metrics. */
class RecordProcessorMetricsTest {

    private static final RecordPosition POSITION =
            new RecordPosition("telemetry.sensor.anomalies.v1", 2, 17L);

    private static final Validator VALIDATOR =
            Validation.buildDefaultValidatorFactory().getValidator();

    private final ObjectMapper mapper = RecordProcessor.newObjectMapper();
    private final AlertMetrics metrics = AlertMetrics.create();
    private PersistResult nextResult = PersistResult.inserted();

    private final AlertSink sink = new AlertSink() {
        @Override
        public PersistResult persist(RecordPosition position, AnomalyEvent event) {
            return nextResult;
        }

        @Override
        public void deadLetter(RecordPosition position, DeadLetter letter) {
        }
    };

    private final RecordProcessor processor = new RecordProcessor(sink, mapper, VALIDATOR, metrics);

    private String payload(String sensorId, AnomalySeverity severity) throws Exception {
        return mapper.writeValueAsString(new AnomalyEvent(
                sensorId, Instant.parse("2026-10-13T10:15:30Z"), 9.5, 2.0, 1.5, 5.0, 3L, severity));
    }

    @Test
    void aPersistedAlertIsCountedWithItsSeverity() throws Exception {
        processor.process(POSITION, payload("sensor-1", AnomalySeverity.CRITICAL));

        assertThat(metrics.scrape())
                .contains("alerting_alerts_persisted_total{severity=\"CRITICAL\"} 1.0")
                .contains("alerting_alerts_duplicate_total 0.0");
    }

    @Test
    void aDuplicateIsCountedAndNotCountedAsPersisted() throws Exception {
        nextResult = PersistResult.duplicate();

        processor.process(POSITION, payload("sensor-1", AnomalySeverity.HIGH));

        assertThat(metrics.scrape())
                .contains("alerting_alerts_duplicate_total 1.0")
                .doesNotContain("alerting_alerts_persisted_total");
    }

    @Test
    void malformedJsonIsCountedAsADeserializationDeadLetter() throws Exception {
        processor.process(POSITION, "{not json");

        assertThat(metrics.scrape())
                .contains("alerting_dead_letters_total{reason=\"DESERIALIZATION\"} 1.0")
                .doesNotContain("alerting_alerts_persisted_total");
    }

    @Test
    void anInvalidEventIsCountedAsAValidationDeadLetter() throws Exception {
        processor.process(POSITION, payload("", AnomalySeverity.HIGH));

        assertThat(metrics.scrape())
                .contains("alerting_dead_letters_total{reason=\"VALIDATION\"} 1.0")
                .doesNotContain("alerting_alerts_persisted_total");
    }

    @Test
    void aRejectedInsertIsCountedAsARejectedDeadLetter() throws Exception {
        nextResult = PersistResult.rejected(new SQLException("unknown sensor", "23503"));

        processor.process(POSITION, payload("sensor-1", AnomalySeverity.HIGH));

        assertThat(metrics.scrape())
                .contains("alerting_dead_letters_total{reason=\"INSERT_REJECTED\"} 1.0")
                .doesNotContain("alerting_alerts_persisted_total");
    }
}
