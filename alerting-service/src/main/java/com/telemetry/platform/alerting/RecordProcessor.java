package com.telemetry.platform.alerting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.telemetry.platform.alerting.AlertWriter.DeadLetter;
import com.telemetry.platform.alerting.AlertWriter.PersistResult;
import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import com.telemetry.platform.events.AnomalyEvent;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides what happens to one Kafka record: malformed JSON and invalid events are
 * dead-lettered, valid events are persisted, and an alert the database permanently
 * rejects is dead-lettered with the real SQLState. Every decision goes to the
 * {@link AlertSink}, so the routing can be tested without Kafka or a database.
 */
final class RecordProcessor {

    private static final Logger log = LoggerFactory.getLogger(RecordProcessor.class);

    private final AlertSink sink;
    private final ObjectMapper mapper;
    private final Validator validator;
    private final AlertMetrics metrics;

    RecordProcessor(AlertSink sink, ObjectMapper mapper, Validator validator, AlertMetrics metrics) {
        this.sink = sink;
        this.mapper = mapper;
        this.validator = validator;
        this.metrics = metrics;
    }

    /** The mapper the consumer uses for events and for the local fallback file. */
    static ObjectMapper newObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    /**
     * Routes one record. The caller commits offsets after the whole polled batch.
     *
     * @throws SQLException if the sink cannot reconnect to the database
     * @throws InterruptedException if shutdown interrupts a reconnect
     */
    void process(RecordPosition position, String rawPayload)
            throws SQLException, InterruptedException {

        AnomalyEvent event;

        // JSON deserialization boundary.
        try {
            event = mapper.readValue(rawPayload, AnomalyEvent.class);

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
                    position.topic(),
                    position.partition(),
                    position.offset(),
                    deserializationFailure.getClass().getSimpleName()
            );

            sink.deadLetter(position, new DeadLetter(
                    "UNKNOWN",
                    rawPayload,
                    "DESERIALIZATION",
                    deserializationFailure.getMessage(),
                    "DESERIALIZATION"
            ));
            metrics.deadLettered("DESERIALIZATION");

            // Malformed messages never reach validation or persistence.
            return;
        }

        // Bean Validation boundary.
        Set<ConstraintViolation<AnomalyEvent>> violations =
                validator.validate(event);

        if (!violations.isEmpty()) {

            String violationSummary = violations.stream()
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));

            log.warn(
                    "Validation failed topic={} partition={} offset={} sensorId={} violations=[{}]",
                    position.topic(),
                    position.partition(),
                    position.offset(),
                    LogSanitizer.safe(event.sensorId()),
                    violationSummary
            );

            sink.deadLetter(position, new DeadLetter(
                    event.sensorId(),
                    rawPayload,
                    "VALIDATION",
                    violationSummary,
                    "VALIDATION"
            ));
            metrics.deadLettered("VALIDATION");

            // Invalid events never reach persistence.
            return;
        }

        PersistResult result = sink.persist(position, event);

        switch (result.outcome()) {
            case INSERTED -> {
                log.info(
                        "ALERT_PERSISTED topic={} partition={} offset={} sensorId={} severity={}",
                        position.topic(),
                        position.partition(),
                        position.offset(),
                        LogSanitizer.safe(event.sensorId()),
                        event.severity()
                );
                metrics.alertPersisted(event.severity());
            }

            case DUPLICATE -> {
                log.info(
                        "ALERT_DUPLICATE topic={} partition={} offset={} sensorId={}",
                        position.topic(),
                        position.partition(),
                        position.offset(),
                        LogSanitizer.safe(event.sensorId())
                );
                metrics.alertDuplicate();
            }

            case REJECTED -> deadLetterRejectedInsert(
                    position,
                    event,
                    rawPayload,
                    result.failure()
            );
        }
    }

    /**
     * The database permanently refused this alert (for example a constraint violation).
     * Capture the payload exactly as Kafka delivered it, with the real SQLState. Using the
     * raw payload means no second serialization step can lose the alert, and the dead letter
     * keeps fields this consumer does not know about.
     */
    private void deadLetterRejectedInsert(
            RecordPosition position,
            AnomalyEvent event,
            String rawPayload,
            SQLException failure
    ) throws SQLException, InterruptedException {

        log.warn(
                "Insert rejected permanently topic={} partition={} offset={} sensorId={} sqlState={}",
                position.topic(),
                position.partition(),
                position.offset(),
                LogSanitizer.safe(event.sensorId()),
                failure.getSQLState()
        );

        sink.deadLetter(position, new DeadLetter(
                event.sensorId(),
                rawPayload,
                failure.getSQLState(),
                failure.getMessage(),
                "INSERT_REJECTED"
        ));
        metrics.deadLettered("INSERT_REJECTED");
    }
}
