package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.telemetry.platform.alerting.AlertWriter.DeadLetter;
import com.telemetry.platform.alerting.AlertWriter.PersistResult;
import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Path;
import jakarta.validation.Validator;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RecordProcessorTest {

    private static final RecordPosition POSITION =
            new RecordPosition("telemetry.sensor.anomalies.v1", 2, 17L);

    private static final AnomalyEvent EVENT = new AnomalyEvent(
            "sensor-1", Instant.parse("2026-10-13T10:15:30Z"), 9.5, 2.0, 1.5, 5.0, 3L, AnomalySeverity.HIGH);

    private final ObjectMapper mapper = RecordProcessor.newObjectMapper();
    private final RecordingSink sink = new RecordingSink();

    @Test
    void persistsAValidEvent() throws Exception {
        String payload = mapper.writeValueAsString(EVENT);

        processor(noViolations()).process(POSITION, payload);

        assertThat(sink.persisted).hasSize(1);
        assertThat(sink.persisted.get(0).position()).isEqualTo(POSITION);
        assertThat(sink.persisted.get(0).event()).isEqualTo(EVENT);
        assertThat(sink.deadLetters).isEmpty();
    }

    @Test
    void ignoresFieldsAddedByNewerProducers() throws Exception {
        String json = mapper.writeValueAsString(EVENT);
        String withExtraField = json.substring(0, json.length() - 1) + ",\"futureField\":1}";

        processor(noViolations()).process(POSITION, withExtraField);

        assertThat(sink.persisted).hasSize(1);
        assertThat(sink.deadLetters).isEmpty();
    }

    @Test
    void treatsADuplicateAsDoneWithoutDeadLettering() throws Exception {
        sink.nextResult = PersistResult.duplicate();

        processor(noViolations()).process(POSITION, mapper.writeValueAsString(EVENT));

        assertThat(sink.persisted).hasSize(1);
        assertThat(sink.deadLetters).isEmpty();
    }

    @Test
    void deadLettersMalformedJsonWithAnUnknownSensor() throws Exception {
        processor(noViolations()).process(POSITION, "{not json");

        assertThat(sink.persisted).isEmpty();
        assertThat(sink.deadLetters).hasSize(1);
        assertThat(sink.deadLetters.get(0).position()).isEqualTo(POSITION);

        DeadLetter letter = sink.deadLetters.get(0).letter();
        assertThat(letter.sensorId()).isEqualTo("UNKNOWN");
        assertThat(letter.payload()).isEqualTo("{not json");
        assertThat(letter.sqlState()).isEqualTo("DESERIALIZATION");
        assertThat(letter.reason()).isEqualTo("DESERIALIZATION");
        assertThat(letter.errorMessage()).isNotBlank();
    }

    @Test
    void deadLettersAJsonNullLiteral() throws Exception {
        processor(noViolations()).process(POSITION, "null");

        assertThat(sink.persisted).isEmpty();
        assertThat(sink.deadLetters).hasSize(1);

        DeadLetter letter = sink.deadLetters.get(0).letter();
        assertThat(letter.sqlState()).isEqualTo("DESERIALIZATION");
        assertThat(letter.errorMessage()).isEqualTo("Payload deserialized to null (JSON null literal)");
    }

    @Test
    void deadLettersARecordWithNoPayload() throws Exception {
        processor(noViolations()).process(POSITION, null);

        assertThat(sink.persisted).isEmpty();
        assertThat(sink.deadLetters).hasSize(1);
        assertThat(sink.deadLetters.get(0).letter().sqlState()).isEqualTo("DESERIALIZATION");
        assertThat(sink.deadLetters.get(0).letter().payload()).isNull();
    }

    @Test
    void deadLettersAnInvalidEventWithSortedViolations() throws Exception {
        String payload = mapper.writeValueAsString(EVENT);
        Validator validator = violations("zScore=must be finite", "value=must not be NaN");

        processor(validator).process(POSITION, payload);

        assertThat(sink.persisted).isEmpty();
        assertThat(sink.deadLetters).hasSize(1);

        DeadLetter letter = sink.deadLetters.get(0).letter();
        assertThat(letter.sensorId()).isEqualTo("sensor-1");
        assertThat(letter.payload()).isEqualTo(payload);
        assertThat(letter.sqlState()).isEqualTo("VALIDATION");
        assertThat(letter.reason()).isEqualTo("VALIDATION");
        assertThat(letter.errorMessage()).isEqualTo("value: must not be NaN; zScore: must be finite");
    }

    @Test
    void deadLettersARejectedInsertWithTheRealSqlState() throws Exception {
        sink.nextResult = PersistResult.rejected(new SQLException("unknown sensor", "23503"));

        processor(noViolations()).process(POSITION, mapper.writeValueAsString(EVENT));

        assertThat(sink.persisted).hasSize(1);
        assertThat(sink.deadLetters).hasSize(1);

        DeadLetter letter = sink.deadLetters.get(0).letter();
        assertThat(letter.sensorId()).isEqualTo("sensor-1");
        assertThat(letter.payload()).isEqualTo(mapper.writeValueAsString(EVENT));
        assertThat(letter.sqlState()).isEqualTo("23503");
        assertThat(letter.errorMessage()).isEqualTo("unknown sensor");
        assertThat(letter.reason()).isEqualTo("INSERT_REJECTED");
    }

    // Regression guard for the old lost-alert gap: the dead letter must carry the raw
    // payload, so a failing re-serialization can never drop the alert.
    @Test
    void deadLettersTheRawPayloadEvenWhenTheEventCannotBeReserialized() throws Exception {
        String payload = mapper.writeValueAsString(EVENT);
        sink.nextResult = PersistResult.rejected(new SQLException("unknown sensor", "23503"));
        RecordProcessor processor =
                new RecordProcessor(sink, new UnserializableMapper(), noViolations(), AlertMetrics.create());

        processor.process(POSITION, payload);

        assertThat(sink.persisted).hasSize(1);
        assertThat(sink.deadLetters).hasSize(1);

        DeadLetter letter = sink.deadLetters.get(0).letter();
        assertThat(letter.payload()).isEqualTo(payload);
        assertThat(letter.sqlState()).isEqualTo("23503");
        assertThat(letter.reason()).isEqualTo("INSERT_REJECTED");
    }
    @Test
    void deadLettersARejectedEventExactlyAsReceived() throws Exception {
        String json = mapper.writeValueAsString(EVENT);
        String withExtraField = json.substring(0, json.length() - 1) + ",\"futureField\":1}";
        sink.nextResult = PersistResult.rejected(new SQLException("unknown sensor", "23503"));

        processor(noViolations()).process(POSITION, withExtraField);

        assertThat(sink.deadLetters).hasSize(1);
        assertThat(sink.deadLetters.get(0).letter().payload()).isEqualTo(withExtraField);
    }
    @Test
    void propagatesADatabaseFailureFromTheSink() throws Exception {
        sink.persistFailure = new SQLException("cannot reconnect", "08001");
        String payload = mapper.writeValueAsString(EVENT);
        RecordProcessor processor = processor(noViolations());

        assertThatThrownBy(() -> processor.process(POSITION, payload)).isInstanceOf(SQLException.class);
    }

    private RecordProcessor processor(Validator validator) {
        return new RecordProcessor(sink, mapper, validator, AlertMetrics.create());
    }

    private static Validator noViolations() {
        return violations();
    }

    /** A validator that reports exactly these violations, each written as "path=message". */
    private static Validator violations(String... pathEqualsMessage) {
        Set<ConstraintViolation<AnomalyEvent>> reported = new LinkedHashSet<>();
        for (String entry : pathEqualsMessage) {
            int split = entry.indexOf('=');
            reported.add(violation(entry.substring(0, split), entry.substring(split + 1)));
        }

        return (Validator) Proxy.newProxyInstance(
                Validator.class.getClassLoader(),
                new Class<?>[] {Validator.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("validate")) {
                        return reported;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    @SuppressWarnings("unchecked")
    private static ConstraintViolation<AnomalyEvent> violation(String path, String message) {
        Path propertyPath = (Path) Proxy.newProxyInstance(
                Path.class.getClassLoader(),
                new Class<?>[] {Path.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "toString" -> path;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });

        return (ConstraintViolation<AnomalyEvent>) Proxy.newProxyInstance(
                ConstraintViolation.class.getClassLoader(),
                new Class<?>[] {ConstraintViolation.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getPropertyPath" -> propertyPath;
                    case "getMessage" -> message;
                    case "toString" -> path + "=" + message;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    /** Reads events normally but cannot write them, to reach the re-serialization failure. */
    private static final class UnserializableMapper extends ObjectMapper {

        private static final long serialVersionUID = 1L;

        UnserializableMapper() {
            registerModule(new JavaTimeModule());
        }

        @Override
        public String writeValueAsString(Object value) throws JsonProcessingException {
            throw new JsonProcessingException("cannot serialize") { };
        }
    }

    private static final class RecordingSink implements AlertSink {

        record Persisted(RecordPosition position, AnomalyEvent event) {
        }

        record Dead(RecordPosition position, DeadLetter letter) {
        }

        final List<Persisted> persisted = new ArrayList<>();
        final List<Dead> deadLetters = new ArrayList<>();
        PersistResult nextResult = PersistResult.inserted();
        SQLException persistFailure;

        @Override
        public PersistResult persist(RecordPosition position, AnomalyEvent event) throws SQLException {
            persisted.add(new Persisted(position, event));
            if (persistFailure != null) {
                throw persistFailure;
            }
            return nextResult;
        }

        @Override
        public void deadLetter(RecordPosition position, DeadLetter letter) {
            deadLetters.add(new Dead(position, letter));
        }
    }
}