package com.telemetry.platform.alerting;

import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class AnomalyEventValidationTest {

    private static final Validator VALIDATOR =
            Validation.buildDefaultValidatorFactory().getValidator();

    private static AnomalyEvent event(String sensorId, Instant ts, double std, long streak,
                                      AnomalySeverity severity) {
        return new AnomalyEvent(sensorId, ts, 200.0, 75.0, std, 21.5, streak, severity);
    }

    private static Set<String> violatedProperties(AnomalyEvent e) {
        return VALIDATOR.validate(e).stream()
                .map(ConstraintViolation::getPropertyPath)
                .map(Object::toString)
                .collect(Collectors.toSet());
    }

    private static final Instant NOW = Instant.parse("2026-10-12T10:00:00Z");

    @Test
    void aFullyPopulatedEventHasNoViolations() {
        assertThat(violatedProperties(event("S-1001", NOW, 5.0, 1, AnomalySeverity.CRITICAL))).isEmpty();
    }

    @Test
    void aBlankSensorIdIsRejected() {
        assertThat(violatedProperties(event("  ", NOW, 5.0, 1, AnomalySeverity.LOW)))
                .containsExactly("sensorId");
    }

    @Test
    void aMissingTimestampAndMissingSeverityAreBothRejected() {
        assertThat(violatedProperties(event("S-1001", null, 5.0, 1, null)))
                .containsExactlyInAnyOrder("readingTimestamp", "severity");
    }

    @Test
    void negativeStdDevAndNegativeStreakAreRejected() {
        assertThat(violatedProperties(event("S-1001", NOW, -1.0, -1, AnomalySeverity.LOW)))
                .containsExactlyInAnyOrder("baselineStdDev", "consecutiveAnomalies");
    }

    @Test
    void aZeroStdDevIsAllowed() {
        assertThat(violatedProperties(event("S-1001", NOW, 0.0, 0, AnomalySeverity.LOW))).isEmpty();
    }
}