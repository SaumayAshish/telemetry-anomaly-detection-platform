package com.telemetry.platform.api.alert;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Pattern;

import java.time.Instant;
import java.util.Locale;

public record AlertQuery(
        @Pattern(regexp = "\\S.{0,63}",
                message = "sensorId must be 1 to 64 characters and must not start with whitespace")
        String sensorId,

        @ValidSeverity
        String severity,

        Instant from,

        Instant to) {

    public String normalizedSeverity() {
        return severity == null ? null : severity.toUpperCase(Locale.ROOT);
    }

    @AssertTrue(message = "'from' and 'to' must be provided together")
    public boolean isRangeComplete() {
        return (from == null) == (to == null);
    }

    @AssertTrue(message = "'from' must not be later than 'to'")
    public boolean isRangeOrdered() {
        return from == null || to == null || !from.isAfter(to);
    }

    @AssertTrue(message = "a time range cannot be combined with sensorId or severity")
    public boolean isRangeStandalone() {
        return (from == null && to == null) || (sensorId == null && severity == null);
    }
}