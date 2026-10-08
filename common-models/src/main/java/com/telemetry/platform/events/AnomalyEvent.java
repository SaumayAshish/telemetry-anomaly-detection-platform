package com.telemetry.platform.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;

public record AnomalyEvent(
        @NotBlank String sensorId,
        @NotNull Instant readingTimestamp,
        double value,
        double baselineMean,
        @PositiveOrZero double baselineStdDev,
        double zScore,
        @PositiveOrZero long consecutiveAnomalies,
        @NotNull AnomalySeverity severity
) {
}