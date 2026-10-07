package com.telemetry.platform.api.alert;

import java.time.Instant;

public record AlertResponse(
        Long id,
        String sensorId,
        String sensorLocation,
        String sensorModel,
        Double value,
        Double baselineMean,
        Double baselineStdDev,
        Double zScore,
        Long consecutiveAnomalies,
        String severity,
        Instant readingTimestamp,
        Instant ingestedAt) {

    static AlertResponse from(AnomalyAlert alert) {
        Sensor sensor = alert.getSensor();
        return new AlertResponse(
                alert.getId(),
                alert.getSensorId(),
                sensor.getLocation(),
                sensor.getModel(),
                alert.getValue(),
                alert.getBaselineMean(),
                alert.getBaselineStdDev(),
                alert.getZScore(),
                alert.getConsecutiveAnomalies(),
                alert.getSeverity(),
                alert.getReadingTimestamp(),
                alert.getIngestedAt());
    }
}
