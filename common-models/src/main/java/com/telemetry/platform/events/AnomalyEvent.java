package com.telemetry.platform.events;
import java.time.Instant;

public record AnomalyEvent(
        String sensorId,
        Instant readingTimestamp,
        double value,
        double baselineMean,
        double baselineStdDev,
        double zScore,
        long consecutiveAnomalies ) {}