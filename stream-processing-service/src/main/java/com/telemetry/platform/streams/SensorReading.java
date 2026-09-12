package com.telemetry.platform.streams;

import java.time.Instant;

public record SensorReading(
        String sensorId,
        Instant timestamp,
        String metricType,
        double value,
        long sequenceNumber
) {}

