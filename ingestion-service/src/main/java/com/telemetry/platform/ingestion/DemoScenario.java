package com.telemetry.platform.ingestion;

import com.telemetry.platform.ingestion.ReadingSimulator.GroundTruthWindow;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * The readings of the demo scenario tick by tick, and the ground-truth record of its faults. Tick t is
 * stamped start + t * interval and carries sequence number t + 1, so a run is fully described by its
 * start time, interval and seed.
 */
final class DemoScenario {

    private static final String HEADER = "sensor_id,fault_type,from,to,magnitude_sigmas";

    private final ReadingSimulator simulator;
    private final Instant start;
    private final Duration interval;

    DemoScenario(ReadingSimulator simulator, Instant start, Duration interval) {
        this.simulator = simulator;
        this.start = start;
        this.interval = interval;
    }

    long totalTicks() {
        return ReadingSimulator.DEMO_TICKS;
    }

    List<SensorReading> readingsAt(long tick) {
        if (tick < 0 || tick >= totalTicks()) {
            throw new IllegalArgumentException("tick " + tick + " is outside 0.." + (totalTicks() - 1));
        }
        Instant timestamp = start.plus(interval.multipliedBy(tick));
        return simulator.sensorIds().stream()
                .map(sensorId -> new SensorReading(
                        sensorId, timestamp, "temperature", simulator.valueAt(sensorId, tick), tick + 1))
                .toList();
    }

    String groundTruthCsv() {
        StringBuilder csv = new StringBuilder(HEADER).append('\n');
        for (GroundTruthWindow window : simulator.groundTruth(start, interval)) {
            csv.append(window.sensorId()).append(',')
                    .append(window.type()).append(',')
                    .append(window.from()).append(',')
                    .append(window.to()).append(',')
                    .append(window.magnitudeSigmas()).append('\n');
        }
        return csv.toString();
    }
}