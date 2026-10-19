package com.telemetry.platform.ingestion;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * A seeded, deterministic sensor signal: a steady baseline plus Gaussian noise, with faults injected on
 * a fixed schedule. The value of a sensor at a tick depends only on (seed, sensor, tick), never on call
 * order, so a run can be reproduced and checked against the ground truth it reports.
 */
final class ReadingSimulator {

    /** Length of the demo scenario, in ticks. */
    static final long DEMO_TICKS = 600;

    private static final long MIX = 0x9E3779B97F4A7C15L;

    enum FaultType { SPIKE, LEVEL_SHIFT, DRIFT }

    /** A sensor's normal behaviour: its mean and the standard deviation of its noise. */
    record SensorProfile(String sensorId, double baselineMean, double noiseStdDev) {
    }

    /**
     * A deviation from normal, measured in noise standard deviations. A spike lasts one tick, a level shift
     * holds the offset for its whole duration, a drift ramps linearly up to the full offset on its last tick.
     */
    record Fault(String sensorId, FaultType type, long startTick, long durationTicks, double magnitudeSigmas) {

        static Fault spike(String sensorId, long tick, double magnitudeSigmas) {
            return new Fault(sensorId, FaultType.SPIKE, tick, 1, magnitudeSigmas);
        }

        static Fault levelShift(String sensorId, long startTick, long durationTicks, double magnitudeSigmas) {
            return new Fault(sensorId, FaultType.LEVEL_SHIFT, startTick, durationTicks, magnitudeSigmas);
        }

        static Fault drift(String sensorId, long startTick, long durationTicks, double magnitudeSigmas) {
            return new Fault(sensorId, FaultType.DRIFT, startTick, durationTicks, magnitudeSigmas);
        }

        long lastTick() {
            return startTick + durationTicks - 1;
        }
    }

    /** A fault expressed in wall-clock time, so it can be joined against stored alerts. */
    record GroundTruthWindow(String sensorId, FaultType type, Instant from, Instant to, double magnitudeSigmas) {
    }

    private final long seed;
    private final Map<String, SensorProfile> profiles = new LinkedHashMap<>();
    private final List<Fault> faults;

    ReadingSimulator(long seed, List<SensorProfile> profiles, List<Fault> faults) {
        if (profiles.isEmpty()) {
            throw new IllegalArgumentException("at least one sensor profile is required");
        }
        for (SensorProfile profile : profiles) {
            if (!(profile.noiseStdDev() > 0.0)) {
                throw new IllegalArgumentException("noise must be positive for " + profile.sensorId());
            }
            if (this.profiles.putIfAbsent(profile.sensorId(), profile) != null) {
                throw new IllegalArgumentException("duplicate sensor profile: " + profile.sensorId());
            }
        }
        for (Fault fault : faults) {
            if (!this.profiles.containsKey(fault.sensorId())) {
                throw new IllegalArgumentException("fault for an unknown sensor: " + fault.sensorId());
            }
            if (fault.startTick() < 0) {
                throw new IllegalArgumentException("a fault cannot start before tick 0");
            }
            if (fault.durationTicks() < 1) {
                throw new IllegalArgumentException("a fault must last at least one tick");
            }
        }
        rejectOverlaps(faults);
        this.seed = seed;
        this.faults = List.copyOf(faults);
    }

    /** The five seeded sensors with a fixed fault schedule; the seed changes the noise, not the schedule. */
    static ReadingSimulator demo(long seed) {
        return new ReadingSimulator(seed,
                List.of(
                        new SensorProfile("S-1001", 70.0, 1.5),
                        new SensorProfile("S-1002", 75.0, 2.0),
                        new SensorProfile("S-1003", 68.0, 1.2),
                        new SensorProfile("S-1004", 80.0, 1.8),
                        new SensorProfile("S-1005", 72.0, 1.5)),
                List.of(
                        Fault.spike("S-1001", 80, 12.0),
                        Fault.levelShift("S-1001", 250, 15, 6.0),
                        Fault.spike("S-1002", 120, -8.0),
                        Fault.drift("S-1002", 300, 80, 6.0),
                        Fault.spike("S-1003", 150, 20.0),
                        Fault.levelShift("S-1003", 400, 20, -5.0),
                        Fault.levelShift("S-1004", 100, 12, 8.0),
                        Fault.spike("S-1004", 350, 4.0),
                        Fault.drift("S-1005", 200, 60, 5.0),
                        Fault.spike("S-1005", 450, 2.5)));
    }

    double valueAt(String sensorId, long tick) {
        SensorProfile profile = profiles.get(sensorId);
        if (profile == null) {
            throw new IllegalArgumentException("unknown sensor: " + sensorId);
        }
        double noise = noiseAt(sensorId, tick);
        double offset = offsetAt(sensorId, tick);
        return profile.baselineMean() + profile.noiseStdDev() * (noise + offset);
    }

    List<Fault> faults() {
        return faults;
    }

    List<String> sensorIds() {
        return List.copyOf(profiles.keySet());
    }

    List<GroundTruthWindow> groundTruth(Instant start, Duration interval) {
        return faults.stream()
                .map(fault -> new GroundTruthWindow(
                        fault.sensorId(),
                        fault.type(),
                        start.plus(interval.multipliedBy(fault.startTick())),
                        start.plus(interval.multipliedBy(fault.lastTick())),
                        fault.magnitudeSigmas()))
                .toList();
    }

    private double noiseAt(String sensorId, long tick) {
        long hash = seed;
        hash = hash * MIX + sensorId.hashCode();
        hash = hash * MIX + tick;
        return new SplittableRandom(hash).nextGaussian();
    }

    private double offsetAt(String sensorId, long tick) {
        for (Fault fault : faults) {
            if (fault.sensorId().equals(sensorId) && tick >= fault.startTick() && tick <= fault.lastTick()) {
                return fault.type() == FaultType.DRIFT
                        ? fault.magnitudeSigmas() * (tick - fault.startTick() + 1) / fault.durationTicks()
                        : fault.magnitudeSigmas();
            }
        }
        return 0.0;
    }

    private static void rejectOverlaps(List<Fault> faults) {
        List<Fault> ordered = faults.stream()
                .sorted(Comparator.comparing(Fault::sensorId).thenComparingLong(Fault::startTick))
                .toList();
        for (int i = 1; i < ordered.size(); i++) {
            Fault previous = ordered.get(i - 1);
            Fault next = ordered.get(i);
            if (previous.sensorId().equals(next.sensorId()) && next.startTick() <= previous.lastTick()) {
                throw new IllegalArgumentException("faults overlap on " + next.sensorId());
            }
        }
    }
}