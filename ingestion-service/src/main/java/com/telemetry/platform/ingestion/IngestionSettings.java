package com.telemetry.platform.ingestion;

import java.util.Locale;
import java.util.function.Function;

/** Runtime settings for the producer, read from the environment in one place. */
final class IngestionSettings {

    static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private IngestionSettings() {
    }

    /** How the producer simulates readings; the defaults keep the original behaviour. */
    record SimulationSettings(boolean demo, long seed, long intervalMillis, String groundTruthPath) {
    }

    static final long DEFAULT_SEED = 42L;
    static final long DEFAULT_INTERVAL_MILLIS = 2000L;
    static final String DEFAULT_GROUND_TRUTH_PATH = "ground-truth.csv";

    static SimulationSettings simulation(Function<String, String> env) {
        return new SimulationSettings(
                demoRequested(env.apply("SIMULATION_SCENARIO")),
                seed(env.apply("SIMULATION_SEED")),
                intervalMillis(env.apply("SIMULATION_INTERVAL_MS")),
                orDefault(env.apply("SIMULATION_GROUND_TRUTH_PATH"), DEFAULT_GROUND_TRUTH_PATH));
    }

    private static boolean demoRequested(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        String scenario = value.trim().toLowerCase(Locale.ROOT);
        if (scenario.equals("legacy")) {
            return false;
        }
        if (scenario.equals("demo")) {
            return true;
        }
        throw new IllegalArgumentException(
                "SIMULATION_SCENARIO must be 'legacy' or 'demo' but was '" + value.trim() + "'");
    }

    private static long seed(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_SEED;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "SIMULATION_SEED must be a whole number but was '" + value.trim() + "'", e);
        }
    }

    private static long intervalMillis(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_INTERVAL_MILLIS;
        }
        long millis;
        try {
            millis = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "SIMULATION_INTERVAL_MS must be a whole number of milliseconds but was '" + value.trim() + "'", e);
        }
        if (millis <= 0) {
            throw new IllegalArgumentException(
                    "SIMULATION_INTERVAL_MS must be positive but was " + millis);
        }
        return millis;
    }

    private static String orDefault(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    static String bootstrapServers() {
        return bootstrapServers(System::getenv);
    }

    static String bootstrapServers(Function<String, String> env) {
        String value = env.apply("KAFKA_BOOTSTRAP_SERVERS");
        if (value == null || value.isBlank()) {
            return DEFAULT_BOOTSTRAP_SERVERS;
        }
        return value.trim();
    }
}
