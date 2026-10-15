package com.telemetry.platform.streams;

import java.util.function.Function;

/** Runtime settings for the stream app, read from the environment in one place. */
final class StreamsSettings {

    static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private StreamsSettings() {
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