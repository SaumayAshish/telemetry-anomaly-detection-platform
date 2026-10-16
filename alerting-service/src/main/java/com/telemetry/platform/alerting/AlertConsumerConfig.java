package com.telemetry.platform.alerting;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public record AlertConsumerConfig(
        String kafkaBootstrapServers,
        String dbUrl,
        String dbUser,
        String dbPassword,
        String dlqFallbackPath) {

    static final String DEFAULT_DLQ_FALLBACK_PATH = "dlq-fallback.jsonl";

    // The original four-argument form, for callers that do not care where the fallback file goes.
    public AlertConsumerConfig(String kafkaBootstrapServers, String dbUrl, String dbUser, String dbPassword) {
        this(kafkaBootstrapServers, dbUrl, dbUser, dbPassword, DEFAULT_DLQ_FALLBACK_PATH);
    }
    public static AlertConsumerConfig fromEnvironment() {
        return load(System::getenv);
    }

    // Package-private so a harness can supply a fake environment.
    static AlertConsumerConfig load(Function<String, String> env) {

        List<String> missing = new ArrayList<>();

        String kafka = require(env, "KAFKA_BOOTSTRAP_SERVERS", missing);
        String url = require(env, "DB_URL", missing);
        String user = require(env, "DB_USER", missing);
        String password = require(env, "DB_PASSWORD", missing);

        if (!missing.isEmpty()) {
            throw new IllegalStateException(
                    "Missing required environment variables: " + String.join(", ", missing));
        }

        String fallbackPath = optional(env, "DLQ_FALLBACK_PATH", DEFAULT_DLQ_FALLBACK_PATH);

        return new AlertConsumerConfig(kafka, url, user, password, fallbackPath);
    }

    private static String optional(Function<String, String> env, String name, String fallback) {
        String value = env.apply(name);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static String require(Function<String, String> env, String name, List<String> missing) {
        String value = env.apply(name);
        if (value == null || value.isBlank()) {
            missing.add(name);
            return null;
        }
        return value;
    }

    // A record's generated toString would print the password. Redact it.
    @Override
    public String toString() {
        return "AlertConsumerConfig[kafkaBootstrapServers=" + kafkaBootstrapServers
                + ", dbUrl=" + dbUrl
                + ", dbUser=" + dbUser
                + ", dbPassword=***"
                + ", dlqFallbackPath=" + dlqFallbackPath + "]";
    }
}