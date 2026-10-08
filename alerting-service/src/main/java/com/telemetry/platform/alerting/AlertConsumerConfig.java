package com.telemetry.platform.alerting;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public record AlertConsumerConfig(
        String kafkaBootstrapServers,
        String dbUrl,
        String dbUser,
        String dbPassword) {

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

        return new AlertConsumerConfig(kafka, url, user, password);
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
                + ", dbPassword=***]";
    }
}