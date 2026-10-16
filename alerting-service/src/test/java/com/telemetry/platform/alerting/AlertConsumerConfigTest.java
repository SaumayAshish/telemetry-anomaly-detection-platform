package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AlertConsumerConfigTest {

    private static final Map<String, String> REQUIRED = Map.of(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:19092",
            "DB_URL", "jdbc:postgresql://db:5432/telemetry",
            "DB_USER", "app",
            "DB_PASSWORD", "s3cret-pw");

    private static AlertConsumerConfig loadWith(String name, String value) {
        Map<String, String> env = new HashMap<>(REQUIRED);
        env.put(name, value);
        return AlertConsumerConfig.load(env::get);
    }

    @Test
    void readsTheRequiredVariables() {
        AlertConsumerConfig config = AlertConsumerConfig.load(REQUIRED::get);

        assertThat(config.kafkaBootstrapServers()).isEqualTo("kafka:19092");
        assertThat(config.dbUrl()).isEqualTo("jdbc:postgresql://db:5432/telemetry");
        assertThat(config.dbUser()).isEqualTo("app");
        assertThat(config.dbPassword()).isEqualTo("s3cret-pw");
    }

    @Test
    void theFallbackPathDefaultsToTheWorkingDirectory() {
        assertThat(AlertConsumerConfig.load(REQUIRED::get).dlqFallbackPath())
                .isEqualTo("dlq-fallback.jsonl");
    }

    @Test
    void theFallbackPathCanBeSet() {
        assertThat(loadWith("DLQ_FALLBACK_PATH", "/data/dlq.jsonl").dlqFallbackPath())
                .isEqualTo("/data/dlq.jsonl");
    }

    @Test
    void aBlankFallbackPathMeansTheDefault() {
        assertThat(loadWith("DLQ_FALLBACK_PATH", "   ").dlqFallbackPath())
                .isEqualTo("dlq-fallback.jsonl");
    }

    @Test
    void theFallbackPathIsTrimmed() {
        assertThat(loadWith("DLQ_FALLBACK_PATH", "  /data/dlq.jsonl ").dlqFallbackPath())
                .isEqualTo("/data/dlq.jsonl");
    }

    @Test
    void everyMissingRequiredVariableIsNamedInOneError() {
        assertThatThrownBy(() -> AlertConsumerConfig.load(name -> null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("KAFKA_BOOTSTRAP_SERVERS")
                .hasMessageContaining("DB_URL")
                .hasMessageContaining("DB_USER")
                .hasMessageContaining("DB_PASSWORD");
    }

    @Test
    void toStringHidesThePasswordAndShowsThePath() {
        String text = AlertConsumerConfig.load(REQUIRED::get).toString();

        assertThat(text).doesNotContain("s3cret-pw").contains("dbPassword=***");
        assertThat(text).contains("dlqFallbackPath=dlq-fallback.jsonl");
    }

    @Test
    void theFourArgumentConstructorUsesTheDefaultPath() {
        AlertConsumerConfig config = new AlertConsumerConfig("k:9092", "jdbc:x", "u", "p");

        assertThat(config.dlqFallbackPath()).isEqualTo("dlq-fallback.jsonl");
    }
}
