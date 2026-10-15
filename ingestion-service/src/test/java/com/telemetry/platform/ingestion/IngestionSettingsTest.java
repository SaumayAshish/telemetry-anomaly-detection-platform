package com.telemetry.platform.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class IngestionSettingsTest {

    @Test
    void usesTheLocalDefaultWhenTheVariableIsNotSet() {
        assertThat(IngestionSettings.bootstrapServers(name -> null)).isEqualTo("localhost:9092");
    }

    @Test
    void usesTheLocalDefaultWhenTheVariableIsBlank() {
        Map<String, String> env = Map.of("KAFKA_BOOTSTRAP_SERVERS", "   ");

        assertThat(IngestionSettings.bootstrapServers(env::get)).isEqualTo("localhost:9092");
    }

    @Test
    void usesTheVariableWhenItIsSet() {
        Map<String, String> env = Map.of("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092");

        assertThat(IngestionSettings.bootstrapServers(env::get)).isEqualTo("kafka:19092");
    }

    @Test
    void trimsWhitespaceAroundTheValue() {
        Map<String, String> env = Map.of("KAFKA_BOOTSTRAP_SERVERS", "  kafka:19092 ");

        assertThat(IngestionSettings.bootstrapServers(env::get)).isEqualTo("kafka:19092");
    }
}
