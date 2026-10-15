package com.telemetry.platform.streams;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class StreamsSettingsTest {

    @Test
    void usesTheLocalDefaultWhenTheVariableIsNotSet() {
        assertThat(StreamsSettings.bootstrapServers(name -> null)).isEqualTo("localhost:9092");
    }

    @Test
    void usesTheLocalDefaultWhenTheVariableIsBlank() {
        Map<String, String> env = Map.of("KAFKA_BOOTSTRAP_SERVERS", "   ");

        assertThat(StreamsSettings.bootstrapServers(env::get)).isEqualTo("localhost:9092");
    }

    @Test
    void usesTheVariableWhenItIsSet() {
        Map<String, String> env = Map.of("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092");

        assertThat(StreamsSettings.bootstrapServers(env::get)).isEqualTo("kafka:19092");
    }

    @Test
    void trimsWhitespaceAroundTheValue() {
        Map<String, String> env = Map.of("KAFKA_BOOTSTRAP_SERVERS", "  kafka:19092 ");

        assertThat(StreamsSettings.bootstrapServers(env::get)).isEqualTo("kafka:19092");
    }
}