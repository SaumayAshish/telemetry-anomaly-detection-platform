package com.telemetry.platform.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.function.Function;
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

    private static Function<String, String> env(String name, String value) {
        Map<String, String> values = Map.of(name, value);
        return values::get;
    }

    @Test
    void simulationIsLegacyWhenTheScenarioIsNotSet() {
        assertThat(IngestionSettings.simulation(name -> null).demo()).isFalse();
    }

    @Test
    void simulationIsLegacyWhenTheScenarioIsBlank() {
        assertThat(IngestionSettings.simulation(env("SIMULATION_SCENARIO", "   ")).demo()).isFalse();
    }

    @Test
    void simulationIsLegacyWhenAskedForExplicitly() {
        assertThat(IngestionSettings.simulation(env("SIMULATION_SCENARIO", "legacy")).demo()).isFalse();
    }

    @Test
    void simulationIsDemoWhenAskedFor() {
        assertThat(IngestionSettings.simulation(env("SIMULATION_SCENARIO", " Demo ")).demo()).isTrue();
    }

    @Test
    void rejectsAnUnknownScenario() {
        assertThatThrownBy(() -> IngestionSettings.simulation(env("SIMULATION_SCENARIO", "chaos")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SIMULATION_SCENARIO");
    }

    @Test
    void theSeedDefaultsTo42() {
        assertThat(IngestionSettings.simulation(name -> null).seed()).isEqualTo(42L);
    }

    @Test
    void theSeedIsReadFromTheEnvironment() {
        assertThat(IngestionSettings.simulation(env("SIMULATION_SEED", "7")).seed()).isEqualTo(7L);
    }

    @Test
    void rejectsANonNumericSeed() {
        assertThatThrownBy(() -> IngestionSettings.simulation(env("SIMULATION_SEED", "abc")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SIMULATION_SEED");
    }

    @Test
    void theIntervalDefaultsToTwoSeconds() {
        assertThat(IngestionSettings.simulation(name -> null).intervalMillis()).isEqualTo(2000L);
    }

    @Test
    void theIntervalIsReadFromTheEnvironment() {
        assertThat(IngestionSettings.simulation(env("SIMULATION_INTERVAL_MS", "500")).intervalMillis())
                .isEqualTo(500L);
    }

    @Test
    void rejectsAZeroInterval() {
        assertThatThrownBy(() -> IngestionSettings.simulation(env("SIMULATION_INTERVAL_MS", "0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SIMULATION_INTERVAL_MS");
    }

    @Test
    void rejectsANegativeInterval() {
        assertThatThrownBy(() -> IngestionSettings.simulation(env("SIMULATION_INTERVAL_MS", "-5")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SIMULATION_INTERVAL_MS");
    }

    @Test
    void rejectsANonNumericInterval() {
        assertThatThrownBy(() -> IngestionSettings.simulation(env("SIMULATION_INTERVAL_MS", "fast")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SIMULATION_INTERVAL_MS");
    }

    @Test
    void theGroundTruthPathDefaultsToAFileInTheWorkingDirectory() {
        assertThat(IngestionSettings.simulation(name -> null).groundTruthPath()).isEqualTo("ground-truth.csv");
    }

    @Test
    void theGroundTruthPathIsReadFromTheEnvironment() {
        assertThat(IngestionSettings.simulation(env("SIMULATION_GROUND_TRUTH_PATH", "/data/ground-truth.csv"))
                .groundTruthPath()).isEqualTo("/data/ground-truth.csv");
    }

    @Test
    void aBlankGroundTruthPathUsesTheDefault() {
        assertThat(IngestionSettings.simulation(env("SIMULATION_GROUND_TRUTH_PATH", "   ")).groundTruthPath())
                .isEqualTo("ground-truth.csv");
    }

    @Test
    void numbersMayHaveSurroundingWhitespace() {
        Map<String, String> values = Map.of("SIMULATION_SEED", " 7 ", "SIMULATION_INTERVAL_MS", " 500 ");

        IngestionSettings.SimulationSettings settings = IngestionSettings.simulation(values::get);

        assertThat(settings.seed()).isEqualTo(7L);
        assertThat(settings.intervalMillis()).isEqualTo(500L);
    }

    @Test
    void theGroundTruthPathIsTrimmed() {
        assertThat(IngestionSettings.simulation(env("SIMULATION_GROUND_TRUTH_PATH", "  /data/x.csv ")).groundTruthPath())
                .isEqualTo("/data/x.csv");
    }
}
