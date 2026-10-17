package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** METRICS_PORT: the port of the scrape endpoint, 9404 unless told otherwise. */
class AlertConsumerConfigMetricsPortTest {

    private static AlertConsumerConfig load(String metricsPort) {
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        env.put("DB_URL", "jdbc:postgresql://db:5432/telemetry");
        env.put("DB_USER", "telemetry");
        env.put("DB_PASSWORD", "s3cret");
        if (metricsPort != null) {
            env.put("METRICS_PORT", metricsPort);
        }
        return AlertConsumerConfig.load(env::get);
    }

    @Test
    void theMetricsPortDefaultsTo9404() {
        AlertConsumerConfig config = load(null);

        assertThat(config.metricsPort()).isEqualTo(AlertConsumerConfig.DEFAULT_METRICS_PORT);
        assertThat(config.metricsPort()).isEqualTo(9404);
        assertThat(config.metricsEnabled()).isTrue();
    }

    @Test
    void aBlankMetricsPortMeansTheDefault() {
        assertThat(load("   ").metricsPort()).isEqualTo(9404);
    }

    @Test
    void theMetricsPortCanBeSet() {
        assertThat(load("9100").metricsPort()).isEqualTo(9100);
    }

    @Test
    void theMetricsPortIsTrimmed() {
        assertThat(load("  9100  ").metricsPort()).isEqualTo(9100);
    }

    @Test
    void offDisablesTheEndpointInAnyCase() {
        for (String value : new String[] {"off", "OFF", " Off "}) {
            AlertConsumerConfig config = load(value);

            assertThat(config.metricsPort()).isEqualTo(AlertConsumerConfig.METRICS_DISABLED);
            assertThat(config.metricsEnabled()).isFalse();
        }
    }

    @Test
    void aValueThatIsNotANumberIsRejectedAndNamed() {
        assertThatThrownBy(() -> load("abc"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("METRICS_PORT")
                .hasMessageContaining("abc");
    }

    @Test
    void aValueOutsideTheValidPortRangeIsRejectedAndNamed() {
        for (String value : new String[] {"0", "-5", "65536", "99999"}) {
            assertThatThrownBy(() -> load(value))
                    .as("METRICS_PORT=%s", value)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("METRICS_PORT");
        }
    }

    @Test
    void theOlderConstructorsLeaveTheEndpointDisabled() {
        AlertConsumerConfig four = new AlertConsumerConfig("k", "u", "n", "p");
        AlertConsumerConfig five = new AlertConsumerConfig("k", "u", "n", "p", "f.jsonl");

        assertThat(four.metricsEnabled()).isFalse();
        assertThat(five.metricsEnabled()).isFalse();
    }

    @Test
    void toStringShowsThePortAndStillHidesThePassword() {
        String text = load("9100").toString();

        assertThat(text).contains("metricsPort=9100").doesNotContain("s3cret");
    }
}
