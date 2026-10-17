package com.telemetry.platform.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

/**
 * The producer half of the AnomalyEvent wire contract. The alerting service's test of the same
 * name holds the same golden document, so a format change on one side fails exactly one of the two.
 */
class AnomalyEventWireFormatTest {

    // Keep identical to GOLDEN in the alerting-service test of the same name.
    private static final String GOLDEN = """
            {
              "sensorId": "S-1001",
              "readingTimestamp": "2026-10-12T10:00:00.123456789Z",
              "value": 200.5,
              "baselineMean": 75.25,
              "baselineStdDev": 5.0,
              "zScore": 24.9,
              "consecutiveAnomalies": 3,
              "severity": "CRITICAL"
            }
            """;

    private static final AnomalyEvent EVENT = new AnomalyEvent(
            "S-1001", Instant.parse("2026-10-12T10:00:00.123456789Z"),
            200.5, 75.25, 5.0, 24.9, 3, AnomalySeverity.CRITICAL);

    private final Serde<AnomalyEvent> serde = AnomalyDetectionStreamApp.jsonSerde(AnomalyEvent.class);

    @Test
    void theStreamAppWritesTheAgreedJson() throws Exception {
        byte[] bytes = serde.serializer().serialize(AnomalyDetectionStreamApp.ANOMALY_TOPIC, EVENT);

        ObjectMapper tree = new ObjectMapper();
        assertThat(tree.readTree(bytes)).isEqualTo(tree.readTree(GOLDEN));
    }

    @Test
    void everySeverityIsWrittenByName() throws Exception {
        ObjectMapper tree = new ObjectMapper();
        for (AnomalySeverity severity : AnomalySeverity.values()) {
            AnomalyEvent event = new AnomalyEvent(
                    "S-1001", EVENT.readingTimestamp(), 1.0, 2.0, 3.0, 4.0, 5, severity);

            byte[] bytes = serde.serializer().serialize(AnomalyDetectionStreamApp.ANOMALY_TOPIC, event);
            assertThat(tree.readTree(bytes).get("severity").asText()).isEqualTo(severity.name());
        }
    }
}
