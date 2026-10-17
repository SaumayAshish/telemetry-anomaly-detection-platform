package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * The consumer half of the AnomalyEvent wire contract. The stream app's test of the same name
 * holds the same golden document, so a format change on one side fails exactly one of the two.
 */
class AnomalyEventWireFormatTest {

    // Keep identical to GOLDEN in the stream-processing-service test of the same name.
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

    private final ObjectMapper mapper = RecordProcessor.newObjectMapper();

    @Test
    void theAlertingMapperReadsTheAgreedJson() throws Exception {
        assertThat(mapper.readValue(GOLDEN, AnomalyEvent.class)).isEqualTo(EVENT);
    }

    @Test
    void theAlertingMapperWritesTheAgreedJson() throws Exception {
        assertThat(mapper.readTree(mapper.writeValueAsString(EVENT))).isEqualTo(mapper.readTree(GOLDEN));
    }

    @Test
    void fieldsAddedByANewerProducerAreIgnored() throws Exception {
        ObjectNode withExtra = (ObjectNode) mapper.readTree(GOLDEN);
        withExtra.put("addedLater", "x");

        assertThat(mapper.readValue(withExtra.toString(), AnomalyEvent.class)).isEqualTo(EVENT);
    }

    @Test
    void everySeveritySurvivesARoundTrip() throws Exception {
        for (AnomalySeverity severity : AnomalySeverity.values()) {
            AnomalyEvent event = new AnomalyEvent(
                    "S-1001", EVENT.readingTimestamp(), 1.0, 2.0, 3.0, 4.0, 5, severity);

            String json = mapper.writeValueAsString(event);
            assertThat(mapper.readValue(json, AnomalyEvent.class)).isEqualTo(event);
        }
    }
}
