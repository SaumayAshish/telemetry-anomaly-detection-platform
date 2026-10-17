package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.platform.alerting.AlertWriter.DeadLetter;
import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileDeadLetterFallbackTest {

    private static final RecordPosition POSITION =
            new RecordPosition("telemetry.sensor.anomalies.v1", 2, 17L);

    private static final DeadLetter LETTER =
            new DeadLetter("sensor-1", "{\"bad\":true}", "22001", "value too long", "VALIDATION");

    private static final SQLException DLQ_FAILURE = new SQLException("dlq table down", "08006");

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path dir;

    @Test
    void writesOneJsonLineWithEveryField() throws Exception {
        Path file = dir.resolve("dlq.jsonl");
        Instant before = Instant.now();

        new FileDeadLetterFallback(file, mapper).write(POSITION, LETTER, DLQ_FAILURE);

        Instant after = Instant.now();
        List<String> lines = lines(file);
        assertThat(lines).hasSize(1);
        JsonNode record = mapper.readTree(lines.get(0));
        List<String> names = new ArrayList<>();
        record.fieldNames().forEachRemaining(names::add);
        assertThat(names).containsExactlyInAnyOrder(
                "kafkaTopic", "kafkaPartition", "kafkaOffset", "sensorId", "payload", "sqlState",
                "errorMessage", "dlqWriteFailureSqlState", "dlqWriteFailureReason", "failedAt");
        assertThat(record.get("kafkaTopic").asText()).isEqualTo("telemetry.sensor.anomalies.v1");
        assertThat(record.get("kafkaPartition").asInt()).isEqualTo(2);
        assertThat(record.get("kafkaOffset").asLong()).isEqualTo(17L);
        assertThat(record.get("sensorId").asText()).isEqualTo("sensor-1");
        assertThat(record.get("payload").asText()).isEqualTo("{\"bad\":true}");
        assertThat(record.get("sqlState").asText()).isEqualTo("22001");
        assertThat(record.get("errorMessage").asText()).isEqualTo("value too long");
        assertThat(record.get("dlqWriteFailureSqlState").asText()).isEqualTo("08006");
        assertThat(record.get("dlqWriteFailureReason").asText()).isEqualTo("dlq table down");
        assertThat(Instant.parse(record.get("failedAt").asText())).isBetween(before, after);
    }

    @Test
    void laterRecordsAreAppendedAndEarlierOnesKept() throws Exception {
        Path file = dir.resolve("dlq.jsonl");
        FileDeadLetterFallback fallback = new FileDeadLetterFallback(file, mapper);
        DeadLetter second = new DeadLetter("sensor-2", "{}", "22001", "again", "VALIDATION");

        fallback.write(POSITION, LETTER, DLQ_FAILURE);
        fallback.write(new RecordPosition("telemetry.sensor.anomalies.v1", 2, 18L), second, DLQ_FAILURE);

        List<String> lines = lines(file);
        assertThat(lines).hasSize(2);
        assertThat(mapper.readTree(lines.get(0)).get("sensorId").asText()).isEqualTo("sensor-1");
        assertThat(mapper.readTree(lines.get(1)).get("sensorId").asText()).isEqualTo("sensor-2");
    }

    @Test
    void aPayloadWithNewlinesStaysOnOneLine() throws Exception {
        Path file = dir.resolve("dlq.jsonl");
        String payload = "{\n\"a\":1,\r\n\"b\":2}";
        DeadLetter letter = new DeadLetter("sensor-1", payload, "22001", "multi line", "VALIDATION");

        new FileDeadLetterFallback(file, mapper).write(POSITION, letter, DLQ_FAILURE);

        List<String> lines = lines(file);
        assertThat(lines).hasSize(1);
        assertThat(mapper.readTree(lines.get(0)).get("payload").asText()).isEqualTo(payload);
    }

    @Test
    void anUnwritableFileIsReportedAsALostRecord() {
        Path file = dir.resolve("missing-directory").resolve("dlq.jsonl");
        FileDeadLetterFallback fallback = new FileDeadLetterFallback(file, mapper);

        assertThatThrownBy(() -> fallback.write(POSITION, LETTER, DLQ_FAILURE))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void aMissingSensorIdAndSqlStateAreWrittenAsJsonNull() throws Exception {
        Path file = dir.resolve("dlq.jsonl");
        DeadLetter letter = new DeadLetter(null, "{not json", "DESERIALIZATION", "bad json", "unparseable");

        new FileDeadLetterFallback(file, mapper).write(POSITION, letter, new SQLException("down"));

        List<String> lines = lines(file);
        assertThat(lines).hasSize(1);
        JsonNode record = mapper.readTree(lines.get(0));
        assertThat(record.get("sensorId").isNull()).isTrue();
        assertThat(record.get("dlqWriteFailureSqlState").isNull()).isTrue();
        assertThat(record.get("payload").asText()).isEqualTo("{not json");
    }

    private static List<String> lines(Path file) throws IOException {
        return Files.exists(file) ? Files.readAllLines(file) : List.of();
    }
}
