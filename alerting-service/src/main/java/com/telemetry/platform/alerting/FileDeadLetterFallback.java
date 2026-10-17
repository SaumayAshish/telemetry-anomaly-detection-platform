package com.telemetry.platform.alerting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.platform.alerting.AlertWriter.DeadLetter;
import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Last-resort persistence for a dead letter the dead-letter table refuses: one JSON line appended
 * to a local file and forced to disk. If even that fails, the record is lost, and the failure is
 * thrown so the consumer does not commit past it.
 */
final class FileDeadLetterFallback implements AlertWriter.Fallback {

    private static final Logger log = LoggerFactory.getLogger(FileDeadLetterFallback.class);

    private final Path file;
    private final ObjectMapper mapper;

    FileDeadLetterFallback(Path file, ObjectMapper mapper) {
        this.file = Objects.requireNonNull(file, "file");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public void write(RecordPosition position, DeadLetter letter, SQLException deadLetterWriteFailure) {
        try {
            Map<String, Object> fallbackRecord = new LinkedHashMap<>();
            fallbackRecord.put("kafkaTopic", position.topic());
            fallbackRecord.put("kafkaPartition", position.partition());
            fallbackRecord.put("kafkaOffset", position.offset());
            fallbackRecord.put("sensorId", letter.sensorId());
            fallbackRecord.put("payload", letter.payload());
            fallbackRecord.put("sqlState", letter.sqlState());
            fallbackRecord.put("errorMessage", letter.errorMessage());
            fallbackRecord.put(
                    "dlqWriteFailureSqlState",
                    deadLetterWriteFailure.getSQLState()
            );
            fallbackRecord.put(
                    "dlqWriteFailureReason",
                    deadLetterWriteFailure.getMessage()
            );
            fallbackRecord.put("failedAt", Instant.now().toString());

            String line =
                    mapper.writeValueAsString(fallbackRecord)
                            + System.lineSeparator();

            try (FileChannel channel = FileChannel.open(
                    file,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE
            )) {
                channel.write(
                        ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8))
                );
                channel.force(true);
            }

            log.error(
                    "DLQ_TABLE_WRITE_FAILED, fell back to local file topic={} partition={} offset={} sensorId={} sqlState={} file={}",
                    position.topic(),
                    position.partition(),
                    position.offset(),
                    LogSanitizer.safe(letter.sensorId()),
                    deadLetterWriteFailure.getSQLState(),
                    file
            );

        } catch (IOException | RuntimeException fallbackFailure) {

            log.error(
                    "DLQ table write AND local file fallback both failed, record permanently lost topic={} partition={} offset={} sensorId={} dlqSqlState={} fallbackException={}",
                    position.topic(),
                    position.partition(),
                    position.offset(),
                    LogSanitizer.safe(letter.sensorId()),
                    deadLetterWriteFailure.getSQLState(),
                    fallbackFailure.getClass().getSimpleName()
            );

            throw new UncheckedIOException(
                    "Unable to persist failed message anywhere - DB and local fallback both failed",
                    fallbackFailure instanceof IOException ioFailure
                            ? ioFailure
                            : new IOException(fallbackFailure)
            );
        }
    }
}
