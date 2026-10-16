package com.telemetry.platform.alerting;

import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import java.sql.SQLException;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One poll-process-commit cycle of the consumer. Offsets are committed only after every
 * record of the polled batch has been handled, so a crash redelivers the batch and the
 * idempotent insert absorbs the duplicates.
 */
final class BatchLoop {

    private static final Logger log = LoggerFactory.getLogger(BatchLoop.class);

    record PolledRecord(RecordPosition position, String value) {
    }

    /** The slice of the Kafka consumer this loop needs. */
    interface BatchSource {
        List<PolledRecord> poll();

        void commit();
    }

    interface RecordHandler {
        void process(RecordPosition position, String rawPayload)
                throws SQLException, InterruptedException;
    }

    private final BatchSource source;
    private final RecordHandler handler;
    private final AlertMetrics metrics;

    BatchLoop(BatchSource source, RecordHandler handler) {
        this(source, handler, AlertMetrics.create());
    }

    BatchLoop(BatchSource source, RecordHandler handler, AlertMetrics metrics) {
        this.source = source;
        this.handler = handler;
        this.metrics = metrics;
    }

    /** Runs one cycle and returns how many records the poll delivered. */
    int runOnce() throws SQLException, InterruptedException {
        List<PolledRecord> batch = source.poll();

        for (PolledRecord record : batch) {
            handler.process(record.position(), record.value());
        }

        if (!batch.isEmpty()) {
            try {
                source.commit();
            } catch (CommitFailedException e) {
                // The group moved our partitions while the batch was processed. Every record
                // was already persisted or dead-lettered, so the next poll redelivers them
                // and the idempotent insert turns them into duplicates.
                log.warn(
                        "COMMIT_LOST batchSize={} exception={}",
                        batch.size(),
                        e.getClass().getSimpleName()
                );
                metrics.commitLost();
            }
        }

        return batch.size();
    }
}
