package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import com.telemetry.platform.alerting.BatchLoop.PolledRecord;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.junit.jupiter.api.Test;

class BatchLoopTest {

    private final List<String> events = new ArrayList<>();
    private List<PolledRecord> nextBatch = List.of();
    private RuntimeException commitFailure;
    private SQLException processFailure;

    private final BatchLoop.BatchSource source = new BatchLoop.BatchSource() {
        @Override
        public List<PolledRecord> poll() {
            return nextBatch;
        }

        @Override
        public void commit() {
            events.add("commit");
            if (commitFailure != null) {
                throw commitFailure;
            }
        }
    };

    private final BatchLoop loop = new BatchLoop(source, (position, value) -> {
        events.add("process:" + position.offset());
        if (processFailure != null) {
            throw processFailure;
        }
    }, AlertMetrics.create());

    private static PolledRecord record(long offset) {
        return new PolledRecord(new RecordPosition("telemetry.sensor.anomalies.v1", 0, offset), "{}");
    }

    @Test
    void processesEveryRecordInOrderThenCommitsOnce() throws Exception {
        nextBatch = List.of(record(0), record(1), record(2));

        int delivered = loop.runOnce();

        assertThat(delivered).isEqualTo(3);
        assertThat(events).containsExactly("process:0", "process:1", "process:2", "commit");
    }

    @Test
    void doesNotCommitAnEmptyBatch() throws Exception {
        int delivered = loop.runOnce();

        assertThat(delivered).isZero();
        assertThat(events).isEmpty();
    }

    @Test
    void doesNotCommitWhenProcessingFails() {
        nextBatch = List.of(record(0), record(1));
        processFailure = new SQLException("cannot reconnect", "08001");

        assertThatThrownBy(loop::runOnce).isInstanceOf(SQLException.class);

        assertThat(events).containsExactly("process:0");
    }

    // Open decision 2: a commit rejected because the group moved this consumer's partitions
    // happens after every record was handled, and the idempotent insert absorbs the
    // redelivery, so the loop carries on instead of crashing main.
    @Test
    void survivesACommitRejectedByTheGroup() throws Exception {
        nextBatch = List.of(record(0), record(1));
        commitFailure = new CommitFailedException();

        int delivered = loop.runOnce();

        assertThat(delivered).isEqualTo(2);
        assertThat(events).containsExactly("process:0", "process:1", "commit");
    }

    @Test
    void keepsWorkingOnTheNextCycleAfterALostCommit() throws Exception {
        nextBatch = List.of(record(0));
        commitFailure = new CommitFailedException();
        loop.runOnce();

        commitFailure = null;
        nextBatch = List.of(record(0), record(1));
        int delivered = loop.runOnce();

        assertThat(delivered).isEqualTo(2);
        assertThat(events)
                .containsExactly("process:0", "commit", "process:0", "process:1", "commit");
    }

    @Test
    void doesNotSwallowOtherCommitFailures() {
        nextBatch = List.of(record(0));
        commitFailure = new IllegalStateException("consumer closed");

        assertThatThrownBy(loop::runOnce).isInstanceOf(IllegalStateException.class);
    }
}