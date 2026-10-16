package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import com.telemetry.platform.alerting.BatchLoop.PolledRecord;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.junit.jupiter.api.Test;

/** Only a commit the group rejected counts as a lost commit. */
class BatchLoopMetricsTest {

    private final AlertMetrics metrics = AlertMetrics.create();
    private RuntimeException commitFailure;

    private final BatchLoop.BatchSource source = new BatchLoop.BatchSource() {
        @Override
        public List<PolledRecord> poll() {
            return List.of(new PolledRecord(
                    new RecordPosition("telemetry.sensor.anomalies.v1", 0, 0L), "{}"));
        }

        @Override
        public void commit() {
            if (commitFailure != null) {
                throw commitFailure;
            }
        }
    };

    private final BatchLoop loop = new BatchLoop(source, (position, value) -> { }, metrics);

    @Test
    void aCommitRejectedByTheGroupIsCounted() throws Exception {
        commitFailure = new CommitFailedException();

        loop.runOnce();

        assertThat(metrics.scrape()).contains("alerting_commits_lost_total 1.0");
    }

    @Test
    void aSuccessfulCommitIsNotCounted() throws Exception {
        loop.runOnce();

        assertThat(metrics.scrape()).contains("alerting_commits_lost_total 0.0");
    }

    @Test
    void anyOtherCommitFailureStillPropagatesAndIsNotCounted() {
        commitFailure = new IllegalStateException("broker gone");

        assertThatThrownBy(loop::runOnce).isInstanceOf(IllegalStateException.class);

        assertThat(metrics.scrape()).contains("alerting_commits_lost_total 0.0");
    }
}
