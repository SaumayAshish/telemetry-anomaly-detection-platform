package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;

import com.telemetry.platform.alerting.AlertWriter.DeadLetter;
import com.telemetry.platform.alerting.AlertWriter.Outcome;
import com.telemetry.platform.alerting.AlertWriter.PersistResult;
import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * The connection is validated only after it has sat idle, not before every record: validation is
 * a network round trip, and a write that fails on a dead connection already reconnects and retries.
 */
class AlertWriterIdleValidationTest {

    private static final RecordPosition POSITION =
            new RecordPosition("telemetry.sensor.anomalies.v1", 2, 17L);

    private static final AnomalyEvent EVENT = new AnomalyEvent(
            "sensor-1", Instant.parse("2026-10-13T10:15:30Z"), 9.5, 2.0, 1.5, 5.0, 3L, AnomalySeverity.HIGH);

    private static final long LONG_IDLE_NANOS = Duration.ofSeconds(31).toNanos();

    private final FakeDatabase db = new FakeDatabase();
    private final long[] now = {0L};

    private AlertWriter newWriter() throws SQLException, InterruptedException {
        return new AlertWriter(db.source(), (position, letter, failure) -> { }, millis -> { }, () -> now[0]);
    }

    @Test
    void doesNotValidateAConnectionThatWasJustUsed() throws Exception {
        AlertWriter writer = newWriter();

        writer.persist(POSITION, EVENT);
        writer.persist(POSITION, EVENT);

        assertThat(db.validityChecksMade()).isZero();
    }

    @Test
    void validatesAConnectionThatSatIdleBeforeWriting() throws Exception {
        AlertWriter writer = newWriter();
        writer.persist(POSITION, EVENT);

        now[0] += LONG_IDLE_NANOS;
        writer.persist(POSITION, EVENT);

        assertThat(db.validityChecksMade()).isEqualTo(1);
    }

    @Test
    void reconnectsAnIdleConnectionThatWentStale() throws Exception {
        AlertWriter writer = newWriter();
        writer.persist(POSITION, EVENT);
        now[0] += LONG_IDLE_NANOS;
        db.nextValidityCheckFails();

        PersistResult result = writer.persist(POSITION, EVENT);

        assertThat(result.outcome()).isEqualTo(Outcome.INSERTED);
        assertThat(db.connectCount()).isEqualTo(2);
    }

    @Test
    void aConnectionThatDiesWhileBusyIsRecoveredByTheRetryPathWithoutAValidityCheck() throws Exception {
        AlertWriter writer = newWriter();
        db.alertInsertThrows(new SQLException("connection lost", "08006"));

        PersistResult result = writer.persist(POSITION, EVENT);

        assertThat(result.outcome()).isEqualTo(Outcome.INSERTED);
        assertThat(db.connectCount()).isEqualTo(2);
        assertThat(db.validityChecksMade()).isZero();
    }

    private static final DeadLetter LETTER =
            new DeadLetter("sensor-1", "{\"bad\":true}", "VALIDATION", "value: must be finite", "VALIDATION");

    @Test
    void aConnectionThatKeepsWorkingIsNeverValidated() throws Exception {
        AlertWriter writer = newWriter();

        writer.persist(POSITION, EVENT);
        now[0] += Duration.ofSeconds(20).toNanos();
        writer.persist(POSITION, EVENT);
        now[0] += Duration.ofSeconds(20).toNanos();
        writer.persist(POSITION, EVENT);

        assertThat(db.validityChecksMade()).isZero();
    }

    @Test
    void aFreshlyOpenedConnectionIsTrustedWhateverTheClockReads() throws Exception {
        now[0] = Duration.ofHours(5).toNanos();
        AlertWriter writer = newWriter();

        writer.persist(POSITION, EVENT);

        assertThat(db.validityChecksMade()).isZero();
    }

    @Test
    void aDeadLetterWriteAlsoCountsAsUse() throws Exception {
        AlertWriter writer = newWriter();

        now[0] += Duration.ofSeconds(20).toNanos();
        writer.deadLetter(POSITION, LETTER);
        now[0] += Duration.ofSeconds(20).toNanos();
        writer.persist(POSITION, EVENT);

        assertThat(db.validityChecksMade()).isZero();
    }
}
