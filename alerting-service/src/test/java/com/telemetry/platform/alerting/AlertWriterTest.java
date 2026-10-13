package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.telemetry.platform.alerting.AlertWriter.DeadLetter;
import com.telemetry.platform.alerting.AlertWriter.Outcome;
import com.telemetry.platform.alerting.AlertWriter.PersistResult;
import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// The timeout guards against a broken retry loop spinning forever.
@Timeout(10)
class AlertWriterTest {

    private static final RecordPosition POSITION =
            new RecordPosition("telemetry.sensor.anomalies.v1", 2, 17L);

    private static final AnomalyEvent EVENT = new AnomalyEvent(
            "sensor-1", Instant.parse("2026-10-13T10:15:30Z"), 9.5, 2.0, 1.5, 5.0, 3L, AnomalySeverity.HIGH);

    private static final DeadLetter LETTER =
            new DeadLetter("sensor-1", "{\"bad\":true}", "VALIDATION", "value: must be finite", "VALIDATION");

    private record FallbackCall(RecordPosition position, DeadLetter letter, SQLException failure) {
    }

    private final FakeDatabase db = new FakeDatabase();
    private final List<FallbackCall> fallbackCalls = new ArrayList<>();

    @Test
    void insertsAnAlertOnAHealthyConnection() throws Exception {
        AlertWriter writer = newWriter();

        PersistResult result = writer.persist(POSITION, EVENT);

        assertThat(result.outcome()).isEqualTo(Outcome.INSERTED);
        assertThat(db.connectCount()).isEqualTo(1);
        assertThat(db.alertInsertAttempts()).isEqualTo(1);
    }

    @Test
    void reportsADuplicateWhenTheDatabaseIgnoredTheInsert() throws Exception {
        db.alertInsertReturns(0);
        AlertWriter writer = newWriter();

        PersistResult result = writer.persist(POSITION, EVENT);

        assertThat(result.outcome()).isEqualTo(Outcome.DUPLICATE);
        assertThat(db.alertInsertAttempts()).isEqualTo(1);
    }

    @Test
    void reconnectsBeforeWritingWhenTheConnectionWentStale() throws Exception {
        AlertWriter writer = newWriter();
        db.nextValidityCheckFails();

        PersistResult result = writer.persist(POSITION, EVENT);

        assertThat(result.outcome()).isEqualTo(Outcome.INSERTED);
        assertThat(db.connectCount()).isEqualTo(2);
        assertThat(db.closedConnections()).containsExactly(1);
        assertThat(db.closedStatements()).isEqualTo(2);
        assertThat(db.alertInsertAttempts()).isEqualTo(1);
    }

    @Test
    void reconnectsAndRetriesATransientInsertFailure() throws Exception {
        db.alertInsertThrows(new SQLException("connection lost", "08006"));
        AlertWriter writer = newWriter();

        PersistResult result = writer.persist(POSITION, EVENT);

        assertThat(result.outcome()).isEqualTo(Outcome.INSERTED);
        assertThat(db.connectCount()).isEqualTo(2);
        assertThat(db.closedConnections()).containsExactly(1);
        assertThat(db.alertInsertAttempts()).isEqualTo(2);
    }

    @Test
    void rejectsAPermanentInsertFailureWithoutReconnecting() throws Exception {
        SQLException violation = new SQLException("unknown sensor", "23503");
        db.alertInsertThrows(violation);
        AlertWriter writer = newWriter();

        PersistResult result = writer.persist(POSITION, EVENT);

        assertThat(result.outcome()).isEqualTo(Outcome.REJECTED);
        assertThat((Object) result.failure()).isSameAs(violation);
        assertThat(db.connectCount()).isEqualTo(1);
        assertThat(db.alertInsertAttempts()).isEqualTo(1);
    }

    @Test
    void writesADeadLetterThroughTheTable() throws Exception {
        AlertWriter writer = newWriter();

        writer.deadLetter(POSITION, LETTER);

        assertThat(db.deadLetterInsertAttempts()).isEqualTo(1);
        assertThat(fallbackCalls).isEmpty();
    }

    @Test
    void retriesADeadLetterWriteAfterATransientFailure() throws Exception {
        db.deadLetterInsertThrows(new SQLException("connection lost", "08006"));
        AlertWriter writer = newWriter();

        writer.deadLetter(POSITION, LETTER);

        assertThat(db.deadLetterInsertAttempts()).isEqualTo(2);
        assertThat(db.connectCount()).isEqualTo(2);
        assertThat(fallbackCalls).isEmpty();
    }

    @Test
    void fallsBackWhenTheDeadLetterTablePermanentlyRefusesTheWrite() throws Exception {
        SQLException tooLong = new SQLException("value too long", "22001");
        db.deadLetterInsertThrows(tooLong);
        AlertWriter writer = newWriter();

        writer.deadLetter(POSITION, LETTER);

        assertThat(fallbackCalls).hasSize(1);
        assertThat(fallbackCalls.get(0).position()).isEqualTo(POSITION);
        assertThat(fallbackCalls.get(0).letter()).isEqualTo(LETTER);
        assertThat((Object) fallbackCalls.get(0).failure()).isSameAs(tooLong);
        assertThat(db.deadLetterInsertAttempts()).isEqualTo(1);
        assertThat(db.connectCount()).isEqualTo(1);
    }

    @Test
    void closeReleasesTheStatementsAndTheConnection() throws Exception {
        AlertWriter writer = newWriter();

        writer.close();

        assertThat(db.closedConnections()).containsExactly(1);
        assertThat(db.closedStatements()).isEqualTo(2);
    }

    @Test
    void shutdownDuringAReconnectPropagatesTheInterruption() throws Exception {
        AtomicInteger connects = new AtomicInteger();
        AlertWriter writer = new AlertWriter(
                () -> {
                    if (connects.incrementAndGet() > 1) {
                        throw new InterruptedException("shutdown");
                    }
                    return db.source().connect();
                },
                this::recordFallback);
        db.nextValidityCheckFails();

        assertThatThrownBy(() -> writer.persist(POSITION, EVENT)).isInstanceOf(InterruptedException.class);
    }

    private AlertWriter newWriter() throws Exception {
        return new AlertWriter(db.source(), this::recordFallback);
    }

    private void recordFallback(RecordPosition position, DeadLetter letter, SQLException failure) {
        fallbackCalls.add(new FallbackCall(position, letter, failure));
    }
}