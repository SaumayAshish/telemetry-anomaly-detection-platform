package com.telemetry.platform.streams;

import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class AnomalyDecisionEngineTest {

    private static final double THRESHOLD = AnomalyDetectionStreamApp.Z_SCORE_THRESHOLD;
    private static final long MIN_SAMPLES = AnomalyDetectionStreamApp.MIN_SAMPLES_BEFORE_SCORING;
    private static final long REBASELINE_AFTER = AnomalyDetectionStreamApp.CONSECUTIVE_ANOMALIES_BEFORE_REBASELINE;
    private static final Instant WHEN = Instant.parse("2026-10-12T10:00:00Z");

    private static RollingStats baseline(int n) {
        RollingStats stats = RollingStats.initial();
        for (int i = 0; i < n; i++) {
            stats = stats.update(i % 2 == 0 ? 10.0 : 20.0);   // mean 15, std dev 5
        }
        return stats;
    }

    private static SensorReading reading(double value) {
        return new SensorReading("S-1001", WHEN, "temperature", value, 1);
    }

    private static AnomalyDecision decide(RollingStats stats, double value) {
        return AnomalyDecisionEngine.decide("S-1001", reading(value), stats,
                THRESHOLD, MIN_SAMPLES, REBASELINE_AFTER);
    }

    @Test
    void normalReadingUpdatesBaselineAndEmitsNothing() {
        RollingStats stats = baseline(30);

        AnomalyDecision decision = decide(stats, 16.0);

        assertThat(decision.event()).isEmpty();
        assertThat(decision.rebaselined()).isFalse();
        assertThat(decision.newStats().count()).isEqualTo(31);
    }

    @Test
    void readingDuringWarmUpIsLearnedNotScored() {
        RollingStats stats = baseline(5);

        AnomalyDecision decision = decide(stats, 1000.0);

        assertThat(decision.event()).isEmpty();
        assertThat(decision.newStats().count()).isEqualTo(6);
    }

    @Test
    void anomalyEmitsEventWithTheBaselineItWasJudgedAgainst_andDoesNotContaminateIt() {
        RollingStats stats = baseline(30);

        AnomalyDecision decision = decide(stats, 200.0);

        AnomalyEvent event = decision.event().orElseThrow();
        assertThat(event.sensorId()).isEqualTo("S-1001");
        assertThat(event.readingTimestamp()).isEqualTo(WHEN);
        assertThat(event.value()).isEqualTo(200.0);
        assertThat(event.baselineMean()).isEqualTo(stats.mean());
        assertThat(event.baselineStdDev()).isEqualTo(stats.stdDev());
        assertThat(event.consecutiveAnomalies()).isEqualTo(1);
        assertThat(event.severity()).isEqualTo(AnomalySeverity.CRITICAL);   // z = 37

        // hysteresis: the outlier must NOT be folded into the baseline
        assertThat(decision.rebaselined()).isFalse();
        assertThat(decision.newStats().count()).isEqualTo(stats.count());
        assertThat(decision.newStats().mean()).isEqualTo(stats.mean());
        assertThat(decision.newStats().consecutiveAnomalies()).isEqualTo(1);
    }

    @Test
    void streakBelowTheLimitDoesNotRebaseline() {
        RollingStats stats = baseline(30);
        for (long i = 0; i < REBASELINE_AFTER - 2; i++) {
            stats = stats.withAnomalyStreak();                     // streak = limit - 2
        }

        AnomalyDecision decision = decide(stats, 200.0);           // streak becomes limit - 1

        assertThat(decision.rebaselined()).isFalse();
        assertThat(decision.event().orElseThrow().consecutiveAnomalies()).isEqualTo(REBASELINE_AFTER - 1);
    }

    @Test
    void reachingTheStreakLimitRebaselinesFromScratchOnTheNewValue() {
        RollingStats stats = baseline(30);
        for (long i = 0; i < REBASELINE_AFTER - 1; i++) {
            stats = stats.withAnomalyStreak();                     // streak = limit - 1
        }

        AnomalyDecision decision = decide(stats, 200.0);           // streak becomes the limit

        assertThat(decision.rebaselined()).isTrue();
        assertThat(decision.event().orElseThrow().consecutiveAnomalies()).isEqualTo(REBASELINE_AFTER);
        assertThat(decision.newStats().count()).isEqualTo(1);       // fresh accumulator, not 31
        assertThat(decision.newStats().mean()).isEqualTo(200.0);
        assertThat(decision.newStats().consecutiveAnomalies()).isZero();
    }
}