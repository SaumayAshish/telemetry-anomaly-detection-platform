package com.telemetry.platform.streams;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class RollingStatsTest {

    private static final double THRESHOLD = AnomalyDetectionStreamApp.Z_SCORE_THRESHOLD;
    private static final long MIN_SAMPLES = AnomalyDetectionStreamApp.MIN_SAMPLES_BEFORE_SCORING;

    /** Folds the values through update(), the way the topology does. */
    private static RollingStats statsOf(double... values) {
        RollingStats stats = RollingStats.initial();
        for (double value : values) {
            stats = stats.update(value);
        }
        return stats;
    }

    /** n alternating 10/20 readings: mean 15, population std dev 5 (when n is even). */
    private static RollingStats baseline(int n) {
        RollingStats stats = RollingStats.initial();
        for (int i = 0; i < n; i++) {
            stats = stats.update(i % 2 == 0 ? 10.0 : 20.0);
        }
        return stats;
    }

    @Test
    void welfordMatchesKnownTextbookDataset() {
        // 2,4,4,4,5,5,7,9: mean 5, population variance 4, std dev 2
        RollingStats stats = statsOf(2, 4, 4, 4, 5, 5, 7, 9);

        assertThat(stats.count()).isEqualTo(8);
        assertThat(stats.mean()).isCloseTo(5.0, within(1e-9));
        assertThat(stats.variance()).isCloseTo(4.0, within(1e-9));
        assertThat(stats.stdDev()).isCloseTo(2.0, within(1e-9));
    }

    @Test
    void varianceIsZeroBelowTwoSamples() {
        assertThat(RollingStats.initial().variance()).isZero();
        assertThat(statsOf(42.0).variance()).isZero();
    }

    @Test
    void valueAtTheMeanIsNotAnAnomaly() {
        RollingStats stats = baseline(30);
        assertThat(stats.isAnomaly(stats.mean(), THRESHOLD, MIN_SAMPLES)).isFalse();
    }

    @Test
    void thresholdIsStrict_equalZScoreIsNotAnAnomaly() {
        RollingStats stats = baseline(30);
        double value = 30.0;
        double exactZ = stats.zScore(value);

        // Using the computed z itself as the threshold avoids floating-point guessing.
        assertThat(stats.isAnomaly(value, exactZ, MIN_SAMPLES)).isFalse();
        assertThat(stats.isAnomaly(value, exactZ - 1e-6, MIN_SAMPLES)).isTrue();
    }

    @Test
    void outliersOnBothSidesAreFlagged() {
        RollingStats stats = baseline(30);
        assertThat(stats.isAnomaly(200.0, THRESHOLD, MIN_SAMPLES)).isTrue();
        assertThat(stats.isAnomaly(-170.0, THRESHOLD, MIN_SAMPLES)).isTrue();
    }

    @Test
    void warmUpGate_belowMinimumSamplesNeverScores_exactlyAtMinimumScores() {
        assertThat(baseline((int) MIN_SAMPLES - 1).isAnomaly(1000.0, THRESHOLD, MIN_SAMPLES)).isFalse();
        assertThat(baseline((int) MIN_SAMPLES).isAnomaly(1000.0, THRESHOLD, MIN_SAMPLES)).isTrue();
    }

    @Test
    void zeroVarianceBaselineNeverFlags_andZScoreIsInfiniteNotAnException() {
        double[] flat = new double[30];
        java.util.Arrays.fill(flat, 50.0);
        RollingStats stats = statsOf(flat);

        assertThat(stats.stdDev()).isZero();
        assertThat(stats.isAnomaly(1000.0, THRESHOLD, MIN_SAMPLES)).isFalse();
        assertThat(stats.zScore(1000.0)).isInfinite();
    }

    @Test
    void anomalyStreakIsCountedWithoutTouchingTheBaseline_andResetByUpdate() {
        RollingStats stats = baseline(30);
        RollingStats flagged = stats.withAnomalyStreak().withAnomalyStreak();

        assertThat(flagged.consecutiveAnomalies()).isEqualTo(2);
        assertThat(flagged.count()).isEqualTo(stats.count());
        assertThat(flagged.mean()).isEqualTo(stats.mean());
        assertThat(flagged.m2()).isEqualTo(stats.m2());

        assertThat(flagged.update(15.0).consecutiveAnomalies()).isZero();
    }
}