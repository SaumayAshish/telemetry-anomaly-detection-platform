package com.telemetry.platform.streams;

/**
 * Standalone verification harness for RollingStats' anomaly-detection decision logic.
 *
 * Deliberately does NOT touch Kafka, Kafka Streams, or the running topology — it
 * calls RollingStats.isAnomaly()/zScore() directly, the exact same methods the
 * AnomalyDetectionStreamApp aggregator calls in production. A passing run here is
 * evidence about the real decision logic, not a reimplementation of it.
 *
 * This is a plain main()-based harness rather than a JUnit test because Phase 17
 * (Testing) hasn't been covered yet. It gets rewritten as real JUnit tests then.
 */
public class RollingStatsVerificationHarness {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {

        double threshold = AnomalyDetectionStreamApp.Z_SCORE_THRESHOLD;
        long minSamples = AnomalyDetectionStreamApp.MIN_SAMPLES_BEFORE_SCORING;

        double mean = 100.0;
        double stdDev = 10.0;
        double variance = stdDev * stdDev;

        // Hand-constructing RollingStats via its canonical constructor, instead of
        // simulating .update() calls, gives us exact, predictable numbers to assert on.
        RollingStats baseline = new RollingStats(minSamples, mean, variance * minSamples);

        check("value exactly at mean is not an anomaly",
                !baseline.isAnomaly(mean, threshold, minSamples));

        check("value just inside the threshold is not an anomaly",
                !baseline.isAnomaly(mean + (threshold - 0.1) * stdDev, threshold, minSamples));

        check("value just outside the threshold IS an anomaly",
                baseline.isAnomaly(mean + (threshold + 0.1) * stdDev, threshold, minSamples));

        check("value exactly at the threshold boundary is not an anomaly (strict >)",
                !baseline.isAnomaly(mean + threshold * stdDev, threshold, minSamples));

        check("symmetric negative-side outlier IS an anomaly",
                baseline.isAnomaly(mean - (threshold + 0.1) * stdDev, threshold, minSamples));

        check("extreme outlier (10 sigma away) IS an anomaly",
                baseline.isAnomaly(mean + 10 * stdDev, threshold, minSamples));

        RollingStats belowWarmup = new RollingStats(minSamples - 1, mean, variance * (minSamples - 1));
        check("extreme value BELOW the warm-up count is not scored at all",
                !belowWarmup.isAnomaly(mean + 10 * stdDev, threshold, minSamples));

        RollingStats exactlyAtWarmup = new RollingStats(minSamples, mean, variance * minSamples);
        check("extreme value exactly AT the warm-up count boundary IS scored",
                exactlyAtWarmup.isAnomaly(mean + 10 * stdDev, threshold, minSamples));

        RollingStats zeroVariance = new RollingStats(minSamples + 20, mean, 0.0);
        check("zero-variance baseline never flags, even for a wildly different value",
                !zeroVariance.isAnomaly(mean * 5, threshold, minSamples));

        check("zero-variance baseline's zScore is Infinity/NaN, not an exception",
                Double.isInfinite(zeroVariance.zScore(mean * 5)) || Double.isNaN(zeroVariance.zScore(mean * 5)));

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed, out of " + (passed + failed) + " checks.");
        if (failed > 0) {
            throw new AssertionError(failed + " verification check(s) failed - see FAIL lines above.");
        }
    }

    private static void check(String description, boolean condition) {
        if (condition) {
            passed++;
            System.out.println("PASS - " + description);
        } else {
            failed++;
            System.out.println("FAIL - " + description);
        }
    }
}