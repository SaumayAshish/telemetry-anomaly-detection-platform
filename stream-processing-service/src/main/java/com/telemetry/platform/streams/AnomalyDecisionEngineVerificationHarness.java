package com.telemetry.platform.streams;

import java.time.Instant;
import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
/**
 * Standalone verification harness for AnomalyDecisionEngine.decide() - the pure
 * decision core extracted out of AnomalyProcessor.
 *
 * Deliberately does NOT touch Kafka, Kafka Streams, or the running topology, and
 * needs no KeyValueStore or FixedKeyProcessorContext - decide() has no dependency
 * on either. A passing run here is evidence about the exact same decision logic
 * AnomalyProcessor.process() delegates to in production, not a reimplementation.
 *
 * This is a plain main()-based harness rather than a JUnit test, same rationale
 * as RollingStatsVerificationHarness - Phase 17 (Testing) hasn't been covered yet.
 * It gets rewritten as real JUnit tests then.
 */
public class AnomalyDecisionEngineVerificationHarness {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {

        double threshold = AnomalyDetectionStreamApp.Z_SCORE_THRESHOLD;
        long minSamples = AnomalyDetectionStreamApp.MIN_SAMPLES_BEFORE_SCORING;
        long rebaselineAfter = AnomalyDetectionStreamApp.CONSECUTIVE_ANOMALIES_BEFORE_REBASELINE;

        double mean = 100.0;
        double stdDev = 10.0;
        double variance = stdDev * stdDev;

        String sensorId = "S-TEST";
        Instant timestamp = Instant.now();

        // ---- Normal reading: should update the baseline, emit no event ----

        RollingStats baseline = new RollingStats(minSamples, mean, variance * minSamples, 0);
        SensorReading normalReading = new SensorReading(sensorId, timestamp, "temperature", mean, 1);

        AnomalyDecision normalDecision = AnomalyDecisionEngine.decide(
                sensorId, normalReading, baseline, threshold, minSamples, rebaselineAfter);

        check("normal reading produces no event",
                normalDecision.event().isEmpty());

        check("normal reading is not marked as a rebaseline",
                !normalDecision.rebaselined());

        // RollingStats is a record, so .equals() compares every field (count, mean,
        // m2, consecutiveAnomalies) structurally - exactly what we want here.
        check("normal reading's newStats equals calling update() directly on the baseline",
                normalDecision.newStats().equals(baseline.update(mean)));

        // ---- Anomalous reading, streak still below the rebaseline threshold ----

        double outlierValue = mean + (threshold + 0.1) * stdDev;
        SensorReading outlierReading = new SensorReading(sensorId, timestamp, "temperature", outlierValue, 2);

        AnomalyDecision anomalyDecision = AnomalyDecisionEngine.decide(
                sensorId, outlierReading, baseline, threshold, minSamples, rebaselineAfter);

        check("anomalous reading produces an event",
                anomalyDecision.event().isPresent());

        check("anomalous reading below the rebaseline threshold is NOT marked as a rebaseline",
                !anomalyDecision.rebaselined());

        check("anomalous reading's newStats equals calling withAnomalyStreak() directly on the baseline",
                anomalyDecision.newStats().equals(baseline.withAnomalyStreak()));

        AnomalyEvent anomalyEvent = anomalyDecision.event().orElseThrow();

        check("event carries the correct sensorId",
                anomalyEvent.sensorId().equals(sensorId));

        check("event carries the reading's own value, not the baseline's",
                anomalyEvent.value() == outlierValue);

        check("event's baselineMean/baselineStdDev reflect the PRE-update baseline",
                anomalyEvent.baselineMean() == baseline.mean()
                        && anomalyEvent.baselineStdDev() == baseline.stdDev());

        check("event's zScore matches calling zScore() directly on the baseline",
                anomalyEvent.zScore() == baseline.zScore(outlierValue));

        check("event's consecutiveAnomalies is the baseline's streak plus one",
                anomalyEvent.consecutiveAnomalies() == baseline.consecutiveAnomalies() + 1);

        // ---- Anomalous reading that crosses the rebaseline threshold ----

        RollingStats almostRebaseline = new RollingStats(minSamples, mean, variance * minSamples, rebaselineAfter - 1);
        double extremeValue = mean + 10 * stdDev;
        SensorReading extremeReading = new SensorReading(sensorId, timestamp, "temperature", extremeValue, 3);

        AnomalyDecision rebaselineDecision = AnomalyDecisionEngine.decide(
                sensorId, extremeReading, almostRebaseline, threshold, minSamples, rebaselineAfter);

        check("crossing the rebaseline threshold still produces an event",
                rebaselineDecision.event().isPresent());

        check("crossing the rebaseline threshold IS marked as a rebaseline",
                rebaselineDecision.rebaselined());

        check("rebaseline's newStats is a full reset: count=1, mean=reading value, m2=0, streak=0",
                rebaselineDecision.newStats().equals(RollingStats.initial().update(extremeValue)));

        check("rebaseline's emitted event still reports the correct final streak count",
                rebaselineDecision.event().orElseThrow().consecutiveAnomalies() == rebaselineAfter);

        // ---- Anomalous reading one short of the rebaseline threshold (boundary check) ----

        RollingStats oneShortOfRebaseline = new RollingStats(minSamples, mean, variance * minSamples, rebaselineAfter - 2);

        AnomalyDecision boundaryDecision = AnomalyDecisionEngine.decide(
                sensorId, extremeReading, oneShortOfRebaseline, threshold, minSamples, rebaselineAfter);

        check("one short of the rebaseline threshold does NOT trigger a rebaseline",
                !boundaryDecision.rebaselined());

        check("one short of the rebaseline threshold still accumulates the streak normally",
                boundaryDecision.newStats().equals(oneShortOfRebaseline.withAnomalyStreak()));

        // ---- Warm-up gating: an extreme value below minSamples is never scored ----

        RollingStats belowWarmup = new RollingStats(minSamples - 1, mean, variance * (minSamples - 1), 0);

        AnomalyDecision warmupDecision = AnomalyDecisionEngine.decide(
                sensorId, extremeReading, belowWarmup, threshold, minSamples, rebaselineAfter);

        check("an extreme value below the warm-up count still produces no event",
                warmupDecision.event().isEmpty());

        check("below the warm-up count, decide() still folds the reading into the baseline via update()",
                warmupDecision.newStats().equals(belowWarmup.update(extremeValue)));

        // ---- Zero-variance baseline never flags, regardless of value ----

        RollingStats zeroVariance = new RollingStats(minSamples + 20, mean, 0.0, 0);

        AnomalyDecision zeroVarianceDecision = AnomalyDecisionEngine.decide(
                sensorId, extremeReading, zeroVariance, threshold, minSamples, rebaselineAfter);

        check("a zero-variance baseline never flags an event, even for a wildly different value",
                zeroVarianceDecision.event().isEmpty());

        // ---- Severity classification: AnomalySeverity.fromZScore() tier boundaries ----
        // Reusing `baseline` from the normal-reading section above (mean=100.0,
        // stdDev=10.0), so a reading of `mean + k * stdDev` produces a z-score of
        // essentially exactly k against it - letting us land ON each tier boundary
        // by construction, the same boundary-testing discipline used for the
        // rebaseline threshold above (exact boundary + one-short-of-boundary pairs).

        double lowValue = mean + 4.9 * stdDev; // z ~ 4.9 -> LOW
        SensorReading lowReading = new SensorReading(sensorId, timestamp, "temperature", lowValue, 4);
        AnomalyEvent lowEvent = AnomalyDecisionEngine.decide(
                sensorId, lowReading, baseline, threshold, minSamples, rebaselineAfter
        ).event().orElseThrow();

        check("z-score just below 5.0 is classified LOW",
                lowEvent.severity() == AnomalySeverity.LOW);

        double mediumBoundaryValue = mean + 5.0 * stdDev; // z == 5.0 exactly -> MEDIUM
        SensorReading mediumBoundaryReading = new SensorReading(sensorId, timestamp, "temperature", mediumBoundaryValue, 5);
        AnomalyEvent mediumBoundaryEvent = AnomalyDecisionEngine.decide(
                sensorId, mediumBoundaryReading, baseline, threshold, minSamples, rebaselineAfter
        ).event().orElseThrow();

        check("z-score of exactly 5.0 is classified MEDIUM (inclusive lower boundary)",
                mediumBoundaryEvent.severity() == AnomalySeverity.MEDIUM);

        double justBelowHighValue = mean + 9.9 * stdDev; // z ~ 9.9 -> still MEDIUM
        SensorReading justBelowHighReading = new SensorReading(sensorId, timestamp, "temperature", justBelowHighValue, 6);
        AnomalyEvent justBelowHighEvent = AnomalyDecisionEngine.decide(
                sensorId, justBelowHighReading, baseline, threshold, minSamples, rebaselineAfter
        ).event().orElseThrow();

        check("z-score just below 10.0 is still classified MEDIUM",
                justBelowHighEvent.severity() == AnomalySeverity.MEDIUM);

        double highBoundaryValue = mean + 10.0 * stdDev; // z == 10.0 exactly -> HIGH
        SensorReading highBoundaryReading = new SensorReading(sensorId, timestamp, "temperature", highBoundaryValue, 7);
        AnomalyEvent highBoundaryEvent = AnomalyDecisionEngine.decide(
                sensorId, highBoundaryReading, baseline, threshold, minSamples, rebaselineAfter
        ).event().orElseThrow();

        check("z-score of exactly 10.0 is classified HIGH (inclusive lower boundary)",
                highBoundaryEvent.severity() == AnomalySeverity.HIGH);

        double justBelowCriticalValue = mean + 14.9 * stdDev; // z ~ 14.9 -> still HIGH
        SensorReading justBelowCriticalReading = new SensorReading(sensorId, timestamp, "temperature", justBelowCriticalValue, 8);
        AnomalyEvent justBelowCriticalEvent = AnomalyDecisionEngine.decide(
                sensorId, justBelowCriticalReading, baseline, threshold, minSamples, rebaselineAfter
        ).event().orElseThrow();

        check("z-score just below 15.0 is still classified HIGH",
                justBelowCriticalEvent.severity() == AnomalySeverity.HIGH);

        double criticalBoundaryValue = mean + 15.0 * stdDev; // z == 15.0 exactly -> CRITICAL
        SensorReading criticalBoundaryReading = new SensorReading(sensorId, timestamp, "temperature", criticalBoundaryValue, 9);
        AnomalyEvent criticalBoundaryEvent = AnomalyDecisionEngine.decide(
                sensorId, criticalBoundaryReading, baseline, threshold, minSamples, rebaselineAfter
        ).event().orElseThrow();

        check("z-score of exactly 15.0 is classified CRITICAL (inclusive lower boundary)",
                criticalBoundaryEvent.severity() == AnomalySeverity.CRITICAL);

        double negativeExtremeValue = mean - 20.0 * stdDev; // z ~ -20.0 -> CRITICAL by magnitude, not sign
        SensorReading negativeExtremeReading = new SensorReading(sensorId, timestamp, "temperature", negativeExtremeValue, 10);
        AnomalyEvent negativeExtremeEvent = AnomalyDecisionEngine.decide(
                sensorId, negativeExtremeReading, baseline, threshold, minSamples, rebaselineAfter
        ).event().orElseThrow();

        check("a large NEGATIVE z-score is classified by magnitude, not sign (CRITICAL)",
                negativeExtremeEvent.severity() == AnomalySeverity.CRITICAL);

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