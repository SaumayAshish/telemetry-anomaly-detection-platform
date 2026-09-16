package com.telemetry.platform.streams;

import java.util.Optional;
import com.telemetry.platform.events.AnomalyEvent;
import com.telemetry.platform.events.AnomalySeverity;
public final class AnomalyDecisionEngine {
    private AnomalyDecisionEngine() {}
    public static AnomalyDecision decide(
            String sensorId,
            SensorReading reading,
            RollingStats currentStats,
            double zScoreThreshold,
            long minSamplesBeforeScoring,
            long consecutiveAnomaliesBeforeRebaseline

    ) {
        boolean anomaly = currentStats.isAnomaly(reading.value(), zScoreThreshold, minSamplesBeforeScoring);
        if (!anomaly) {
            RollingStats newStats = currentStats.update(reading.value());
            return new AnomalyDecision(newStats, Optional.empty(), false);
        }
        long newStreak = currentStats.consecutiveAnomalies() + 1;
        double zScore = currentStats.zScore(reading.value());
        double baselineMean = currentStats.mean();
        double baselineStdDev = currentStats.stdDev();

        boolean rebaseline =  newStreak >= consecutiveAnomaliesBeforeRebaseline;
        RollingStats newStats = rebaseline
                ? RollingStats.initial().update(reading.value())
                : currentStats.withAnomalyStreak();

        AnomalyEvent event = new AnomalyEvent(
                sensorId,
                reading.timestamp(),
                reading.value(),
                baselineMean,
                baselineStdDev,
                zScore,
                newStreak,
                AnomalySeverity.fromZScore(zScore)
        );
        return new AnomalyDecision(newStats, Optional.of(event), rebaseline);
    }


}
