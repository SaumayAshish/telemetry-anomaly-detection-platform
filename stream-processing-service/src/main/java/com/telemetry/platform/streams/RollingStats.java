package com.telemetry.platform.streams;

public record RollingStats(long count, double mean, double m2){

    public static RollingStats initial() {
        return new RollingStats(0, 0.0, 0.0);
    }
    public RollingStats update(double newValue) {
        long newCount = count + 1;
        double delta = newValue - mean;
        double newMean = mean + delta /  newCount;
        double delta2 = newValue - newMean;
        double newM2 = m2 + delta * delta2;
        return new RollingStats(newCount, newMean, newM2);
    }

    public double variance() {
        return count < 2 ? 0.0 : m2 / count;
    }

    public double stdDev() {
        return Math.sqrt(variance());
    }
}
