package com.telemetry.platform.streams;

public record RollingStats(long count, double sum){
    public double average() {
        return count == 0 ? 0.0 : sum / count;
    }
}
