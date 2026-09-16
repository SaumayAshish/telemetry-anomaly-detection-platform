package com.telemetry.platform.events;

public enum AnomalySeverity {
    LOW,
    MEDIUM,
    HIGH,
    CRITICAL;

    public static AnomalySeverity fromZScore(double zScore) {
        double magnitude = Math.abs(zScore);
        if (magnitude >= 15.0){
            return CRITICAL;
        } else if (magnitude >= 10.0) {
            return HIGH;
        } else if (magnitude >= 5.0) {
            return MEDIUM;
        } else  {
            return LOW;
        }
    }

}
