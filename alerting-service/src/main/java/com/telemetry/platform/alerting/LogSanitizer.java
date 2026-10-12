package com.telemetry.platform.alerting;

/** Makes untrusted values (Kafka payload fields, headers) safe to put in one log line. */
final class LogSanitizer {

    private LogSanitizer() {}

    static String safe(String value) {
        if (value == null) {
            return "null";
        }
        String cleaned = value.replaceAll("[\\p{Cntrl}\\p{Cc}\\p{Zl}\\p{Zp}]", "_");
        return cleaned.length() > 64 ? cleaned.substring(0, 64) + "..." : cleaned;
    }
}