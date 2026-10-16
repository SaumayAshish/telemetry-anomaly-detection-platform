package com.telemetry.platform.alerting;

import com.telemetry.platform.events.AnomalySeverity;
import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * The consumer's operational counters, readable as Prometheus text. Labels are bounded: severity
 * and dead-letter reason come from small fixed sets, never from payload content.
 */
final class AlertMetrics {

    private final PrometheusMeterRegistry registry =
            new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final Counter duplicates;
    private final Counter commitsLost;

    private AlertMetrics() {
        // Registered up front so a scrape shows 0 before the first event.
        duplicates = registry.counter("alerting.alerts.duplicate");
        commitsLost = registry.counter("alerting.commits.lost");
    }

    static AlertMetrics create() {
        return new AlertMetrics();
    }

    void alertPersisted(AnomalySeverity severity) {
        registry.counter("alerting.alerts.persisted", "severity", severity.name()).increment();
    }

    void alertDuplicate() {
        duplicates.increment();
    }

    void deadLettered(String reason) {
        registry.counter("alerting.dead.letters", "reason", reason).increment();
    }

    void commitLost() {
        commitsLost.increment();
    }

    String scrape() {
        return registry.scrape();
    }
}
