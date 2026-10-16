package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;

import com.telemetry.platform.events.AnomalySeverity;
import org.junit.jupiter.api.Test;

class AlertMetricsTest {

    private final AlertMetrics metrics = AlertMetrics.create();

    @Test
    void persistedAlertsAreCountedPerSeverity() {
        metrics.alertPersisted(AnomalySeverity.HIGH);
        metrics.alertPersisted(AnomalySeverity.HIGH);
        metrics.alertPersisted(AnomalySeverity.CRITICAL);

        assertThat(metrics.scrape())
                .contains("alerting_alerts_persisted_total{severity=\"HIGH\"} 2.0")
                .contains("alerting_alerts_persisted_total{severity=\"CRITICAL\"} 1.0")
                .doesNotContain("severity=\"LOW\"");
    }

    @Test
    void duplicatesAreCounted() {
        metrics.alertDuplicate();
        metrics.alertDuplicate();
        metrics.alertDuplicate();

        assertThat(metrics.scrape()).contains("alerting_alerts_duplicate_total 3.0");
    }

    @Test
    void deadLettersAreCountedPerReason() {
        metrics.deadLettered("VALIDATION");
        metrics.deadLettered("VALIDATION");
        metrics.deadLettered("DESERIALIZATION");

        assertThat(metrics.scrape())
                .contains("alerting_dead_letters_total{reason=\"VALIDATION\"} 2.0")
                .contains("alerting_dead_letters_total{reason=\"DESERIALIZATION\"} 1.0");
    }

    @Test
    void lostCommitsAreCounted() {
        metrics.commitLost();

        assertThat(metrics.scrape()).contains("alerting_commits_lost_total 1.0");
    }
}
