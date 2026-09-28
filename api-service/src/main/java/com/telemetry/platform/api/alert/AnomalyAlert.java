package com.telemetry.platform.api.alert;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "anomaly_alerts")
public class AnomalyAlert {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "sensor_id", nullable = false)
    private String sensorId;

    @Column(name = "value", nullable = false)
    private Double value;

    @Column(name = "baseline_mean", nullable = false)
    private Double baselineMean;

    @Column(name = "baseline_std_dev", nullable = false)
    private Double baselineStdDev;

    @Column(name = "z_score", nullable = false)
    private Double zScore;

    @Column(name = "consecutive_anomalies", nullable = false)
    private Integer consecutiveAnomalies;

    @Column(name = "severity", nullable = false)
    private String severity;

    @Column(name = "reading_timestamp", nullable = false)
    private Instant readingTimestamp;

    @Column(name = "ingested_at", nullable = false)
    private Instant ingestedAt;

    protected AnomalyAlert() {
        // Required by Hibernate. Never called directly by our code -
        // Hibernate instantiates entities via reflection.
    }

    public Long getId() { return id; }
    public String getSensorId() { return sensorId; }
    public Double getValue() { return value; }
    public Double getBaselineMean() { return baselineMean; }
    public Double getBaselineStdDev() { return baselineStdDev; }
    public Double getZScore() { return zScore; }
    public Integer getConsecutiveAnomalies() { return consecutiveAnomalies; }
    public String getSeverity() { return severity; }
    public Instant getReadingTimestamp() { return readingTimestamp; }
    public Instant getIngestedAt() { return ingestedAt; }
}