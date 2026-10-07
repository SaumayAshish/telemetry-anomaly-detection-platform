package com.telemetry.platform.api.alert;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
public class AlertService {

    private final AnomalyAlertRepository repository;

    public AlertService(AnomalyAlertRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public List<AlertResponse> findAlerts(String sensorId, String severity, Instant from, Instant to) {
        List<AnomalyAlert> alerts;
        if (from != null && to != null) {
            alerts = repository.findByReadingTimestampBetween(from, to);
        } else if (sensorId != null && severity != null) {
            alerts = repository.findBySensorIdAndSeverity(sensorId, severity);
        } else if (sensorId != null) {
            alerts = repository.findBySensorId(sensorId);
        } else if (severity != null) {
            alerts = repository.findBySeverity(severity);
        } else {
            alerts = repository.findAllWithSensor();
        }
        return alerts.stream().map(AlertResponse::from).toList();
    }
}