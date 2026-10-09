package com.telemetry.platform.api.alert;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class AlertService {

    private final AnomalyAlertRepository repository;
    private static final Logger log = LoggerFactory.getLogger(AlertService.class);

    public AlertService(AnomalyAlertRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public List<AlertResponse> findAlerts(AlertQuery query) {

        String severity = query.normalizedSeverity();
        List<AnomalyAlert> alerts;

        // AlertQuery validation guarantees a time range arrives alone and complete,
        // so no filter is silently ignored here.
        if (query.from() != null && query.to() != null) {
            alerts = repository.findByReadingTimestampBetween(query.from(), query.to());
        } else if (query.sensorId() != null && severity != null) {
            alerts = repository.findBySensorIdAndSeverity(query.sensorId(), severity);
        } else if (query.sensorId() != null) {
            alerts = repository.findBySensorId(query.sensorId());
        } else if (severity != null) {
            alerts = repository.findBySeverity(severity);
        } else {
            alerts = repository.findAllWithSensor();
        }

        List<AlertResponse> result = alerts.stream().map(AlertResponse::from).toList();
        log.info("Alert query returned {} rows", result.size());
        return result;
    }
}