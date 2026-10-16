package com.telemetry.platform.api.alert;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
    public Page<AlertResponse> findAlerts(AlertQuery query, Pageable pageable) {

        String severity = query.normalizedSeverity();
        Page<AnomalyAlert> alerts;

        // AlertQuery validation guarantees a time range arrives alone and complete,
        // so no filter is silently ignored here.
        if (query.from() != null && query.to() != null) {
            alerts = repository.findByReadingTimestampBetween(query.from(), query.to(), pageable);
        } else if (query.sensorId() != null && severity != null) {
            alerts = repository.findBySensorIdAndSeverity(query.sensorId(), severity, pageable);
        } else if (query.sensorId() != null) {
            alerts = repository.findBySensorId(query.sensorId(), pageable);
        } else if (severity != null) {
            alerts = repository.findBySeverity(severity, pageable);
        } else {
            alerts = repository.findAllWithSensor(pageable);
        }

        Page<AlertResponse> result = alerts.map(AlertResponse::from);
        log.info("Alert query returned {} rows", result.getNumberOfElements());
        return result;
    }
}