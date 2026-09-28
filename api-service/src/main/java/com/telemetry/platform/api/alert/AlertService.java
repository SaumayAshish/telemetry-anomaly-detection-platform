package com.telemetry.platform.api.alert;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class AlertService {

    private final AnomalyAlertRepository repository;

    public AlertService(AnomalyAlertRepository repository) {
        this.repository = repository;
    }

    public List<AnomalyAlert> findAlerts(String sensorId, String severity, Instant from, Instant to) {
        // Time range takes precedence and is handled independently. Combining it
        // with sensorId/severity in this derived-method style would mean writing
        // a findBySensorIdAndSeverityAndReadingTimestampBetween-shaped method for
        // every combination - that combinatorial blow-up is exactly what pushes
        // real production code toward JPA Specifications instead. Out of scope here.
        if (from != null && to != null) {
            return repository.findByReadingTimestampBetween(from, to);
        }
        if (sensorId != null && severity != null) {
            return repository.findBySensorIdAndSeverity(sensorId, severity);
        }
        if (sensorId != null) {
            return repository.findBySensorId(sensorId);
        }
        if (severity != null) {
            return repository.findBySeverity(severity);
        }
        return repository.findAll();
    }
}
