package com.telemetry.platform.api.alert;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface AnomalyAlertRepository extends JpaRepository<AnomalyAlert, Long> {

    List<AnomalyAlert> findBySensorId(String sensorId);

    List<AnomalyAlert> findBySeverity(String severity);

    List<AnomalyAlert> findBySensorIdAndSeverity(String sensorId, String severity);

    List<AnomalyAlert> findByReadingTimestampBetween(Instant from, Instant to);
}