package com.telemetry.platform.api.alert;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface AnomalyAlertRepository extends JpaRepository<AnomalyAlert, Long> {

    @Override
    @EntityGraph(attributePaths = "sensor")
    List<AnomalyAlert> findAll();

    @EntityGraph(attributePaths = "sensor")
    List<AnomalyAlert> findBySensorId(String sensorId);

    @EntityGraph(attributePaths = "sensor")
    List<AnomalyAlert> findBySeverity(String severity);

    @EntityGraph(attributePaths = "sensor")
    List<AnomalyAlert> findBySensorIdAndSeverity(String sensorId, String severity);

    @EntityGraph(attributePaths = "sensor")
    List<AnomalyAlert> findByReadingTimestampBetween(Instant from, Instant to);
}