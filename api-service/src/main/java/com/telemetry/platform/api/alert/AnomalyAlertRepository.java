package com.telemetry.platform.api.alert;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;

public interface AnomalyAlertRepository extends JpaRepository<AnomalyAlert, Long> {

    @EntityGraph(attributePaths = "sensor")
    @Query("SELECT a FROM AnomalyAlert a")
    List<AnomalyAlert> findAllWithSensor();

    @EntityGraph(attributePaths = "sensor")
    List<AnomalyAlert> findBySensorId(String sensorId);

    @EntityGraph(attributePaths = "sensor")
    List<AnomalyAlert> findBySeverity(String severity);

    @EntityGraph(attributePaths = "sensor")
    List<AnomalyAlert> findBySensorIdAndSeverity(String sensorId, String severity);

    @EntityGraph(attributePaths = "sensor")
    List<AnomalyAlert> findByReadingTimestampBetween(Instant from, Instant to);
}