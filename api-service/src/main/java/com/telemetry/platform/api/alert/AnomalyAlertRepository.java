package com.telemetry.platform.api.alert;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;

public interface AnomalyAlertRepository extends JpaRepository<AnomalyAlert, Long> {

    @EntityGraph(attributePaths = "sensor")
    @Query("SELECT a FROM AnomalyAlert a")
    Page<AnomalyAlert> findAllWithSensor(Pageable pageable);

    @EntityGraph(attributePaths = "sensor")
    Page<AnomalyAlert> findBySensorId(String sensorId, Pageable pageable);

    @EntityGraph(attributePaths = "sensor")
    Page<AnomalyAlert> findBySeverity(String severity, Pageable pageable);

    @EntityGraph(attributePaths = "sensor")
    Page<AnomalyAlert> findBySensorIdAndSeverity(String sensorId, String severity, Pageable pageable);

    @EntityGraph(attributePaths = "sensor")
    Page<AnomalyAlert> findByReadingTimestampBetween(Instant from, Instant to, Pageable pageable);
}