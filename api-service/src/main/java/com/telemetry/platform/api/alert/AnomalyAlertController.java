package com.telemetry.platform.api.alert;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/alerts")
public class AnomalyAlertController {

    private final AlertService alertService;

    public AnomalyAlertController(AlertService alertService) {
        this.alertService = alertService;
    }

    @GetMapping
    public List<AnomalyAlert> getAlerts(
            @RequestParam(name = "sensorId", required = false) String sensorId,
            @RequestParam(name = "severity", required = false) String severity,
            @RequestParam(name = "from", required = false) Instant from,
            @RequestParam(name = "to", required = false) Instant to) {
        return alertService.findAlerts(sensorId, severity, from, to);
    }
}