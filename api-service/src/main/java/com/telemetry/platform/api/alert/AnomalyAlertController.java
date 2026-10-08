package com.telemetry.platform.api.alert;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/alerts")
public class AnomalyAlertController {

    private final AlertService alertService;
    private final Validator validator;

    public AnomalyAlertController(AlertService alertService, Validator validator) {
        this.alertService = alertService;
        this.validator = validator;
    }

    @GetMapping
    public List<AlertResponse> getAlerts(
            @RequestParam(name = "sensorId", required = false) String sensorId,
            @RequestParam(name = "severity", required = false) String severity,
            @RequestParam(name = "from", required = false) Instant from,
            @RequestParam(name = "to", required = false) Instant to) {

        AlertQuery query = new AlertQuery(sensorId, severity, from, to);

        Set<ConstraintViolation<AlertQuery>> violations = validator.validate(query);

        if (!violations.isEmpty()) {
            String summary = violations.stream()
                    .map(ConstraintViolation::getMessage)
                    .sorted()
                    .collect(Collectors.joining("; "));

            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, summary);
        }

        return alertService.findAlerts(query);
    }
}