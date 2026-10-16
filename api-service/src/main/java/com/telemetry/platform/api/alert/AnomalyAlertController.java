package com.telemetry.platform.api.alert;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/api/alerts")
public class AnomalyAlertController {

    private static final int MAX_PAGE_SIZE = 200;

    private final AlertService alertService;
    private final Validator validator;

    public AnomalyAlertController(AlertService alertService, Validator validator) {
        this.alertService = alertService;
        this.validator = validator;
    }

    @GetMapping
    public ResponseEntity<List<AlertResponse>> getAlerts(
            @RequestParam(name = "sensorId", required = false) String sensorId,
            @RequestParam(name = "severity", required = false) String severity,
            @RequestParam(name = "from", required = false) Instant from,
            @RequestParam(name = "to", required = false) Instant to,
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "50") int size) {

        AlertQuery query = new AlertQuery(sensorId, severity, from, to);

        Set<ConstraintViolation<AlertQuery>> violations = validator.validate(query);

        List<String> errors = new ArrayList<>(violations.stream()
                .map(ConstraintViolation::getMessage)
                .sorted()
                .toList());
        if (page < 0) {
            errors.add("page must not be negative");
        }
        if (size < 1 || size > MAX_PAGE_SIZE) {
            errors.add("size must be between 1 and " + MAX_PAGE_SIZE);
        }
        if (!errors.isEmpty()) {
            throw new InvalidAlertQueryException(errors);
        }

        Pageable pageable = PageRequest.of(page, size,
                Sort.by(Sort.Order.desc("readingTimestamp"), Sort.Order.desc("id")));
        Page<AlertResponse> result = alertService.findAlerts(query, pageable);

        return ResponseEntity.ok()
                .header("X-Total-Count", String.valueOf(result.getTotalElements()))
                .body(result.getContent());
    }
}