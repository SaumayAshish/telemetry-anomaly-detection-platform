package com.telemetry.platform.api.alert;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class LazyLoadingVerificationRunner implements CommandLineRunner {

    private final AnomalyAlertRepository repository;

    public LazyLoadingVerificationRunner(AnomalyAlertRepository repository) {
        this.repository = repository;
    }

    @Override
    public void run(String... args) {
        List<AnomalyAlert> alerts = repository.findAll();
        if (alerts.isEmpty()) {
            System.out.println("No alerts found - skipping lazy-loading verification.");
            return;
        }
        AnomalyAlert first = alerts.get(0);
        System.out.println("Loaded AnomalyAlert id=" + first.getId()
                + " sensorId=" + first.getSensorId()
                + " - only the SELECT above should appear in the Hibernate log; "
                + "the lazy sensor association was never touched, so no join to sensors happened.");
    }
}
