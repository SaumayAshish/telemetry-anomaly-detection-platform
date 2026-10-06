package com.telemetry.platform.api.alert;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class LazyLoadingVerificationRunner implements CommandLineRunner {

    private final AnomalyAlertRepository repository;

    public LazyLoadingVerificationRunner(AnomalyAlertRepository repository) {
        this.repository = repository;
    }

    @Override
    @Transactional
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

        System.out.println("Now touching getSensor() on all " + alerts.size() + " alerts:");
        for (AnomalyAlert alert : alerts) {
            Sensor sensor = alert.getSensor(); // proxy access — watch the log for a new SELECT per distinct sensor_id
            System.out.println("  alert id=" + alert.getId()
                    + " sensorId=" + alert.getSensorId()
                    + " -> location=" + sensor.getLocation());
        }
    }
}
