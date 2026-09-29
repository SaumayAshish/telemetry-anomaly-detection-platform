package com.telemetry.platform.api.alert;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDate;

@Entity
@Table(name = "sensors")
public class Sensor {

    @Id
    @Column(name = "sensor_id")
    private String sensorId;

    @Column(name = "location", nullable = false)
    private String location;

    @Column(name = "install_date", nullable = false)
    private LocalDate installDate;

    @Column(name = "model", nullable = false)
    private String model;

    protected Sensor() {
        // Required by Hibernate.
    }

    public String getSensorId() { return sensorId; }
    public String getLocation() { return location; }
    public LocalDate getInstallDate() { return installDate; }
    public String getModel() { return model; }
}
