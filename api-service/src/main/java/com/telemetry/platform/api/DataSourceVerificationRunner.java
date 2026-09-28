package com.telemetry.platform.api;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@Component
public class DataSourceVerificationRunner implements CommandLineRunner {

    private final DataSource dataSource;

    public DataSourceVerificationRunner(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Injected DataSource implementation: " + dataSource.getClass().getName());
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM anomaly_alerts")) {
            rs.next();
            System.out.println("Row count in anomaly_alerts (via Spring Boot auto-configured DataSource): " + rs.getLong(1));
        }
    }
}