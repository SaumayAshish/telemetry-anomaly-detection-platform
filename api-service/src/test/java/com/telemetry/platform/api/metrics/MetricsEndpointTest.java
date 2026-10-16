package com.telemetry.platform.api.metrics;

import static com.telemetry.platform.api.security.TestJwtSupport.validToken;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.telemetry.platform.api.security.TestJwtSupport;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The whole application, real security and a real PostgreSQL: the Prometheus endpoint exists,
 * is readable only with the METRICS_READER role, and exposes JVM, connection pool and HTTP
 * request measurements.
 */
@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureMetrics
class MetricsEndpointTest {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16");
    private static final Path PUBLIC_KEY = writePublicKey();

    static {
        POSTGRES.start();
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .load()
                .migrate();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("app.security.jwt.public-key-path", PUBLIC_KEY::toString);
        registry.add("app.security.jwt.issuer", () -> TestJwtSupport.ISSUER);
    }

    @Autowired
    private MockMvc mockMvc;

    private static String bearer(String role) {
        return "Bearer " + validToken(List.of(role));
    }

    @Test
    void anonymousScrapeIsRejected() throws Exception {
        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void anAlertReaderMayNotScrape() throws Exception {
        mockMvc.perform(get("/actuator/prometheus").header("Authorization", bearer("ALERT_READER")))
                .andExpect(status().isForbidden());
    }

    @Test
    void aMetricsReaderGetsJvmAndConnectionPoolMeasurements() throws Exception {
        mockMvc.perform(get("/actuator/prometheus").header("Authorization", bearer("METRICS_READER")))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("jvm_memory_used_bytes")))
                .andExpect(content().string(containsString("hikaricp_connections_active")));
    }

    @Test
    void requestsToTheAlertsApiAreCounted() throws Exception {
        mockMvc.perform(get("/api/alerts").header("Authorization", bearer("ALERT_READER")))
                .andExpect(status().isOk());

        mockMvc.perform(get("/actuator/prometheus").header("Authorization", bearer("METRICS_READER")))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("http_server_requests_seconds_count")))
                .andExpect(content().string(containsString("uri=\"/api/alerts\"")));
    }

    private static Path writePublicKey() {
        try {
            String body = Base64.getMimeEncoder(64, "\n".getBytes())
                    .encodeToString(TestJwtSupport.SIGNING_KEY.toRSAPublicKey().getEncoded());
            Path file = Files.createTempFile("test-public-key", ".pem");
            file.toFile().deleteOnExit();
            Files.writeString(file,
                    "-----BEGIN PUBLIC KEY-----\n" + body + "\n-----END PUBLIC KEY-----\n");
            return file;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}