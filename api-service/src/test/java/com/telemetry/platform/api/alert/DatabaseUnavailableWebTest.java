package com.telemetry.platform.api.alert;

import com.telemetry.platform.api.security.ProblemAccessDeniedHandler;
import com.telemetry.platform.api.security.ProblemAuthenticationEntryPoint;
import com.telemetry.platform.api.security.SecurityConfig;
import com.telemetry.platform.api.security.TestJwtSupport;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransientConnectionException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.transaction.CannotCreateTransactionException;

import static com.telemetry.platform.api.security.TestJwtSupport.validToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * A database outage is the service being unavailable, not a bug in it: clients get 503 with
 * Retry-After and no driver or pool details, while unrelated failures stay 500.
 */
@WebMvcTest(AnomalyAlertController.class)
@Import({SecurityConfig.class, ProblemAuthenticationEntryPoint.class,
        ProblemAccessDeniedHandler.class, TestJwtSupport.Config.class})
class DatabaseUnavailableWebTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AlertService alertService;

    private static RequestPostProcessor reader() {
        return request -> {
            request.addHeader("Authorization", "Bearer " + validToken(List.of("ALERT_READER")));
            return request;
        };
    }

    private static CannotCreateTransactionException outage() {
        return new CannotCreateTransactionException(
                "Could not open JPA EntityManager for transaction",
                new SQLTransientConnectionException(
                        "HikariPool-1 - Connection is not available, request timed out after 3000ms"));
    }

    @Test
    void aDatabaseOutageGets503WithRetryAfter() throws Exception {
        when(alertService.findAlerts(any(), any())).thenThrow(outage());

        mockMvc.perform(get("/api/alerts").with(reader()))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "5"))
                .andExpect(header().exists("X-Correlation-Id"))
                .andExpect(jsonPath("$.code").value("SERVICE_UNAVAILABLE"))
                .andExpect(jsonPath("$.correlationId").exists());
    }

    @Test
    void theOutageResponseDoesNotLeakDriverOrPoolDetails() throws Exception {
        when(alertService.findAlerts(any(), any())).thenThrow(outage());

        String body = mockMvc.perform(get("/api/alerts").with(reader()))
                .andExpect(status().isServiceUnavailable())
                .andReturn().getResponse().getContentAsString();

        assertThat(body).doesNotContain("Hikari", "EntityManager", "JDBC", "timed out");
    }

    @Test
    void anUnrelatedFailureStillGets500() throws Exception {
        when(alertService.findAlerts(any(), any())).thenThrow(new IllegalStateException("boom"));

        mockMvc.perform(get("/api/alerts").with(reader()))
                .andExpect(status().isInternalServerError())
                .andExpect(header().doesNotExist("Retry-After"))
                .andExpect(jsonPath("$.code").value("INTERNAL_ERROR"));
    }

    @Test
    void aConstraintViolationIsNotAnOutage() throws Exception {
        when(alertService.findAlerts(any(), any())).thenThrow(new DataIntegrityViolationException(
                "could not execute statement", new SQLIntegrityConstraintViolationException("duplicate key")));

        mockMvc.perform(get("/api/alerts").with(reader()))
                .andExpect(status().isInternalServerError())
                .andExpect(header().doesNotExist("Retry-After"))
                .andExpect(jsonPath("$.code").value("INTERNAL_ERROR"));
    }
}
