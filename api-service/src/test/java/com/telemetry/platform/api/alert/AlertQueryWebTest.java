package com.telemetry.platform.api.alert;

import com.telemetry.platform.api.security.ProblemAccessDeniedHandler;
import com.telemetry.platform.api.security.ProblemAuthenticationEntryPoint;
import com.telemetry.platform.api.security.SecurityConfig;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AnomalyAlertController.class)
@Import({SecurityConfig.class, ProblemAuthenticationEntryPoint.class, ProblemAccessDeniedHandler.class})
@TestPropertySource(properties = {
        "app.security.reader-password=test-reader-pass",
        "app.security.guest-password=test-guest-pass"
})
class AlertQueryWebTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AlertService alertService;

    private static RequestPostProcessor reader() {
        return httpBasic("reader", "test-reader-pass");
    }

    @Test
    void fromWithoutToIsRejected() throws Exception {
        mockMvc.perform(get("/api/alerts").param("from", "2026-10-01T00:00:00Z").with(reader()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_QUERY"))
                .andExpect(jsonPath("$.errors[0]").value("'from' and 'to' must be provided together"));
    }

    @Test
    void reversedRangeIsRejected() throws Exception {
        mockMvc.perform(get("/api/alerts")
                        .param("from", "2026-10-08T00:00:00Z")
                        .param("to", "2026-10-01T00:00:00Z")
                        .with(reader()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]").value("'from' must not be later than 'to'"));
    }

    @Test
    void unknownSeverityIsRejected() throws Exception {
        mockMvc.perform(get("/api/alerts").param("severity", "BANANA").with(reader()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]")
                        .value("severity must be one of LOW, MEDIUM, HIGH, CRITICAL (case-insensitive)"));
    }

    @Test
    void blankSeverityIsRejected() throws Exception {
        mockMvc.perform(get("/api/alerts").param("severity", "").with(reader()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_QUERY"));
    }

    @Test
    void rangeCombinedWithSensorIdIsRejected() throws Exception {
        mockMvc.perform(get("/api/alerts")
                        .param("sensorId", "S-1001")
                        .param("from", "2026-09-01T00:00:00Z")
                        .param("to", "2026-10-31T00:00:00Z")
                        .with(reader()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]")
                        .value("a time range cannot be combined with sensorId or severity"));
    }

    @Test
    void bareDateIsRejectedAsInvalidParameter() throws Exception {
        mockMvc.perform(get("/api/alerts").param("from", "2026-10-01").with(reader()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_PARAMETER"));
    }

    @Test
    void equalFromAndToIsAValidRange() throws Exception {
        mockMvc.perform(get("/api/alerts")
                        .param("from", "2026-09-18T13:11:15Z")
                        .param("to", "2026-09-18T13:11:15Z")
                        .with(reader()))
                .andExpect(status().isOk());
    }

    @Test
    void lowercaseSeverityIsAcceptedAndNormalized() throws Exception {
        mockMvc.perform(get("/api/alerts").param("severity", "critical").with(reader()))
                .andExpect(status().isOk());

        ArgumentCaptor<AlertQuery> captor = ArgumentCaptor.forClass(AlertQuery.class);
        verify(alertService).findAlerts(captor.capture());
        assertThat(captor.getValue().normalizedSeverity()).isEqualTo("CRITICAL");
    }

    @Test
    void unexpectedFailureGivesGeneric500WithMatchingCorrelationId() throws Exception {
        when(alertService.findAlerts(any())).thenThrow(new IllegalStateException("secret detail"));

        MvcResult result = mockMvc.perform(get("/api/alerts")
                        .header("X-Correlation-Id", "boom-1")
                        .with(reader()))
                .andExpect(status().isInternalServerError())
                .andExpect(header().string("X-Correlation-Id", "boom-1"))
                .andExpect(jsonPath("$.code").value("INTERNAL_ERROR"))
                .andExpect(jsonPath("$.correlationId").value("boom-1"))
                .andReturn();

        assertThat(result.getResponse().getContentAsString()).doesNotContain("secret detail");
    }
}
