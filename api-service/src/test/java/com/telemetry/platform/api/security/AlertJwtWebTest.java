
package com.telemetry.platform.api.security;

import com.telemetry.platform.api.alert.AlertService;
import com.telemetry.platform.api.alert.AnomalyAlertController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

import java.time.Instant;
import java.util.List;

import static com.telemetry.platform.api.security.TestJwtSupport.ISSUER;
import static com.telemetry.platform.api.security.TestJwtSupport.OTHER_KEY;
import static com.telemetry.platform.api.security.TestJwtSupport.SIGNING_KEY;
import static com.telemetry.platform.api.security.TestJwtSupport.token;
import static com.telemetry.platform.api.security.TestJwtSupport.validToken;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AnomalyAlertController.class)
@Import({
        SecurityConfig.class,
        ProblemAuthenticationEntryPoint.class,
        ProblemAccessDeniedHandler.class,
        TestJwtSupport.Config.class
})
class AlertJwtWebTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AlertService alertService;

    @BeforeEach
    void defaultEmptyPage() {
        when(alertService.findAlerts(any(), any())).thenReturn(Page.empty());
    }

    private static RequestPostProcessor reader() {
        return request -> {
            request.addHeader(
                    "Authorization",
                    "Bearer " + validToken(List.of("ALERT_READER"))
            );
            return request;
        };
    }

    @Test
    void validTokenWithReaderRoleGets200() throws Exception {
        mockMvc.perform(get("/api/alerts")
                        .with(reader()))
                .andExpect(status().isOk());
    }

    @Test
    void expiredTokenGets401() throws Exception {
        String expiredToken = token(
                SIGNING_KEY,
                ISSUER,
                List.of("ALERT_READER"),
                Instant.now().minusSeconds(3600)
        );

        mockMvc.perform(get("/api/alerts")
                        .header("Authorization", "Bearer " + expiredToken))
                .andExpect(status().isUnauthorized())
                .andExpect(header().exists("X-Correlation-Id"))
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
    }

    @Test
    void tokenSignedWithAnotherKeyGets401() throws Exception {
        String invalidSignatureToken = token(
                OTHER_KEY,
                ISSUER,
                List.of("ALERT_READER"),
                Instant.now().plusSeconds(3600)
        );

        mockMvc.perform(get("/api/alerts")
                        .header("Authorization", "Bearer " + invalidSignatureToken))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
    }

    @Test
    void tokenFromAnotherIssuerGets401() throws Exception {
        String invalidIssuerToken = token(
                SIGNING_KEY,
                "someone-else",
                List.of("ALERT_READER"),
                Instant.now().plusSeconds(3600)
        );

        mockMvc.perform(get("/api/alerts")
                        .header("Authorization", "Bearer " + invalidIssuerToken))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void validTokenWithoutReaderRoleGets403() throws Exception {
        String guestToken = validToken(List.of("GUEST"));

        mockMvc.perform(get("/api/alerts")
                        .header("Authorization", "Bearer " + guestToken))
                .andExpect(status().isForbidden())
                .andExpect(header().exists("X-Correlation-Id"))
                .andExpect(jsonPath("$.code").value("FORBIDDEN"));
    }

    @Test
    void garbageTokenGets401() throws Exception {
        mockMvc.perform(get("/api/alerts")
                        .header("Authorization", "Bearer not-a-jwt"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
    }
}
