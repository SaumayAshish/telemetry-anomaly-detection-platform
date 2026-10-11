package com.telemetry.platform.api.security;

import com.telemetry.platform.api.alert.AlertService;
import com.telemetry.platform.api.alert.AnomalyAlertController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static com.telemetry.platform.api.security.TestJwtSupport.validToken;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AnomalyAlertController.class)
@Import({SecurityConfig.class, ProblemAuthenticationEntryPoint.class,
        ProblemAccessDeniedHandler.class, TestJwtSupport.Config.class})
class AlertSecurityWebTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AlertService alertService;

    private static String bearer(String role) {
        return "Bearer " + validToken(List.of(role));
    }

    @Test
    void anonymousRequestGets401WithProblemBodyAndCorrelationId() throws Exception {
        mockMvc.perform(get("/api/alerts"))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string("WWW-Authenticate", startsWith("Bearer")))
                .andExpect(header().exists("X-Correlation-Id"))
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
    }

    @Test
    void basicCredentialsAreNoLongerAccepted() throws Exception {
        mockMvc.perform(get("/api/alerts").with(httpBasic("reader", "anything")))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string("WWW-Authenticate", startsWith("Bearer")))
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
    }

    @Test
    void guestTokenIsAuthenticatedButForbidden() throws Exception {
        mockMvc.perform(get("/api/alerts").header("Authorization", bearer("GUEST")))
                .andExpect(status().isForbidden())
                .andExpect(header().exists("X-Correlation-Id"))
                .andExpect(jsonPath("$.code").value("FORBIDDEN"));
    }

    @Test
    void readerTokenGets200() throws Exception {
        mockMvc.perform(get("/api/alerts").header("Authorization", bearer("ALERT_READER")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());
    }

    @Test
    void unknownPathIsHiddenFromAnonymousCallers() throws Exception {
        mockMvc.perform(get("/api/nope"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void validInboundCorrelationIdIsEchoed() throws Exception {
        mockMvc.perform(get("/api/alerts")
                        .header("X-Correlation-Id", "demo-123")
                        .header("Authorization", bearer("ALERT_READER")))
                .andExpect(header().string("X-Correlation-Id", "demo-123"));
    }

    @Test
    void hostileInboundCorrelationIdIsReplaced() throws Exception {
        mockMvc.perform(get("/api/alerts")
                        .header("X-Correlation-Id", "bad value with spaces")
                        .header("Authorization", bearer("ALERT_READER")))
                .andExpect(header().string("X-Correlation-Id", not("bad value with spaces")));
    }
}