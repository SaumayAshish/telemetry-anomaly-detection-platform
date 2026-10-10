package com.telemetry.platform.api.security;

import com.telemetry.platform.api.alert.AlertService;
import com.telemetry.platform.api.alert.AnomalyAlertController;
// IntelliJ: add the imports for @WebMvcTest and @MockitoBean here
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@WebMvcTest(AnomalyAlertController.class)
@Import({SecurityConfig.class, ProblemAuthenticationEntryPoint.class, ProblemAccessDeniedHandler.class,TestJwtSupport.Config.class })
@TestPropertySource(properties = {
        "app.security.reader-password=test-reader-pass",
        "app.security.guest-password=test-guest-pass"
})
class AlertSecurityWebTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AlertService alertService;

    @Test
    void anonymousRequestGets401WithProblemBodyAndCorrelationId() throws Exception {
        mockMvc.perform(get("/api/alerts"))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string("WWW-Authenticate", startsWith("Basic")))
                .andExpect(header().exists("X-Correlation-Id"))
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
    }

    @Test
    void wrongPasswordGets401() throws Exception {
        mockMvc.perform(get("/api/alerts").with(httpBasic("reader", "wrong")))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
    }

    @Test
    void guestIsAuthenticatedButForbidden() throws Exception {
        mockMvc.perform(get("/api/alerts").with(httpBasic("guest", "test-guest-pass")))
                .andExpect(status().isForbidden())
                .andExpect(header().exists("X-Correlation-Id"))
                .andExpect(jsonPath("$.code").value("FORBIDDEN"));
    }

    @Test
    void readerGets200() throws Exception {
        mockMvc.perform(get("/api/alerts").with(httpBasic("reader", "test-reader-pass")))
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
                        .with(httpBasic("reader", "test-reader-pass")))
                .andExpect(header().string("X-Correlation-Id", "demo-123"));
    }

    @Test
    void hostileInboundCorrelationIdIsReplaced() throws Exception {
        mockMvc.perform(get("/api/alerts")
                        .header("X-Correlation-Id", "bad value with spaces")
                        .with(httpBasic("reader", "test-reader-pass")))
                .andExpect(header().string("X-Correlation-Id", not("bad value with spaces")));
    }
}