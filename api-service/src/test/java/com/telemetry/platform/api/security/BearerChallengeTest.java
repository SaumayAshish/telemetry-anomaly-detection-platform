package com.telemetry.platform.api.security;

import com.telemetry.platform.api.alert.AlertService;
import com.telemetry.platform.api.alert.AnomalyAlertController;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static com.telemetry.platform.api.security.TestJwtSupport.ISSUER;
import static com.telemetry.platform.api.security.TestJwtSupport.OTHER_KEY;
import static com.telemetry.platform.api.security.TestJwtSupport.SIGNING_KEY;
import static com.telemetry.platform.api.security.TestJwtSupport.token;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * RFC 6750: a request that carried a token the server rejected is told so with
 * error="invalid_token", while a request with no token only gets the bare challenge.
 */
@WebMvcTest(AnomalyAlertController.class)
@Import({SecurityConfig.class, ProblemAuthenticationEntryPoint.class,
        ProblemAccessDeniedHandler.class, TestJwtSupport.Config.class})
class BearerChallengeTest {

    private static final String BARE = "Bearer realm=\"telemetry\"";
    private static final String INVALID_TOKEN = "Bearer realm=\"telemetry\", error=\"invalid_token\"";

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AlertService alertService;

    @Test
    void aRequestWithoutATokenGetsTheBareChallenge() throws Exception {
        mockMvc.perform(get("/api/alerts"))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string("WWW-Authenticate", BARE))
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
    }

    @Test
    void anExpiredTokenIsChallengedAsInvalid() throws Exception {
        String expired = token(SIGNING_KEY, ISSUER, List.of("ALERT_READER"), Instant.now().minusSeconds(3600));

        mockMvc.perform(get("/api/alerts").header("Authorization", "Bearer " + expired))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string("WWW-Authenticate", INVALID_TOKEN))
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
    }

    @Test
    void aTokenSignedWithAnotherKeyIsChallengedAsInvalid() throws Exception {
        String forged = token(OTHER_KEY, ISSUER, List.of("ALERT_READER"), Instant.now().plusSeconds(3600));

        mockMvc.perform(get("/api/alerts").header("Authorization", "Bearer " + forged))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string("WWW-Authenticate", INVALID_TOKEN));
    }

    @Test
    void aGarbageTokenIsChallengedAsInvalid() throws Exception {
        mockMvc.perform(get("/api/alerts").header("Authorization", "Bearer not-a-jwt"))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string("WWW-Authenticate", INVALID_TOKEN));
    }
}
