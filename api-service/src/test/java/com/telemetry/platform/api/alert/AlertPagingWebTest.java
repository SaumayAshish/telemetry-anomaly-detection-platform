package com.telemetry.platform.api.alert;

import com.telemetry.platform.api.security.ProblemAccessDeniedHandler;
import com.telemetry.platform.api.security.ProblemAuthenticationEntryPoint;
import com.telemetry.platform.api.security.SecurityConfig;
import com.telemetry.platform.api.security.TestJwtSupport;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

import static com.telemetry.platform.api.security.TestJwtSupport.validToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * GET /api/alerts is bounded and ordered: page and size select a slice, newest first with the id as
 * the tiebreaker so pages never overlap, and the total number of matches is reported in a header.
 */
@WebMvcTest(AnomalyAlertController.class)
@Import({SecurityConfig.class, ProblemAuthenticationEntryPoint.class,
        ProblemAccessDeniedHandler.class, TestJwtSupport.Config.class})
class AlertPagingWebTest {

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
            request.addHeader("Authorization", "Bearer " + validToken(List.of("ALERT_READER")));
            return request;
        };
    }

    private Pageable requestedPage() {
        ArgumentCaptor<Pageable> captor = ArgumentCaptor.forClass(Pageable.class);
        verify(alertService).findAlerts(any(), captor.capture());
        return captor.getValue();
    }

    @Test
    void defaultsToTheFirstPageOfFiftyNewestFirst() throws Exception {
        mockMvc.perform(get("/api/alerts").with(reader())).andExpect(status().isOk());

        Pageable page = requestedPage();
        assertThat(page.getPageNumber()).isZero();
        assertThat(page.getPageSize()).isEqualTo(50);
        assertThat(page.getSort().stream().map(o -> o.getProperty() + ":" + o.getDirection()).toList())
                .containsExactly("readingTimestamp:DESC", "id:DESC");
    }

    @Test
    void passesPageAndSizeThrough() throws Exception {
        mockMvc.perform(get("/api/alerts").param("page", "2").param("size", "10").with(reader()))
                .andExpect(status().isOk());

        Pageable page = requestedPage();
        assertThat(page.getPageNumber()).isEqualTo(2);
        assertThat(page.getPageSize()).isEqualTo(10);
    }

    @Test
    void acceptsTheLargestAllowedSize() throws Exception {
        mockMvc.perform(get("/api/alerts").param("size", "200").with(reader()))
                .andExpect(status().isOk());

        assertThat(requestedPage().getPageSize()).isEqualTo(200);
    }

    @Test
    void rejectsASizeOfZero() throws Exception {
        mockMvc.perform(get("/api/alerts").param("size", "0").with(reader()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_QUERY"))
                .andExpect(jsonPath("$.errors[0]").value("size must be between 1 and 200"));
    }

    @Test
    void rejectsASizeAboveTheLimit() throws Exception {
        mockMvc.perform(get("/api/alerts").param("size", "201").with(reader()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]").value("size must be between 1 and 200"));
    }

    @Test
    void rejectsANegativePage() throws Exception {
        mockMvc.perform(get("/api/alerts").param("page", "-1").with(reader()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_QUERY"))
                .andExpect(jsonPath("$.errors[0]").value("page must not be negative"));
    }

    @Test
    void rejectsANonNumericSize() throws Exception {
        mockMvc.perform(get("/api/alerts").param("size", "abc").with(reader()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_PARAMETER"));
    }

    @Test
    void reportsTheTotalNumberOfMatchesInAHeader() throws Exception {
        when(alertService.findAlerts(any(), any()))
                .thenReturn(new PageImpl<>(List.of(), PageRequest.of(0, 50), 123));

        mockMvc.perform(get("/api/alerts").with(reader()))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Total-Count", "123"));
    }

    @Test
    void reportsAZeroTotalWhenNothingMatches() throws Exception {
        mockMvc.perform(get("/api/alerts").with(reader()))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Total-Count", "0"));
    }
}
