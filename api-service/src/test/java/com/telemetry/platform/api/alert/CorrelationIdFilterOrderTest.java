package com.telemetry.platform.api.alert;

import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.OrderUtils;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationIdFilterOrderTest {

    // Spring Security's filter chain is registered at order -100 (as I understand
    // Security's default). MockMvc does not reproduce the real container's filter
    // ordering, so the web-layer tests cannot catch a missing @Order; this guards
    // the annotation itself.
    @Test
    void correlationIdFilterRunsBeforeTheSecurityFilterChain() {
        Integer order = OrderUtils.getOrder(CorrelationIdFilter.class);

        assertThat(order).isNotNull();
        assertThat(order).isLessThan(-100);
    }
}