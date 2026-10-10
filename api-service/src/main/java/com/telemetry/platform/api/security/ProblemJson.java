package com.telemetry.platform.api.security;

import com.telemetry.platform.api.alert.CorrelationIdFilter;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;

import java.io.IOException;

final class ProblemJson {

    private ProblemJson() {
    }

    // Every value written here is a constant or the correlation id, which the
    // filter only accepts if it matches [A-Za-z0-9._-]{1,64}, so no JSON escaping
    // is needed. No user-supplied text is ever put into this body.
    static void write(HttpServletResponse response, int status, String title,
                      String detail, String code) throws IOException {

        String correlationId = MDC.get(CorrelationIdFilter.MDC_KEY);

        String body = "{\"title\":\"" + title + "\""
                + ",\"status\":" + status
                + ",\"detail\":\"" + detail + "\""
                + ",\"code\":\"" + code + "\""
                + (correlationId != null ? ",\"correlationId\":\"" + correlationId + "\"" : "")
                + "}";

        response.setStatus(status);
        response.setContentType("application/problem+json");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(body);
    }
}
