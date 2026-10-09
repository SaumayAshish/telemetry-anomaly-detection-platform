package com.telemetry.platform.api.alert;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

@Component
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";

    // The inbound header is untrusted input. Accept only short, plain identifiers;
    // anything else (spaces, control characters, very long values) is replaced.
    private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {

        String id = request.getHeader(HEADER);

        if (id == null || !SAFE_ID.matcher(id).matches()) {
            id = UUID.randomUUID().toString().substring(0, 8);
        }

        MDC.put(MDC_KEY, id);
        response.setHeader(HEADER, id);

        try {
            chain.doFilter(request, response);
        } finally {
            // Tomcat reuses worker threads: without this, the next request handled
            // by this thread would inherit this request's id.
            MDC.remove(MDC_KEY);
        }
    }
}