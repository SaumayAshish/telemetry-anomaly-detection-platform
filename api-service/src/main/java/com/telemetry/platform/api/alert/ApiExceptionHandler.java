package com.telemetry.platform.api.alert;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.net.URI;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.List;
import java.util.UUID;
import org.slf4j.MDC;

@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);
    private static final String RETRY_AFTER_SECONDS = "5";

    @ExceptionHandler(InvalidAlertQueryException.class)
    public ProblemDetail handleInvalidQuery(InvalidAlertQueryException ex,
                                            HttpServletRequest request) {

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "The request parameters are invalid.");
        problem.setTitle("Invalid query");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("code", "INVALID_QUERY");
        problem.setProperty("errors", ex.getErrors());
        return problem;
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail handleTypeMismatch(MethodArgumentTypeMismatchException ex,
                                            HttpServletRequest request) {

        String expected = ex.getRequiredType() == null
                ? "value" : ex.getRequiredType().getSimpleName();

        // The rejected value itself is deliberately not echoed back to the client.
        String hint = "Instant".equals(expected)
                ? " (ISO-8601, for example 2026-10-01T00:00:00Z)" : "";

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "A request parameter could not be read.");
        problem.setTitle("Invalid parameter");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("code", "INVALID_PARAMETER");
        problem.setProperty("errors", List.of(
                "Parameter '" + ex.getName() + "' must be a valid " + expected + hint));
        return problem;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex, HttpServletRequest request,
                                          HttpServletResponse response) {

        String correlationId = MDC.get(CorrelationIdFilter.MDC_KEY);
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString().substring(0, 8);
        }

        if (isDatabaseUnavailable(ex)) {
            // The class name only: during an outage every request would otherwise log a stack trace.
            log.warn("Database unavailable, correlationId={}, path={}, exception={}",
                    correlationId, request.getRequestURI(), ex.getClass().getSimpleName());

            response.setHeader("Retry-After", RETRY_AFTER_SECONDS);

            ProblemDetail unavailable = ProblemDetail.forStatusAndDetail(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "The service is temporarily unavailable. Please retry shortly.");
            unavailable.setTitle("Service unavailable");
            unavailable.setInstance(URI.create(request.getRequestURI()));
            unavailable.setProperty("code", "SERVICE_UNAVAILABLE");
            unavailable.setProperty("correlationId", correlationId);
            return unavailable;
        }
        // Full detail and stack trace stay on the server, tagged with the same id.
        log.error("Unhandled exception, correlationId={}, path={}",
                correlationId, request.getRequestURI(), ex);

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred.");
        problem.setTitle("Internal error");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("code", "INTERNAL_ERROR");
        problem.setProperty("correlationId", correlationId);
        return problem;
    }

    // Walks the cause chain: the driver, the pool and Hibernate each wrap the same failure,
    // so matching one outer class would stop working as soon as a layer changes.
    private static boolean isDatabaseUnavailable(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof SQLTransientConnectionException
                    || t instanceof SQLNonTransientConnectionException) {
                return true;
            }
        }
        return false;
    }
}
