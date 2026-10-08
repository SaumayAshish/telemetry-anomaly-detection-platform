package com.telemetry.platform.api.alert;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

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
    public ProblemDetail handleUnexpected(Exception ex, HttpServletRequest request) {

        String correlationId = UUID.randomUUID().toString().substring(0, 8);

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
}
