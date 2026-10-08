package com.telemetry.platform.api.alert;

import java.util.List;

public class InvalidAlertQueryException extends RuntimeException {

    private final List<String> errors;

    public InvalidAlertQueryException(List<String> errors) {
        super("Invalid alert query: " + String.join("; ", errors));
        this.errors = List.copyOf(errors);
    }

    public List<String> getErrors() {
        return errors;
    }
}