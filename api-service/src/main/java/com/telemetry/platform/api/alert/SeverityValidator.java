package com.telemetry.platform.api.alert;

import com.telemetry.platform.events.AnomalySeverity;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

public class SeverityValidator implements ConstraintValidator<ValidSeverity, String> {

    // Built from the enum itself, so this list can never drift from the real contract.
    // Safe to put in a message template because it is a constant, not user input.
    private static final String ALLOWED = Arrays.stream(AnomalySeverity.values())
            .map(Enum::name)
            .collect(Collectors.joining(", "));

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {

        if (value == null) {
            return true; // not provided; presence is a different rule
        }

        try {
            AnomalySeverity.valueOf(value.toUpperCase(Locale.ROOT));
            return true;
        } catch (IllegalArgumentException e) {

            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate(
                    "severity must be one of " + ALLOWED + " (case-insensitive)"
            ).addConstraintViolation();

            return false;
        }
    }
}