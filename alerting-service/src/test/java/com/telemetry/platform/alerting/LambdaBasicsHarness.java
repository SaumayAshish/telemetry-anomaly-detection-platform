package com.telemetry.platform.alerting;

import java.util.function.Predicate;

/**
 * Phase 7, Step 1 — Lambda Expressions.
 *
 * Plain main()-based harness (same convention as RollingStatsVerificationHarness
 * and AnomalyDecisionEngineVerificationHarness): no JUnit, no Kafka, no DB.
 * Run main() directly from IntelliJ.
 */
public class LambdaBasicsHarness {

    // A custom functional interface: exactly ONE abstract method (SAM = Single
    // Abstract Method). @FunctionalInterface isn't required for a lambda to work
    // here, but it makes the compiler ERROR if a second abstract method is ever
    // added by mistake, instead of silently breaking every lambda implementing it.
    @FunctionalInterface
    interface ThresholdCheck {
        boolean exceeds(double value, double threshold);
    }

    // The pre-lambda way: a named class implementing the interface.
    static class NamedThresholdCheck implements ThresholdCheck {
        @Override
        public boolean exceeds(double value, double threshold) {
            return value > threshold;
        }
    }

    public static void main(String[] args) {

        System.out.println("=== Step 1: Named class ===");
        ThresholdCheck namedVersion = new NamedThresholdCheck();
        System.out.println("72.5 > 65.0 ? " + namedVersion.exceeds(72.5, 65.0));
        System.out.println("Implementation class: " + namedVersion.getClass().getName());

        System.out.println();
        System.out.println("=== Step 2: Anonymous inner class ===");
        ThresholdCheck anonymousVersion = new ThresholdCheck() {
            @Override
            public boolean exceeds(double value, double threshold) {
                return value > threshold;
            }
        };
        System.out.println("72.5 > 65.0 ? " + anonymousVersion.exceeds(72.5, 65.0));
        System.out.println("Implementation class: " + anonymousVersion.getClass().getName());

        System.out.println();
        System.out.println("=== Step 3: Lambda expression ===");
        ThresholdCheck lambdaVersion = (value, threshold) -> value > threshold;
        System.out.println("72.5 > 65.0 ? " + lambdaVersion.exceeds(72.5, 65.0));
        System.out.println("Implementation class: " + lambdaVersion.getClass().getName());

        System.out.println();
        System.out.println("=== Step 4: Effectively-final variable capture ===");
        double capturedBaselineMean = 75.09; // never reassigned after this -> effectively final
        ThresholdCheck baselineCheck = (value, threshold) -> {
            // capturedBaselineMean is captured from the enclosing scope.
            // Uncomment the next line and the file will FAIL TO COMPILE:
            // capturedBaselineMean = 80.0;
            System.out.println("  (captured baseline mean was: " + capturedBaselineMean + ")");
            return value > threshold;
        };
        baselineCheck.exceeds(90.0, 85.0);

        System.out.println();
        System.out.println("=== Step 5: The built-in equivalent — java.util.function.Predicate ===");
        // ThresholdCheck takes TWO arguments (value, threshold). Predicate<T> takes
        // exactly ONE and returns boolean, so to reuse it here the threshold has to
        // be "baked in" as a captured variable instead of a parameter:
        double fixedThreshold = 65.0;
        Predicate<Double> isAboveFixedThreshold = value -> value > fixedThreshold;
        System.out.println("72.5 above fixed threshold 65.0 ? " + isAboveFixedThreshold.test(72.5));
        System.out.println("Implementation class: " + isAboveFixedThreshold.getClass().getName());
    }
}