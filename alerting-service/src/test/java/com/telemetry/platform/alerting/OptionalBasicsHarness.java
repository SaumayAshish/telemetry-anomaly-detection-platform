package com.telemetry.platform.alerting;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class OptionalBasicsHarness {

    record Reading(String deviceId, double zScore) {}

    static Reading findByDeviceIdNullable(List<Reading> readings, String deviceId) {
        for (Reading r : readings) {
            if (r.deviceId().equals(deviceId)) {
                return r;
            }
        }
        return null;
    }

    static Optional<Reading> findByDeviceIdOptional(List<Reading> readings, String deviceId) {
        for (Reading r : readings) {
            if (r.deviceId().equals(deviceId)) {
                return Optional.of(r);
            }
        }
        return Optional.empty();
    }

    public static void main(String[] args) {

        List<Reading> readings = List.of(
                new Reading("sensor-A1", 21.5),
                new Reading("sensor-B2", 6.9),
                new Reading("sensor-C3", -12.2)
        );

        System.out.println("=== Step 1: null vs Optional as an explicit 'may be absent' signal ===");
        Reading nullableResult = findByDeviceIdNullable(readings, "sensor-Z9");
        System.out.println("findByDeviceIdNullable(\"sensor-Z9\") returns: " + nullableResult
                + " -- nothing in the method signature warns a caller this can happen.");

        Optional<Reading> optionalResult = findByDeviceIdOptional(readings, "sensor-Z9");
        System.out.println("findByDeviceIdOptional(\"sensor-Z9\") returns: " + optionalResult
                + " -- the return TYPE itself forces the caller to deal with absence.");

        System.out.println();
        System.out.println("=== Step 2: of() vs ofNullable() vs empty() ===");
        Optional<Reading> presentViaOf = Optional.of(readings.get(0));
        System.out.println("Optional.of(readings.get(0)): " + presentViaOf);

        Reading maybeNullReading = null;
        Optional<Reading> viaOfNullable = Optional.ofNullable(maybeNullReading);
        System.out.println("Optional.ofNullable(null): " + viaOfNullable);

        try {
            Optional.of(maybeNullReading);
            System.out.println("This line never runs.");
        } catch (NullPointerException e) {
            System.out.println("Optional.of(null) threw NullPointerException immediately, at creation - fail fast, not deferred.");
        }

        System.out.println();
        System.out.println("=== Step 3: map() and filter() - transform without unwrapping ===");
        Optional<Double> maybeZScore = findByDeviceIdOptional(readings, "sensor-A1").map(Reading::zScore);
        System.out.println("map(Reading::zScore) on a present Optional: " + maybeZScore);

        Optional<Double> maybeAnomalousZScore = findByDeviceIdOptional(readings, "sensor-B2")
                .map(Reading::zScore)
                .filter(z -> Math.abs(z) >= 15.0);
        System.out.println("map then filter(|z|>=15.0) on sensor-B2 (z=6.9, fails filter): " + maybeAnomalousZScore);

        System.out.println();
        System.out.println("=== Step 4: orElse() vs orElseGet() - eager vs lazy evaluation ===");
        Supplier<Double> loggingDefaultSupplier = () -> {
            System.out.println("  (computing expensive default - this line proves WHEN it runs)");
            return 0.0;
        };

        System.out.println("Calling orElse() on a PRESENT optional:");
        double viaOrElse = findByDeviceIdOptional(readings, "sensor-A1")
                .map(Reading::zScore)
                .orElse(loggingDefaultSupplier.get());
        System.out.println("  result: " + viaOrElse + " (note: the logging line above still printed, even though the value was present)");

        System.out.println("Calling orElseGet() on a PRESENT optional:");
        double viaOrElseGet = findByDeviceIdOptional(readings, "sensor-A1")
                .map(Reading::zScore)
                .orElseGet(loggingDefaultSupplier);
        System.out.println("  result: " + viaOrElseGet + " (note: no logging line printed - the supplier was never invoked)");

        System.out.println();
        System.out.println("=== Step 5: orElseThrow() with a custom exception ===");
        try {
            findByDeviceIdOptional(readings, "sensor-Z9")
                    .orElseThrow(() -> new NoSuchElementException("No reading found for sensor-Z9"));
        } catch (NoSuchElementException e) {
            System.out.println("Caught expected exception: " + e.getMessage());
        }

        System.out.println();
        System.out.println("=== Step 6: ifPresent()/ifPresentOrElse() - the exact pattern from AnomalyProcessor.process() ===");
        findByDeviceIdOptional(readings, "sensor-C3")
                .filter(r -> Math.abs(r.zScore()) >= 5.0)
                .ifPresentOrElse(
                        r -> System.out.println("  ANOMALY: forwarding " + r.deviceId() + " (z=" + r.zScore() + ") downstream"),
                        () -> System.out.println("  no anomaly to forward for that lookup")
                );

        System.out.println();
        System.out.println("=== Step 7: Optional::stream + flatMap - collecting only the present values from many Optionals ===");
        List<Optional<Reading>> lookupResults = List.of(
                findByDeviceIdOptional(readings, "sensor-A1"),
                findByDeviceIdOptional(readings, "sensor-Z9"),
                findByDeviceIdOptional(readings, "sensor-B2"),
                findByDeviceIdOptional(readings, "sensor-Y8")
        );
        List<Reading> presentOnly = lookupResults.stream()
                .flatMap(Optional::stream)
                .collect(Collectors.toList());
        System.out.println("4 lookups attempted, 2 succeeded. Present results only: " + presentOnly);
    }
}