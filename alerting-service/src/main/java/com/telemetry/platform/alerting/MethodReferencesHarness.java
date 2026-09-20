package com.telemetry.platform.alerting;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Phase 7, Step 3 — Method References.
 *
 * Plain main()-based harness (same convention as every prior harness in this
 * project). Demonstrates all four kinds of method references, each paired
 * with the equivalent lambda it replaces, plus a real example from this
 * project's own code of a lambda that CANNOT become a method reference.
 */
public class MethodReferencesHarness {

    public static void main(String[] args) {

        List<Double> sampleZScores = List.of(
                1.2, -0.8, 6.9, 19.3, -3.2, 21.5, 4.1, 25.48, 0.3, -12.2
        );

        System.out.println("=== Kind 1: static method reference (ClassName::staticMethod) ===");
        UnaryOperator<Double> absViaLambda = z -> Math.abs(z);
        UnaryOperator<Double> absViaMethodRef = Math::abs;
        System.out.println("Lambda result:     " + absViaLambda.apply(-12.2));
        System.out.println("Method ref result: " + absViaMethodRef.apply(-12.2));
        System.out.println("Lambda impl class:     " + absViaLambda.getClass().getName());
        System.out.println("Method ref impl class: " + absViaMethodRef.getClass().getName());

        System.out.println();
        System.out.println("=== Kind 2: bound instance method reference (instance::method) ===");
        // Same filter+map pipeline from Step 2 — printed there with a for-loop.
        List<String> anomalousFormatted = sampleZScores.stream()
                .filter(z -> Math.abs(z) >= 5.0)
                .map(z -> String.format("z=%.2f", z))
                .toList();
        System.out.println("Step 2 printed this list with a for-loop. Here it is with System.out::println:");
        anomalousFormatted.forEach(System.out::println);

        System.out.println();
        System.out.println("=== Kind 3: unbound instance method reference (ClassName::method) ===");
        Function<String, Integer> lengthViaLambda = s -> s.length();
        Function<String, Integer> lengthViaMethodRef = String::length;
        System.out.println("Lambda result:     " + lengthViaLambda.apply("CRITICAL"));
        System.out.println("Method ref result: " + lengthViaMethodRef.apply("CRITICAL"));
        Function<String, String> upperViaMethodRef = String::toUpperCase;
        System.out.println("String::toUpperCase on \"critical\": " + upperViaMethodRef.apply("critical"));

        System.out.println();
        System.out.println("=== Kind 4: constructor reference (ClassName::new) ===");
        Supplier<List<Double>> listFactoryViaLambda = () -> new ArrayList<>();
        Supplier<List<Double>> listFactoryViaMethodRef = ArrayList::new;
        List<Double> fromLambda = listFactoryViaLambda.get();
        List<Double> fromMethodRef = listFactoryViaMethodRef.get();
        fromMethodRef.addAll(sampleZScores);
        System.out.println("New list from lambda factory, size:        " + fromLambda.size());
        System.out.println("New list from constructor reference, size: " + fromMethodRef.size());

        System.out.println();
        System.out.println("=== Where a method reference is NOT possible ===");
        System.out.println("AlertConsumer's Deserializer<AnomalyEvent> lambda wraps objectMapper.readValue(...)");
        System.out.println("inside a try/catch that rethrows on JsonProcessingException. A method reference can");
        System.out.println("only replace a lambda whose ENTIRE body is one method call with matching arguments");
        System.out.println("and return type - no extra statements, no try/catch. That's exactly why it stays a lambda.");
    }
}