package com.telemetry.platform.alerting;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Phase 7, Step 2 — Streams.
 *
 * Plain main()-based harness (same convention as LambdaBasicsHarness and every
 * prior verification harness in this project). Every lambda here is written
 * out in full deliberately — Step 3 (Method References) will show the
 * shorthand for several of these exact patterns (e.g. System.out::println,
 * Math::abs).
 */
public class StreamsBasicsHarness {

    public static void main(String[] args) {

        // Sample z-scores, shaped like real values from this project's own
        // live-verification history (Phase 4 through Phase 6).
        List<Double> sampleZScores = List.of(
                1.2, -0.8, 6.9, 19.3, -3.2, 21.5, 4.1, 25.48, 0.3, -12.2
        );

        System.out.println("=== Step 1: Laziness — nothing runs until a terminal operation ===");
        Stream<Double> lazyPipeline = sampleZScores.stream()
                .peek(z -> System.out.println("  (peek saw: " + z + ")"))
                .filter(z -> Math.abs(z) >= 5.0);
        System.out.println("Pipeline built above this line. Notice: no peek output yet — nothing has run.");
        System.out.println("Now calling a terminal operation, count():");
        long anomalousCount = lazyPipeline.count();
        System.out.println("Anomalous count (|z| >= 5.0): " + anomalousCount);

        System.out.println();
        System.out.println("=== Step 2: filter + sorted + map + collect, chained ===");
        List<String> anomalousFormatted = sampleZScores.stream()
                .filter(z -> Math.abs(z) >= 5.0)
                .sorted((a, b) -> Double.compare(Math.abs(b), Math.abs(a))) // descending by magnitude
                .map(z -> String.format("z=%.2f", z))
                .collect(Collectors.toList());
        for (String line : anomalousFormatted) {
            System.out.println(line);
        }

        System.out.println();
        System.out.println("=== Step 3: a numeric reduction — average magnitude ===");
        double averageMagnitude = sampleZScores.stream()
                .filter(z -> Math.abs(z) >= 5.0)
                .mapToDouble(z -> Math.abs(z))
                .average()
                .orElse(0.0);
        System.out.println("Average |z| among anomalous readings: " + averageMagnitude);

        System.out.println();
        System.out.println("=== Step 4: a stream can only be consumed ONCE ===");
        Stream<Double> singleUseStream = sampleZScores.stream().filter(z -> z > 0);
        long positiveCount = singleUseStream.count();
        System.out.println("Positive count: " + positiveCount);
        try {
            long positiveCountAgain = singleUseStream.count(); // reusing the SAME stream object
            System.out.println("This line never runs: " + positiveCountAgain);
        } catch (IllegalStateException e) {
            System.out.println("Caught expected exception: " + e.getMessage());
        }
    }
}