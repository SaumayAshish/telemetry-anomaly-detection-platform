package com.telemetry.platform.alerting;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PerformanceHarness {

    static double sumSquareRootsLoop(double[] data) {
        double sum = 0.0;
        for (int i = 0; i < data.length; i++) {
            sum += Math.sqrt(data[i]);
        }
        return sum;
    }

    static double sumSquareRootsPrimitiveStream(double[] data) {
        return java.util.Arrays.stream(data).map(Math::sqrt).sum();
    }

    static double sumSquareRootsBoxedStream(Double[] data) {
        return java.util.Arrays.stream(data)
                .map(Math::sqrt)
                .mapToDouble(Double::doubleValue)
                .sum();
    }

    public static void main(String[] args) {

        System.out.println("=== Section 1: plain loop vs primitive stream vs boxed stream, on real CPU-bound work ===");
        int n = 20_000_000;
        double[] primitiveData = new double[n];
        Double[] boxedData = new Double[n];
        for (int i = 0; i < n; i++) {
            double value = ThreadLocalRandom.current().nextDouble(60, 90);
            primitiveData[i] = value;
            boxedData[i] = value;
        }

        for (int i = 0; i < 3; i++) {
            sumSquareRootsLoop(primitiveData);
            sumSquareRootsPrimitiveStream(primitiveData);
            sumSquareRootsBoxedStream(boxedData);
        }

        long t0 = System.nanoTime();
        double loopSum = sumSquareRootsLoop(primitiveData);
        long t1 = System.nanoTime();
        double primitiveStreamSum = sumSquareRootsPrimitiveStream(primitiveData);
        long t2 = System.nanoTime();
        double boxedStreamSum = sumSquareRootsBoxedStream(boxedData);
        long t3 = System.nanoTime();

        System.out.println("Plain for-loop         (ms): " + (t1 - t0) / 1_000_000.0);
        System.out.println("Primitive DoubleStream (ms): " + (t2 - t1) / 1_000_000.0);
        System.out.println("Boxed Stream<Double>   (ms): " + (t3 - t2) / 1_000_000.0);
        System.out.println("All three results match (within floating-point tolerance): "
                + (Math.abs(loopSum - primitiveStreamSum) < 0.001 && Math.abs(loopSum - boxedStreamSum) < 0.001));

        System.out.println();
        System.out.println("=== Section 2: non-capturing lambdas are cached; capturing lambdas allocate a new instance each time ===");
        List<DoubleUnaryOperator> nonCapturingInstances = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            DoubleUnaryOperator nonCapturing = z -> z * 2.0;
            nonCapturingInstances.add(nonCapturing);
        }
        boolean allSameInstance = true;
        for (int i = 1; i < nonCapturingInstances.size(); i++) {
            if (nonCapturingInstances.get(0) != nonCapturingInstances.get(i)) {
                allSameInstance = false;
            }
        }
        System.out.println("Non-capturing lambda, 5 iterations, same instance every time: " + allSameInstance);

        List<DoubleUnaryOperator> capturingInstances = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            double multiplier = i + 1.0;
            DoubleUnaryOperator capturing = z -> z * multiplier;
            capturingInstances.add(capturing);
        }
        boolean allSameInstanceCapturing = true;
        for (int i = 1; i < capturingInstances.size(); i++) {
            if (capturingInstances.get(0) != capturingInstances.get(i)) {
                allSameInstanceCapturing = false;
            }
        }
        System.out.println("Capturing lambda, 5 iterations, same instance every time: " + allSameInstanceCapturing);

        System.out.println();
        System.out.println("=== Section 3: short-circuiting - anyMatch stops at the first match instead of scanning everything ===");
        List<Integer> largeList = IntStream.range(0, 10_000_000).boxed().collect(Collectors.toList());
        int[] elementsChecked = {0};
        long m0 = System.nanoTime();
        boolean found = largeList.stream()
                .peek(x -> elementsChecked[0]++)
                .anyMatch(x -> x == 5);
        long m1 = System.nanoTime();
        System.out.println("anyMatch(x == 5) over a 10,000,000-element list found=" + found
                + ", elements actually visited=" + elementsChecked[0] + ", time(ms)=" + (m1 - m0) / 1_000_000.0);

        System.out.println();
        System.out.println("=== Section 4: where this project applies these lessons ===");
        System.out.println("AlertConsumer's hot per-record path (Phase 6) stays a plain loop deliberately: it is called once per");
        System.out.println("Kafka record, at Kafka's own throughput, and every allocation there is pure overhead with zero");
        System.out.println("readability payoff, since there is no meaningful transformation pipeline to express.");
        System.out.println("AnomalyDecisionEngine and the Collectors-based harnesses in this phase stay Streams-based on purpose:");
        System.out.println("they run far less often, and the clarity of a declarative pipeline is worth more there than the");
        System.out.println("small, one-time cost of building it.");
    }
}