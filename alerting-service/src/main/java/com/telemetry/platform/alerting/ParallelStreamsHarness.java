package com.telemetry.platform.alerting;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParallelStreamsHarness {

    static double sumSquareRoots(double[] data) {
        return java.util.Arrays.stream(data).map(Math::sqrt).sum();
    }

    static double sumSquareRootsParallel(double[] data) {
        return java.util.Arrays.stream(data).parallel().map(Math::sqrt).sum();
    }

    public static void main(String[] args) {

        System.out.println("=== Step 1: parallel streams run on the shared ForkJoinPool.commonPool() ===");
        System.out.println("Available processors on this machine: " + Runtime.getRuntime().availableProcessors());
        Set<String> threadNames = ConcurrentHashMap.newKeySet();
        IntStream.range(0, 8).parallel().forEach(i -> threadNames.add(Thread.currentThread().getName()));
        System.out.println("Distinct thread(s) that participated: " + threadNames.size()
                + " - the calling thread plus zero or more ForkJoinPool.commonPool-worker-N threads (exact count depends on your CPU's core count).");

        System.out.println();
        System.out.println("=== Step 2: parallel speedup on real CPU-bound work (with JIT warm-up) ===");
        int n = 20_000_000;
        double[] readings = new double[n];
        for (int i = 0; i < n; i++) {
            readings[i] = ThreadLocalRandom.current().nextDouble(60, 90);
        }
        for (int i = 0; i < 3; i++) {
            sumSquareRoots(readings);
            sumSquareRootsParallel(readings);
        }
        long t0 = System.nanoTime();
        double sequentialSum = sumSquareRoots(readings);
        long t1 = System.nanoTime();
        double parallelSum = sumSquareRootsParallel(readings);
        long t2 = System.nanoTime();
        System.out.println("Sequential time (ms): " + (t1 - t0) / 1_000_000.0);
        System.out.println("Parallel   time (ms): " + (t2 - t1) / 1_000_000.0);
        System.out.println("Results match (within floating-point tolerance): " + (Math.abs(sequentialSum - parallelSum) < 0.001));

        System.out.println();
        System.out.println("=== Step 3: the correctness trap - unsynchronized shared mutable state ===");
        System.out.println("Running the SAME unsafe operation 5 times. Watch the sizes - they will NOT reliably be 200000:");
        for (int trial = 1; trial <= 5; trial++) {
            List<Integer> unsafeList = new ArrayList<>();
            try {
                IntStream.range(0, 200_000).parallel().forEach(unsafeList::add);
                System.out.println("  trial " + trial + ": size=" + unsafeList.size()
                        + (unsafeList.size() == 200_000 ? " (looks correct by luck)" : " (CORRUPTED - lost updates)"));
            } catch (Exception e) {
                System.out.println("  trial " + trial + ": threw " + e.getClass().getSimpleName()
                        + " - ArrayList is not thread-safe under concurrent structural modification");
            }
        }

        System.out.println();
        System.out.println("=== Step 4: the fix - let collect() do the thread-safe combining ===");
        for (int trial = 1; trial <= 3; trial++) {
            List<Integer> safeList = IntStream.range(0, 200_000).parallel().boxed().collect(Collectors.toList());
            System.out.println("  trial " + trial + ": size=" + safeList.size()
                    + " (Collectors.toList() merges each thread's own partial list safely, every time)");
        }

        System.out.println();
        System.out.println("=== Step 5: forEach() has no ordering guarantee under parallel; forEachOrdered() restores it ===");
        System.out.print("parallel forEach (order not guaranteed):  ");
        IntStream.range(0, 10).parallel().forEach(i -> System.out.print(i + " "));
        System.out.println();
        System.out.print("parallel forEachOrdered (order guaranteed): ");
        IntStream.range(0, 10).parallel().forEachOrdered(i -> System.out.print(i + " "));
        System.out.println();

        System.out.println();
        System.out.println("=== Step 6: where this project would - and would NOT - use parallel streams ===");
        System.out.println("AlertConsumer's poll loop can NEVER be parallelized: it has ordered, stateful, shared-mutable-state");
        System.out.println("side effects per record (retry-with-backoff reconnection, offset commit ordering, deadLetter() fallthrough)");
        System.out.println("- exactly the kind of unsynchronized shared state Step 3 just showed corrupting silently.");
        System.out.println("A legitimate future candidate: a batch job re-scoring thousands of already-persisted, independent");
        System.out.println("historical alerts pulled from anomaly_alerts, with no shared state between records and enough volume");
        System.out.println("to amortize the ForkJoinPool coordination cost - the same shape as Step 2's CPU-bound benchmark.");
    }
}