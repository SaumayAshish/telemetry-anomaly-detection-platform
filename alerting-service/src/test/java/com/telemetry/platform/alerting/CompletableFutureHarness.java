package com.telemetry.platform.alerting;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CompletableFutureHarness {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        System.out.println("=== Section 1: supplyAsync + thenApply + thenAccept - a non-blocking pipeline ===");
        CompletableFuture<Void> pipeline = CompletableFuture
                .supplyAsync(() -> {
                    sleepQuietly(100);
                    return 21;
                })
                .thenApply(value -> value * 2)
                .thenAccept(result -> System.out.println("  pipeline result: " + result
                        + " (computed on " + Thread.currentThread().getName() + ", a callback, not the main thread)"));
        System.out.println("  main thread did NOT block here - this line prints before the pipeline necessarily finishes");
        pipeline.join();
        System.out.println("  (join() at the very end is only here so this demo program doesn't exit before the async");
        System.out.println("  work completes - it is NOT how you'd normally consume a CompletableFuture mid-pipeline)");

        System.out.println();
        System.out.println("=== Section 2: thenCompose vs thenApply - flattening a DEPENDENT async call ===");
        CompletableFuture<CompletableFuture<String>> nested = lookupSensorId("S-1001")
                .thenApply(sensorId -> lookupOwnerFor(sensorId));
        String nestedResult = nested.get().get();
        System.out.println("  thenApply produces a NESTED CompletableFuture<CompletableFuture<String>> - needed .get().get()");
        System.out.println("  to unwrap: " + nestedResult);

        CompletableFuture<String> flattened = lookupSensorId("S-1001")
                .thenCompose(sensorId -> lookupOwnerFor(sensorId));
        String flattenedResult = flattened.get();
        System.out.println("  thenCompose flattens automatically into CompletableFuture<String> - one .get(): " + flattenedResult);
        System.out.println("  (use thenApply when the next step is a plain, already-computed value; use thenCompose when");
        System.out.println("  the next step itself returns a CompletableFuture - e.g. it triggers another async call)");

        System.out.println();
        System.out.println("=== Section 3: thenCombine - two INDEPENDENT async calls running concurrently ===");
        long combineStart = System.nanoTime();
        CompletableFuture<Integer> readingCount = CompletableFuture.supplyAsync(() -> {
            sleepQuietly(200);
            return 1_500_000;
        });
        CompletableFuture<Integer> anomalyCount = CompletableFuture.supplyAsync(() -> {
            sleepQuietly(200);
            return 47;
        });
        CompletableFuture<String> summary = readingCount.thenCombine(anomalyCount,
                (readings, anomalies) -> readings + " readings, " + anomalies + " anomalies");
        String summaryResult = summary.get();
        long combineElapsedMs = (System.nanoTime() - combineStart) / 1_000_000;
        System.out.println("  Combined summary: " + summaryResult);
        System.out.println("  Elapsed: " + combineElapsedMs + "ms (each individual call sleeps 200ms - if they ran");
        System.out.println("  sequentially this would be ~400ms; running concurrently, it should be closer to ~200ms)");

        System.out.println();
        System.out.println("=== Section 4: exceptionally() and handle() - recovering from failure in a pipeline ===");
        CompletableFuture<Integer> failing = CompletableFuture
                .<Integer>supplyAsync(() -> {
                    throw new IllegalStateException("simulated lookup failure");
                })
                .exceptionally(ex -> {
                    System.out.println("  exceptionally() caught: " + ex.getCause().getClass().getSimpleName()
                            + ": " + ex.getCause().getMessage());
                    return -1;
                });
        System.out.println("  Recovered value from failing pipeline: " + failing.get());

        CompletableFuture<String> handledSuccess = CompletableFuture.supplyAsync(() -> 42)
                .handle((value, ex) -> ex == null ? "succeeded with " + value : "failed: " + ex.getMessage());
        CompletableFuture<String> handledFailure = CompletableFuture
                .<Integer>supplyAsync(() -> {
                    throw new RuntimeException("simulated handle() failure");
                })
                .handle((value, ex) -> ex == null ? "succeeded with " + value : "failed: " + ex.getCause().getMessage());
        System.out.println("  handle() on success path: " + handledSuccess.get());
        System.out.println("  handle() on failure path: " + handledFailure.get());
        System.out.println("  (handle() always runs, seeing either a result or an exception - exceptionally() only");
        System.out.println("  runs on failure, and unlike handle(), it cannot see or act on a successful result)");

        System.out.println();
        System.out.println("=== Section 5: allOf / anyOf - fan-out to multiple futures ===");
        CompletableFuture<Void> slowTask = CompletableFuture.runAsync(() -> sleepQuietly(150));
        CompletableFuture<Void> fastTask = CompletableFuture.runAsync(() -> sleepQuietly(50));
        CompletableFuture<Void> mediumTask = CompletableFuture.runAsync(() -> sleepQuietly(100));

        long allOfStart = System.nanoTime();
        CompletableFuture.allOf(slowTask, fastTask, mediumTask).join();
        long allOfElapsedMs = (System.nanoTime() - allOfStart) / 1_000_000;
        System.out.println("  allOf() completed after " + allOfElapsedMs + "ms (must wait for the SLOWEST of the three,");
        System.out.println("  ~150ms, not the sum of all three - the tasks ran concurrently, not sequentially)");

        CompletableFuture<String> winnerA = CompletableFuture.supplyAsync(() -> {
            sleepQuietly(300);
            return "slow-source";
        });
        CompletableFuture<String> winnerB = CompletableFuture.supplyAsync(() -> {
            sleepQuietly(20);
            return "fast-source";
        });
        Object firstToFinish = CompletableFuture.anyOf(winnerA, winnerB).get();
        System.out.println("  anyOf() returned as soon as the FIRST source finished: " + firstToFinish
                + " (fast-source, given the 20ms vs 300ms gap - this should hold on any machine)");

        System.out.println();
        System.out.println("=== Section 6: which executor runs your callback, and why it matters ===");
        String defaultExecutorThread = CompletableFuture.supplyAsync(
                () -> Thread.currentThread().getName()).get();
        System.out.println("  Default executor (no argument): ran on " + defaultExecutorThread);
        System.out.println("  (supplyAsync()/thenApply()/etc. with no Executor argument default to");
        System.out.println("  ForkJoinPool.commonPool() ONLY when commonPool supports parallelism >= 2. If commonPool's");
        System.out.println("  parallelism is 1 (a low-core-count machine), the JDK documents a fallback: a brand-new");
        System.out.println("  Thread is created per async call instead. Watch the thread name above - on a machine with");
        System.out.println("  several cores you should see 'ForkJoinPool.commonPool-worker-N'; on a very low-core one,");
        System.out.println("  a plain 'Thread-N' instead.)");

        ExecutorService ioPool = Executors.newFixedThreadPool(2, r -> new Thread(r, "io-worker"));
        String customExecutorThread = CompletableFuture.supplyAsync(
                () -> Thread.currentThread().getName(), ioPool).get();
        System.out.println("  Explicit Executor argument: ran on " + customExecutorThread);
        System.out.println("  (a BLOCKING I/O call - a JDBC query, an HTTP request - run with no executor argument");
        System.out.println("  ties up a commonPool worker thread for the whole blocking duration. Since commonPool is");
        System.out.println("  a JVM-wide SHARED resource (used by parallel streams, Fork/Join, and any other library's");
        System.out.println("  async work), blocking it with I/O can starve unrelated CPU-bound work elsewhere in the same");
        System.out.println("  process. The fix is exactly what this section shows: pass a dedicated Executor - built with");
        System.out.println("  Step 7's Executors.newFixedThreadPool() - sized and reserved for that blocking work.");
        ioPool.shutdown();
        ioPool.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println();
        System.out.println("=== Section 7: why AlertConsumer's poll loop stays synchronous, not CompletableFuture-based ===");
        System.out.println("CompletableFuture is for composing independent or dependent async work without blocking");
        System.out.println("threads unnecessarily. AlertConsumer's poll loop (Phase 6) cannot use it for its core logic:");
        System.out.println("persisting an alert and committing its Kafka offset must happen in that EXACT order for every");
        System.out.println("single record, and a CompletableFuture pipeline's callbacks can complete in whatever order the");
        System.out.println("underlying async work finishes - reordering here would silently reintroduce the exact");
        System.out.println("lost-alert-vs-duplicate tradeoff Phase 6 was built around. The tool for THIS project's actual");
        System.out.println("async opportunities would be something like enriching an alert with two independent, unrelated");
        System.out.println("lookups (e.g. device metadata plus an audit-log write) via thenCombine before persisting -");
        System.out.println("never the persist-then-commit sequence itself.");
    }

    private static CompletableFuture<String> lookupSensorId(String sensorId) {
        return CompletableFuture.supplyAsync(() -> {
            sleepQuietly(20);
            return sensorId;
        });
    }

    private static CompletableFuture<String> lookupOwnerFor(String sensorId) {
        return CompletableFuture.supplyAsync(() -> {
            sleepQuietly(20);
            return "owner-of-" + sensorId;
        });
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}