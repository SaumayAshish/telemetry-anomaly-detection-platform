package com.telemetry.platform.alerting;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ExecutorsHarness {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Section 1: a fixed thread pool reuses a small number of threads across many tasks ===");
        ExecutorService fixedPool = Executors.newFixedThreadPool(2);
        Set<String> threadNamesUsed = ConcurrentHashMap.newKeySet();
        int taskCount = 8;

        List<Future<?>> submissions = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            final int taskId = i;
            submissions.add(fixedPool.submit(() -> {
                threadNamesUsed.add(Thread.currentThread().getName());
                sleepQuietly(20);
                System.out.println("  task " + taskId + " ran on " + Thread.currentThread().getName());
            }));
        }
        for (Future<?> f : submissions) {
            waitQuietly(f);
        }
        System.out.println("Distinct thread names used across " + taskCount + " tasks: " + threadNamesUsed.size()
                + " (must be at most 2, the pool size - not " + taskCount + " like Steps 1-6's new Thread(...) per task)");

        System.out.println();
        System.out.println("=== Section 2: Callable<T> and Future<T> - getting a result back from a background task ===");
        Callable<Integer> computeSquare = () -> {
            sleepQuietly(50);
            return 7 * 7;
        };
        Future<Integer> squareFuture = fixedPool.submit(computeSquare);
        System.out.println("  Task submitted, main thread keeps going immediately (not blocked yet)...");
        try {
            Integer result = squareFuture.get();
            System.out.println("  future.get() returned: " + result + " (this call DID block until the task finished)");
        } catch (ExecutionException e) {
            System.out.println("  unexpected exception: " + e);
        }

        System.out.println();
        System.out.println("=== Section 3: exceptions thrown inside a task surface through Future.get() ===");
        Callable<Integer> failingTask = () -> {
            sleepQuietly(20);
            throw new IllegalStateException("simulated failure inside the task");
        };
        Future<Integer> failingFuture = fixedPool.submit(failingTask);
        try {
            failingFuture.get();
            System.out.println("  unexpectedly succeeded");
        } catch (ExecutionException e) {
            System.out.println("  future.get() threw ExecutionException, as expected");
            System.out.println("  actual cause: " + e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
        }

        System.out.println();
        System.out.println("=== Section 4: proper shutdown - shutdown() + awaitTermination(), not just abandoning the pool ===");
        System.out.println("  Before calling shutdown(): isShutdown=" + fixedPool.isShutdown());
        fixedPool.shutdown();
        System.out.println("  After calling shutdown(): isShutdown=" + fixedPool.isShutdown()
                + " (no new tasks accepted, but already-submitted tasks still run to completion)");
        boolean finishedInTime = fixedPool.awaitTermination(5, TimeUnit.SECONDS);
        System.out.println("  awaitTermination(5s) returned: " + finishedInTime
                + " (true means every task finished before the timeout - the pool's threads have now actually exited)");

        System.out.println();
        System.out.println("=== Section 5: why real production code rarely uses Executors.newFixedThreadPool() directly ===");
        System.out.println("newFixedThreadPool() backs its threads with an UNBOUNDED task queue - if tasks arrive faster");
        System.out.println("than they can be processed, the queue grows without limit and can eventually exhaust memory,");
        System.out.println("failing slowly and unpredictably instead of failing fast. newCachedThreadPool() has the");
        System.out.println("opposite problem - an unbounded THREAD count under sustained load. Production code typically");
        System.out.println("constructs a ThreadPoolExecutor directly, with an explicit bounded queue and a RejectedExecutionHandler");
        System.out.println("policy (e.g. CallerRunsPolicy) that defines exactly what happens when the system is overloaded,");
        System.out.println("rather than leaving that behavior to chance.");
    }

    private static void waitQuietly(Future<?> future) {
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("  task failed: " + e);
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}