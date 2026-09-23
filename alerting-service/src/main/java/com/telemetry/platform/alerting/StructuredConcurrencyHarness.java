package com.telemetry.platform.alerting;

import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.concurrent.atomic.AtomicBoolean;

public class StructuredConcurrencyHarness {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Section 1: fork/join a fixed-size group of subtasks - the basic shape ===");
        try (var scope = StructuredTaskScope.open()) {
            Subtask<String> sensorLookup = scope.fork(() -> {
                sleepQuietly(50);
                return "sensor-metadata-for-S-1001";
            });
            Subtask<Integer> auditWrite = scope.fork(() -> {
                sleepQuietly(30);
                return 1;
            });
            scope.join();
            System.out.println("Both subtasks succeeded: " + sensorLookup.get() + " | auditRowsWritten=" + auditWrite.get());
        }
        System.out.println("(StructuredTaskScope.open() with no Joiner argument uses the default joiner - it waits for");
        System.out.println("every forked subtask to finish, throws if any of them failed, and lets you call .get() on each");
        System.out.println("Subtask handle afterward. Notice this reads like ordinary synchronous code - no callbacks, no");
        System.out.println("thenCombine - even though sensorLookup and auditWrite actually ran concurrently on their own");
        System.out.println("virtual threads underneath.)");

        System.out.println();
        System.out.println("=== Section 2: when one subtask fails, its sibling is automatically CANCELLED, not left running ===");
        AtomicBoolean siblingWasInterrupted = new AtomicBoolean(false);
        try (var scope = StructuredTaskScope.open()) {
            scope.fork(() -> {
                sleepQuietly(20);
                throw new IllegalStateException("simulated device-metadata lookup failure");
            });
            scope.fork(() -> {
                try {
                    sleepQuietly(5000);
                    System.out.println("  (slow sibling completed normally - this should NOT print)");
                } catch (RuntimeException e) {
                    // sleepQuietly swallows InterruptedException as interrupt-and-return;
                    // detect cancellation via the thread's interrupted status instead.
                }
                if (Thread.currentThread().isInterrupted()) {
                    siblingWasInterrupted.set(true);
                }
                return null;
            });
            try {
                scope.join();
                System.out.println("  (join() did not throw - this should NOT print)");
            } catch (StructuredTaskScope.FailedException e) {
                System.out.println("  join() threw FailedException, caused by: " + e.getCause().getClass().getSimpleName()
                        + ": " + e.getCause().getMessage());
            }
        }
        System.out.println("Sibling subtask was interrupted/cancelled: " + siblingWasInterrupted.get());
        System.out.println("(The default joiner fails fast: the instant one subtask throws, join() propagates that failure");
        System.out.println("AND the scope interrupts every other still-running subtask, rather than letting a 5-second sleep");
        System.out.println("run to completion for no reason. Contrast this with Phase 8 Step 9's CompletableFuture.allOf() -");
        System.out.println("if one stage in an allOf() group fails, the OTHER stages are never cancelled; they keep running");
        System.out.println("to completion in the background even though nothing will ever use their result.)");

        System.out.println();
        System.out.println("=== Section 3: Joiner.anySuccessfulOrThrow() - a race where the losers are cancelled ===");
        AtomicBoolean loserWasInterrupted = new AtomicBoolean(false);
        try (var scope = StructuredTaskScope.open(Joiner.<String>anySuccessfulOrThrow())) {
            scope.fork(() -> {
                sleepQuietly(20);
                return "fast-source";
            });
            scope.fork(() -> {
                try {
                    sleepQuietly(3000);
                } catch (RuntimeException e) {
                    // see note above
                }
                if (Thread.currentThread().isInterrupted()) {
                    loserWasInterrupted.set(true);
                }
                return "slow-source";
            });
            String winner = scope.join();
            System.out.println("Winner: " + winner);
        }
        System.out.println("Losing subtask was interrupted/cancelled: " + loserWasInterrupted.get());
        System.out.println("(Compare this to Phase 8 Step 9's CompletableFuture.anyOf() - it also returns as soon as the");
        System.out.println("first future completes, but the LOSING future is never cancelled there either; it keeps running");
        System.out.println("to completion, silently burning a thread/resource for a result nothing will ever read. This is a");
        System.out.println("real, named advantage of structured concurrency over ad-hoc CompletableFuture composition: no");
        System.out.println("orphaned work survives the scope.)");

        System.out.println();
        System.out.println("=== Section 4: the actual STRUCTURE - you are not ALLOWED to forget to join what you fork ===");
        AtomicBoolean unjoinedSubtaskWasInterrupted = new AtomicBoolean(false);
        long structureStart = System.nanoTime();
        try {
            try (var scope = StructuredTaskScope.open()) {
                scope.fork(() -> {
                    try {
                        sleepQuietly(2000);
                    } catch (RuntimeException e) {
                        // see note above
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        unjoinedSubtaskWasInterrupted.set(true);
                    }
                    return null;
                });
                System.out.println("  (deliberately NOT calling scope.join() here - exiting the try-with-resources block directly)");
            }
            System.out.println("  (close() did not throw - this should NOT print)");
        } catch (IllegalStateException e) {
            long structureElapsedMs = (System.nanoTime() - structureStart) / 1_000_000;
            System.out.println("close() threw after " + structureElapsedMs + "ms: " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        }
        System.out.println("Forked subtask was interrupted/cancelled before close() threw: " + unjoinedSubtaskWasInterrupted.get());
        System.out.println("(This is stricter than 'a leaked thread gets cancelled' - the API refuses to let you forget to");
        System.out.println("join at all. Skipping join() is treated as a programming error and close() throws");
        System.out.println("IllegalStateException(\"Owner did not join after forking\") immediately, rather than silently");
        System.out.println("cancelling and moving on. Note it still cancelled the still-running subtask FIRST (in well under");
        System.out.println("the full 2-second sleep) before raising that error - the cancellation guarantee holds even in the");
        System.out.println("failure path. This is what makes it 'structured': not just that a subtask's lifetime can never");
        System.out.println("exceed the lexical block that forked it, but that the compiler-enforced try-with-resources shape");
        System.out.println("makes forgetting to reconcile with your forked work a loud, immediate exception - unlike Step 10's");
        System.out.println("newVirtualThreadPerTaskExecutor() or Step 7's ExecutorService, where nothing stops a submitted");
        System.out.println("task from running long after the method that submitted it has already returned, and nothing");
        System.out.println("warns you if you never even tried to wait for it.)");

        System.out.println();
        System.out.println("=== Section 5: where this project would actually reach for structured concurrency ===");
        System.out.println("Phase 8 Step 9 named a real opportunity for this project: enriching an alert with two");
        System.out.println("independent, unrelated lookups (device metadata, an audit-log write) before persisting it in");
        System.out.println("AlertConsumer, and suggested thenCombine() for it. Section 1 above is exactly that enrichment,");
        System.out.println("written with structured concurrency instead: it reads as plain sequential code (fork, fork,");
        System.out.println("join, use the results), any exception surfaces as a normal thrown exception at the join() call");
        System.out.println("site (a real stack trace, not a wrapped callback-chain exception), and if either lookup fails");
        System.out.println("the other is automatically cancelled - all for free, with no separate combinator method to");
        System.out.println("learn. The rule of thumb: prefer structured concurrency when a fixed, known set of child tasks");
        System.out.println("all belong to ONE logical unit of work whose lifetime is exactly one calling method (this");
        System.out.println("enrichment case) - prefer CompletableFuture when work needs to be composed, handed off, or kept");
        System.out.println("alive independently of any single call site. IMPORTANT CAVEAT: as of this project's JDK, this");
        System.out.println("API is still a JDK PREVIEW feature (JEP 533, seventh preview) - it compiles and runs only with");
        System.out.println("--enable-preview, and its exact method names have changed across previous preview rounds (an");
        System.out.println("earlier JDK 21 preview used a completely different subclassing-based API). Real production code");
        System.out.println("should not depend on it yet without an explicit, team-level decision to accept that churn risk.");
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted", e);
        }
    }
}