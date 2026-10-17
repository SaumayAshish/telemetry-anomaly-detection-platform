package com.telemetry.platform.alerting;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualThreadsHarness {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Section 1: creation cost - platform threads vs virtual threads at scale ===");
        int platformCount = 10_000;
        int virtualCount = 100_000;

        long platformStart = System.nanoTime();
        List<Thread> platformThreads = new ArrayList<>(platformCount);
        for (int i = 0; i < platformCount; i++) {
            Thread t = new Thread(() -> sleepQuietly(100));
            t.start();
            platformThreads.add(t);
        }
        for (Thread t : platformThreads) {
            t.join();
        }
        long platformElapsedMs = (System.nanoTime() - platformStart) / 1_000_000;
        System.out.println(platformCount + " platform threads (each sleeping 100ms): " + platformElapsedMs + "ms total");

        long virtualStart = System.nanoTime();
        List<Thread> virtualThreads = new ArrayList<>(virtualCount);
        for (int i = 0; i < virtualCount; i++) {
            Thread t = Thread.ofVirtual().start(() -> sleepQuietly(100));
            virtualThreads.add(t);
        }
        for (Thread t : virtualThreads) {
            t.join();
        }
        long virtualElapsedMs = (System.nanoTime() - virtualStart) / 1_000_000;
        System.out.println(virtualCount + " virtual threads (each sleeping 100ms): " + virtualElapsedMs + "ms total");
        System.out.println("(10x more virtual threads than platform threads, yet virtual threads should complete in roughly");
        System.out.println("the same ballpark as the 100ms sleep itself - platform thread creation is heavy enough, at this");
        System.out.println("count, that its total time is dominated by OS thread-creation/scheduling overhead, not the sleep.)");

        System.out.println();
        System.out.println("=== Section 2: creating a virtual thread - three ways ===");
        Thread namedStarted = Thread.ofVirtual().name("vt-named").start(() -> {
            System.out.println("  running on: " + Thread.currentThread()
                    + " | isVirtual()=" + Thread.currentThread().isVirtual());
        });
        namedStarted.join();

        Thread unstarted = Thread.ofVirtual().name("vt-unstarted").unstarted(() ->
                System.out.println("  unstarted-then-started virtual thread ran"));
        unstarted.start();
        unstarted.join();

        try (ExecutorService vExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            vExecutor.submit(() -> System.out.println("  submitted via newVirtualThreadPerTaskExecutor(), isVirtual()="
                    + Thread.currentThread().isVirtual()));
            vExecutor.shutdown();
            vExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
        System.out.println("  main thread isVirtual()=" + Thread.currentThread().isVirtual());
        System.out.println("  (three equivalent ways to get a virtual thread: Thread.ofVirtual().start(...), .unstarted()");
        System.out.println("  then .start() separately, and the Executors factory - the last one matters most in practice,");
        System.out.println("  since it slots into the exact same ExecutorService shape used since Step 7.)");

        System.out.println();
        System.out.println("=== Section 3: many virtual threads, few carrier (platform) threads underneath ===");
        int taskCount = 1_000;
        Set<String> carrierNamesUsed = ConcurrentHashMap.newKeySet();
        AtomicInteger completed = new AtomicInteger();
        try (ExecutorService vExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < taskCount; i++) {
                vExecutor.submit(() -> {
                    sleepQuietly(20);
                    String vthreadDescription = Thread.currentThread().toString();
                    int atIndex = vthreadDescription.indexOf('@');
                    carrierNamesUsed.add(atIndex >= 0 ? vthreadDescription.substring(atIndex + 1) : vthreadDescription);
                    completed.incrementAndGet();
                });
            }
            vExecutor.shutdown();
            vExecutor.awaitTermination(30, TimeUnit.SECONDS);
        }
        System.out.println(taskCount + " virtual threads completed: " + completed.get());
        System.out.println("Distinct carrier threads (from each virtual thread's own toString()): " + carrierNamesUsed.size());
        System.out.println("Runtime.availableProcessors()=" + Runtime.getRuntime().availableProcessors());
        System.out.println("(Every virtual thread's toString() names the platform 'carrier' thread it is currently mounted");
        System.out.println("on, e.g. VirtualThread[#123]/runnable@ForkJoinPool-1-worker-2 - " + taskCount + " virtual threads");
        System.out.println("ran using only a handful of real OS threads, because each one unmounts from its carrier the");
        System.out.println("instant it blocks on something like Thread.sleep(), freeing that carrier to run a different");
        System.out.println("virtual thread while this one is waiting. The exact carrier count scales with core count, not");
        System.out.println("task count - expect a small number close to availableProcessors(), never anywhere near " + taskCount + ".)");

        System.out.println();
        System.out.println("=== Section 4: pinning - when a virtual thread CANNOT unmount from its carrier ===");
        int pinnedTaskCount = Runtime.getRuntime().availableProcessors() * 2;
        System.out.println("Running " + pinnedTaskCount + " virtual threads (2x availableProcessors), each sleeping 300ms,");
        System.out.println("first WITHOUT a synchronized block, then WITH one wrapping the sleep (different monitor object");
        System.out.println("per task, so there is no lock contention - only the pinning effect is being isolated):");

        long unpinnedStart = System.nanoTime();
        runConcurrentTasks(pinnedTaskCount, false);
        long unpinnedElapsedMs = (System.nanoTime() - unpinnedStart) / 1_000_000;

        long pinnedStart = System.nanoTime();
        runConcurrentTasks(pinnedTaskCount, true);
        long pinnedElapsedMs = (System.nanoTime() - pinnedStart) / 1_000_000;

        System.out.println("Without synchronized (unpinned): " + unpinnedElapsedMs + "ms");
        System.out.println("With synchronized (pinned):       " + pinnedElapsedMs + "ms");
        System.out.println("Running on: " + Runtime.version());
        boolean pinningObserved = pinnedElapsedMs > (unpinnedElapsedMs * 3L / 2);
        if (pinningObserved) {
            System.out.println("(Pinning WAS observed: with only availableProcessors()-many carriers and twice that many");
            System.out.println("pinned tasks, the carriers had to serve the tasks in roughly two waves. This is the classic");
            System.out.println("pre-JDK-24 behavior - a virtual thread blocked inside a synchronized block could not unmount");
            System.out.println("from its carrier, so it held that OS thread hostage for the whole sleep, exactly like a");
            System.out.println("platform thread would.)");
        } else {
            System.out.println("(Pinning was NOT observed - pinned and unpinned times are close. This is the CORRECT and");
            System.out.println("EXPECTED result on JDK 24 and later: JEP 491 (\"Synchronize Virtual Threads without Pinning\",");
            System.out.println("delivered in JDK 24) reworked the JVM's monitor implementation so a virtual thread CAN now");
            System.out.println("unmount from its carrier even while it holds a synchronized lock. On a pre-JDK-24 runtime,");
            System.out.println("this exact same code would show the pinned run taking roughly double the unpinned run instead -");
            System.out.println("the classic pinning problem this section was originally written to demonstrate. What STILL");
            System.out.println("pins a virtual thread on JDK 24+: a native method call, a blocking Foreign Function & Memory");
            System.out.println("API call, or a callback FROM native code that blocks or synchronizes - none of which apply");
            System.out.println("to this section's plain Java synchronized block.)");
        }

        System.out.println();
        System.out.println("=== Section 5: why this project's AlertConsumer poll loop stays exactly as it is ===");
        System.out.println("Virtual threads solve one specific problem: how to run a huge NUMBER of concurrent blocking-I/O");
        System.out.println("tasks without paying a platform thread's ~1MB stack and OS-scheduling cost per task - the exact");
        System.out.println("problem Step 7's bounded Executors.newFixedThreadPool() has to size carefully around. AlertConsumer's");
        System.out.println("poll loop is a single sequential consumer of one partition-ordered stream, not a fan-out of many");
        System.out.println("independent blocking tasks - there is nothing here to parallelize with more threads, virtual or");
        System.out.println("otherwise, and Phase 8 Step 9's CompletableFuture case study already established why reordering");
        System.out.println("the persist-then-commit sequence would be actively harmful. Also worth naming explicitly, and");
        System.out.println("corrected by Section 4's own live result: on a PRE-JDK-24 runtime, a JDBC call issued from inside");
        System.out.println("a synchronized block could pin its carrier for the call's full duration - a real historical risk");
        System.out.println("for connection-pool code that happened to wrap a blocking call in synchronized. On JDK 24+ (this");
        System.out.println("project runs JDK 26), JEP 491 removes that specific risk for plain synchronized blocks. The");
        System.out.println("narrower remaining risk for a future virtual-thread-per-request service (a Spring Boot REST layer,");
        System.out.println("Phase 11): a JDBC driver or connection-pool library that reaches into native code (JNI) or the");
        System.out.println("Foreign Function & Memory API on its blocking path would still pin its carrier - worth checking");
        System.out.println("before betting that layer's scalability on virtual threads, even though the old synchronized-block");
        System.out.println("concern itself is gone.");
        System.out.println("The place virtual threads WOULD genuinely help in this project's future: a REST API sitting in");
        System.out.println("front of this platform, handling many simultaneous inbound requests that each block on a DB query -");
        System.out.println("thread-per-request scales naturally with virtual threads in a way it never could with a bounded");
        System.out.println("platform-thread pool.");
    }

    private static void runConcurrentTasks(int count, boolean pinned) throws InterruptedException {
        try (ExecutorService vExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < count; i++) {
                Object monitor = new Object();
                vExecutor.submit(() -> {
                    if (pinned) {
                        synchronized (monitor) {
                            sleepQuietly(300);
                        }
                    } else {
                        sleepQuietly(300);
                    }
                });
            }
            vExecutor.shutdown();
            vExecutor.awaitTermination(30, TimeUnit.SECONDS);
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