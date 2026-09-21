package com.telemetry.platform.alerting;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;

public class SynchronizationHarness {

    private static long safeCounter = 0;

    private static synchronized void safeIncrement() {
        safeCounter++;
    }

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Section 1: fixing Step 2's lost-update race with synchronized ===");
        int threadCount = 10;
        int incrementsPerThread = 100_000;
        long expected = (long) threadCount * incrementsPerThread;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    safeIncrement();
                }
            });
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        System.out.println("Expected total: " + expected);
        System.out.println("Actual total:   " + safeCounter);
        System.out.println("(Unlike Step 2's unsafeCounter, this will be exactly equal, every single run.)");

        System.out.println();
        System.out.println("=== Section 2: fixing Step 2's check-then-act race with a synchronized block ===");
        Map<String, Boolean> seenAlerts = new HashMap<>();
        String alertId = "ALERT-42";
        Object lock = new Object();

        Runnable checkThenActSafe = () -> {
            synchronized (lock) {
                if (!seenAlerts.containsKey(alertId)) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    seenAlerts.put(alertId, true);
                    System.out.println("  [" + Thread.currentThread().getName() + "] processed " + alertId + " (first time)");
                } else {
                    System.out.println("  [" + Thread.currentThread().getName() + "] skipped " + alertId + " - already seen");
                }
            }
        };

        Thread p1 = new Thread(checkThenActSafe, "processor-1");
        Thread p2 = new Thread(checkThenActSafe, "processor-2");
        p1.start();
        Thread.sleep(20);
        p2.start();
        p1.join();
        p2.join();
        System.out.println("(processor-2 could not even begin its check until processor-1 released the lock -");
        System.out.println("the check and the act are now one atomic unit, so no duplicate is possible.)");

        System.out.println();
        System.out.println("=== Section 3: intrinsic locks are reentrant ===");
        ReentrantDemo demo = new ReentrantDemo();
        demo.outer();

        System.out.println();
        System.out.println("=== Section 4: deadlock - two threads, two locks, opposite acquisition order ===");
        Object lockA = new Object();
        Object lockB = new Object();

        Thread threadOne = new Thread(() -> {
            synchronized (lockA) {
                System.out.println("  [thread-1] holding lockA, waiting for lockB...");
                sleepQuietly(200);
                synchronized (lockB) {
                    System.out.println("  [thread-1] acquired both locks (this line should never print)");
                }
            }
        }, "thread-1");

        Thread threadTwo = new Thread(() -> {
            synchronized (lockB) {
                System.out.println("  [thread-2] holding lockB, waiting for lockA...");
                sleepQuietly(200);
                synchronized (lockA) {
                    System.out.println("  [thread-2] acquired both locks (this line should never print)");
                }
            }
        }, "thread-2");

        threadOne.start();
        threadTwo.start();
        Thread.sleep(2000);

        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long[] deadlockedIds = threadBean.findDeadlockedThreads();
        if (deadlockedIds != null) {
            System.out.println("Deadlock detected! " + deadlockedIds.length + " threads are permanently blocked:");
            for (long id : deadlockedIds) {
                ThreadInfo info = threadBean.getThreadInfo(id);
                System.out.println("  " + info.getThreadName() + " is " + info.getThreadState()
                        + ", waiting on " + info.getLockName()
                        + " (owned by " + info.getLockOwnerName() + ")");
            }
        } else {
            System.out.println("No deadlock detected (unexpected - check timing).");
        }

        System.out.println();
        System.out.println("=== Section 5: same scenario, fixed via consistent lock ordering - no deadlock ===");
        Object lockC = new Object();
        Object lockD = new Object();

        Thread threadThree = new Thread(() -> {
            synchronized (lockC) {
                sleepQuietly(100);
                synchronized (lockD) {
                    System.out.println("  [thread-3] acquired both locks (lockC then lockD)");
                }
            }
        }, "thread-3");

        Thread threadFour = new Thread(() -> {
            synchronized (lockC) {
                sleepQuietly(50);
                synchronized (lockD) {
                    System.out.println("  [thread-4] acquired both locks (lockC then lockD)");
                }
            }
        }, "thread-4");

        threadThree.start();
        threadFour.start();
        threadThree.join();
        threadFour.join();
        System.out.println("Both threads finished cleanly - always acquiring lockC before lockD, never the reverse,");
        System.out.println("makes a circular wait impossible no matter how the threads interleave.");

        System.out.println();
        System.out.println("=== Section 4's deadlocked threads are still stuck forever - ending the JVM explicitly ===");
        System.exit(0);
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static class ReentrantDemo {
        synchronized void outer() {
            System.out.println("  outer() acquired the lock on 'this'");
            inner();
        }

        synchronized void inner() {
            System.out.println("  inner() acquired the SAME lock on 'this' again, from within outer() - no deadlock");
        }
    }
}