package com.telemetry.platform.alerting;

import java.util.HashMap;
import java.util.Map;

public class RaceConditionHarness {

    private static long unsafeCounter = 0;

    private static void unsafeIncrement() {
        unsafeCounter++;
    }

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Section 1: lost updates on a shared counter ===");
        int threadCount = 10;
        int incrementsPerThread = 100_000;
        long expected = (long) threadCount * incrementsPerThread;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    unsafeIncrement();
                }
            });
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        System.out.println("Expected total: " + expected);
        System.out.println("Actual total:   " + unsafeCounter);
        System.out.println("Lost updates:   " + (expected - unsafeCounter));
        System.out.println("(This number will vary run to run, and may occasionally even be 0 - that does NOT mean the code is correct.)");

        System.out.println();
        System.out.println("=== Section 2: why - counter++ is really three separate steps ===");
        System.out.println("counter++ compiles to: read counter into a temp, add 1 to the temp, write the temp back.");
        System.out.println("If two threads both read the same old value before either writes back, one increment is lost.");
        System.out.println("Forcing that interleaving with an artificial delay between the read and the write:");

        DelayedCounter delayed = new DelayedCounter();
        Thread a = new Thread(delayed::slowIncrement, "thread-A");
        Thread b = new Thread(delayed::slowIncrement, "thread-B");
        a.start();
        Thread.sleep(20);
        b.start();
        a.join();
        b.join();
        System.out.println("Both threads called slowIncrement() once each. Final value: " + delayed.value
                + " (if the interleaving forced the race, this will be 1, not 2)");

        System.out.println();
        System.out.println("=== Section 3: a check-then-act race - duplicate processing, not just lost counts ===");
        Map<String, Boolean> seenAlerts = new HashMap<>();
        String alertId = "ALERT-42";

        Runnable checkThenAct = () -> {
            if (!seenAlerts.containsKey(alertId)) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                Boolean previous = seenAlerts.put(alertId, true);
                System.out.println("  [" + Thread.currentThread().getName() + "] processed " + alertId
                        + (previous != null ? " (DUPLICATE - already processed once before!)" : " (first time)"));
            } else {
                System.out.println("  [" + Thread.currentThread().getName() + "] skipped " + alertId + " - already seen");
            }
        };

        Thread p1 = new Thread(checkThenAct, "processor-1");
        Thread p2 = new Thread(checkThenAct, "processor-2");
        p1.start();
        Thread.sleep(20);
        p2.start();
        p1.join();
        p2.join();

        System.out.println();
        System.out.println("=== Section 4: same root cause as Phase 7 Step 7's ArrayList corruption ===");
        System.out.println("The unsynchronized ArrayList.add() race from the Parallel Streams step, this counter, and this");
        System.out.println("HashMap check-then-act race are all the exact same bug: multiple threads reading and writing");
        System.out.println("shared mutable state with no coordination. The data structure changes; the root cause does not.");
        System.out.println("Step 3 (synchronized) gives you the tool to make a read-modify-write sequence into one atomic step.");
    }

    static class DelayedCounter {
        long value = 0;

        void slowIncrement() {
            long current = value;
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            value = current + 1;
        }
    }
}