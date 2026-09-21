package com.telemetry.platform.alerting;
public class VolatileHarness {

    private static boolean readyNonVolatile = false;
    private static volatile boolean readyVolatile = false;
    private static volatile boolean spinnerStillRunningNonVolatile = true;
    private static volatile boolean spinnerStillRunningVolatile = true;

    private static long unsafeVolatileCounter = 0;

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Section 1: the visibility problem - a non-volatile flag may never be observed ===");
        Thread spinnerNonVolatile = new Thread(() -> {
            long spins = 0;
            while (!readyNonVolatile) {
                spins++;
            }
            spinnerStillRunningNonVolatile = false;
            System.out.println("  [spinner-nonvolatile] observed the flag after " + spins + " spins");
        }, "spinner-nonvolatile");
        spinnerNonVolatile.setDaemon(true);
        spinnerNonVolatile.start();

        Thread.sleep(200);
        System.out.println("  Main thread setting readyNonVolatile = true now...");
        readyNonVolatile = true;

        Thread.sleep(1500);
        if (spinnerStillRunningNonVolatile) {
            System.out.println("  Still spinning 1.5 seconds after the write - the JIT-compiled loop cached the old value");
            System.out.println("  and never re-read main memory. This is the visibility bug, live.");
        } else {
            System.out.println("  The spinner did observe the write this time - visibility bugs are timing/JIT-dependent,");
            System.out.println("  not guaranteed to reproduce on every run or every machine (see 'How to verify' below).");
        }

        System.out.println();
        System.out.println("=== Section 2: the fix - marking the field volatile ===");
        Thread spinnerVolatile = new Thread(() -> {
            long spins = 0;
            while (!readyVolatile) {
                spins++;
            }
            spinnerStillRunningVolatile = false;
            System.out.println("  [spinner-volatile] observed the flag after " + spins + " spins");
        }, "spinner-volatile");
        spinnerVolatile.setDaemon(true);
        spinnerVolatile.start();

        Thread.sleep(200);
        System.out.println("  Main thread setting readyVolatile = true now...");
        readyVolatile = true;

        Thread.sleep(500);
        System.out.println("  Still spinning: " + spinnerStillRunningVolatile
                + " (this must be false - volatile guarantees every read sees the most recent write)");

        System.out.println();
        System.out.println("=== Section 3: volatile fixes visibility, NOT atomicity ===");
        int threadCount = 10;
        int incrementsPerThread = 100_000;
        long expected = (long) threadCount * incrementsPerThread;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    unsafeVolatileCounter++;
                }
            });
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        System.out.println("Expected total: " + expected);
        System.out.println("Actual total:   " + unsafeVolatileCounter);
        System.out.println("(Even if unsafeVolatileCounter were declared volatile, this would STILL show lost updates -");
        System.out.println("volatile only guarantees each read/write is visible across threads, it does not make");
        System.out.println("read-modify-write sequences like ++ atomic. Only synchronized (Step 3) or an Atomic class");
        System.out.println("(Step 5) fix this specific bug.)");

        System.out.println();
        System.out.println("=== Section 4: why synchronized also fixes visibility, not just atomicity ===");
        System.out.println("The Java Memory Model defines a 'happens-before' relationship: a volatile write happens-before");
        System.out.println("every subsequent volatile read of that same field, guaranteeing the reading thread sees it");
        System.out.println("and everything written before it. Acquiring/releasing a synchronized lock creates the exact");
        System.out.println("same happens-before edge - which is why Step 3's synchronized fix for the lost-update counter");
        System.out.println("also incidentally fixed visibility, even though visibility was never the problem being solved there.");

        System.out.println();
        System.out.println("=== Section 5: why this project used consumer.wakeup(), not a volatile flag, for shutdown ===");
        System.out.println("A volatile 'running' flag would correctly solve visibility for a loop like 'while (running) {...}' -");
        System.out.println("the shutdown-hook thread's write would be reliably seen by the main thread's next loop check.");
        System.out.println("But AlertConsumer's main thread is not spinning in a loop re-checking a condition - it is parked");
        System.out.println("inside the blocking native call consumer.poll(Duration), which does not re-evaluate anything");
        System.out.println("until it returns on its own. volatile solves visibility; it does not wake up a thread parked");
        System.out.println("in a blocking call. That is a different problem, and consumer.wakeup() is the tool for it.");
    }
}