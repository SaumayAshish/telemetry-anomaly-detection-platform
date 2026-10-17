package com.telemetry.platform.alerting;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class LocksAndAtomicsHarness {

    private static final AtomicLong atomicCounter = new AtomicLong(0);
    private static long synchronizedCounter = 0;

    private static synchronized void incrementSynchronized() {
        synchronizedCounter++;
    }

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Section 1: fixing Step 2/3's lost-update race with AtomicLong (no synchronized needed) ===");
        int threadCount = 10;
        int incrementsPerThread = 100_000;
        long expected = (long) threadCount * incrementsPerThread;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    atomicCounter.incrementAndGet();
                }
            });
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        System.out.println("Expected total: " + expected);
        System.out.println("Actual total:   " + atomicCounter.get());
        System.out.println("(Exactly equal, every run - with no synchronized keyword and no explicit lock anywhere.)");

        System.out.println();
        System.out.println("=== Section 2: what incrementAndGet() does internally - a manual compare-and-swap retry loop ===");
        AtomicLong manualCas = new AtomicLong(10);
        long observed = manualCas.get();
        long updated = observed * 2;
        boolean success = manualCas.compareAndSet(observed, updated);
        System.out.println("  Read " + observed + ", computed " + updated + ", compareAndSet succeeded: " + success
                + ", final value: " + manualCas.get());

        System.out.println("  Now simulating another thread sneaking in a change between our read and our swap attempt:");
        long staleRead = manualCas.get();
        manualCas.set(999);
        boolean staleSuccess = manualCas.compareAndSet(staleRead, staleRead + 1);
        System.out.println("  compareAndSet using the now-stale read (" + staleRead + ") succeeded: " + staleSuccess
                + " (must be false - the value changed to 999 underneath us)");
        System.out.println("  Final value is still: " + manualCas.get()
                + " (the stale swap was correctly rejected instead of silently overwriting the concurrent update)");

        System.out.println();
        System.out.println("=== Section 3: fixing Step 2/3's check-then-act race with ReentrantLock instead of synchronized ===");
        Map<String, Boolean> seenAlerts = new HashMap<>();
        String alertId = "ALERT-42";
        ReentrantLock checkThenActLock = new ReentrantLock();

        Runnable checkThenActSafe = () -> {
            checkThenActLock.lock();
            try {
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
            } finally {
                checkThenActLock.unlock();
            }
        };

        Thread p1 = new Thread(checkThenActSafe, "processor-1");
        Thread p2 = new Thread(checkThenActSafe, "processor-2");
        p1.start();
        Thread.sleep(20);
        p2.start();
        p1.join();
        p2.join();
        System.out.println("(Same correctness as Step 3's synchronized block - but note the mandatory try/finally: unlike");
        System.out.println("synchronized, ReentrantLock does NOT release automatically, even if an exception is thrown.)");

        System.out.println();
        System.out.println("=== Section 4: tryLock() with a timeout - backing off instead of blocking forever ===");
        ReentrantLock timedLock = new ReentrantLock();

        Thread holder = new Thread(() -> {
            timedLock.lock();
            try {
                System.out.println("  [holder] acquired the lock, holding it for 1000ms...");
                sleepQuietly(1000);
            } finally {
                timedLock.unlock();
                System.out.println("  [holder] released the lock");
            }
        }, "holder");

        holder.start();
        sleepQuietly(100);

        boolean acquired = timedLock.tryLock(300, TimeUnit.MILLISECONDS);
        if (acquired) {
            System.out.println("  [main] unexpectedly acquired the lock immediately");
            timedLock.unlock();
        } else {
            System.out.println("  [main] tryLock(300ms) timed out while the holder was still working - backed off gracefully");
            System.out.println("  instead of blocking forever. A plain synchronized block has no equivalent - it always waits.");
        }
        holder.join();

        System.out.println();
        System.out.println("=== Section 5: synchronized vs AtomicLong under real contention - a performance comparison ===");
        int perfThreadCount = 10;
        int perfIncrements = 500_000;

        for (int warmup = 0; warmup < 2; warmup++) {
            runSynchronizedBenchmark(perfThreadCount, perfIncrements);
            runAtomicBenchmark(perfThreadCount, perfIncrements);
        }

        long syncStart = System.nanoTime();
        runSynchronizedBenchmark(perfThreadCount, perfIncrements);
        long syncEnd = System.nanoTime();

        long atomicStart = System.nanoTime();
        runAtomicBenchmark(perfThreadCount, perfIncrements);
        long atomicEnd = System.nanoTime();

        System.out.println("synchronized counter (ms): " + (syncEnd - syncStart) / 1_000_000.0);
        System.out.println("AtomicLong counter   (ms): " + (atomicEnd - atomicStart) / 1_000_000.0);
        System.out.println("(Exact numbers are machine-dependent - the point is that AtomicLong avoids OS-level lock");
        System.out.println("acquisition entirely via CAS retries, which is typically, but not always, faster under");
        System.out.println("moderate contention on a single simple field like this.)");
    }

    private static void runSynchronizedBenchmark(int threadCount, int incrementsPerThread) throws InterruptedException {
        synchronizedCounter = 0;
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    incrementSynchronized();
                }
            });
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
    }

    private static void runAtomicBenchmark(int threadCount, int incrementsPerThread) throws InterruptedException {
        AtomicLong localCounter = new AtomicLong(0);
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    localCounter.incrementAndGet();
                }
            });
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}