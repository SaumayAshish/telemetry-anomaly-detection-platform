package com.telemetry.platform.alerting;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConcurrentCollectionsHarness {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Section 1: fixing Step 2/3/5's check-then-act race with ConcurrentHashMap.putIfAbsent() ===");
        Map<String, Boolean> seenAlerts = new ConcurrentHashMap<>();
        String alertId = "ALERT-42";

        Runnable checkThenActAtomic = () -> {
            Boolean previous = seenAlerts.putIfAbsent(alertId, true);
            if (previous == null) {
                System.out.println("  [" + Thread.currentThread().getName() + "] processed " + alertId + " (first time)");
            } else {
                System.out.println("  [" + Thread.currentThread().getName() + "] skipped " + alertId + " - already seen");
            }
        };

        Thread p1 = new Thread(checkThenActAtomic, "processor-1");
        Thread p2 = new Thread(checkThenActAtomic, "processor-2");
        p1.start();
        p2.start();
        p1.join();
        p2.join();
        System.out.println("(No synchronized block, no ReentrantLock, no explicit lock object anywhere - putIfAbsent()");
        System.out.println("does the check and the insert as a single atomic operation inside the map itself.)");

        System.out.println();
        System.out.println("=== Section 2: why this scales better - per-bin locking, not one whole-map lock ===");
        System.out.println("A synchronized HashMap (via Collections.synchronizedMap()) or the legacy Hashtable class");
        System.out.println("locks the ENTIRE map for every operation, so two threads touching completely unrelated keys");
        System.out.println("still serialize behind each other. ConcurrentHashMap instead locks (or CAS-updates) only the");
        System.out.println("specific bucket a key hashes to, so threads working on different buckets proceed in parallel.");

        System.out.println();
        System.out.println("=== Section 3: a plain ArrayList throws ConcurrentModificationException under concurrent structural change ===");
        List<Integer> plainList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            plainList.add(i);
        }

        Thread modifier = new Thread(() -> {
            sleepQuietly(50);
            plainList.add(999);
            System.out.println("  [modifier] added an element while iteration was in progress");
        }, "modifier");
        modifier.start();

        try {
            for (Integer value : plainList) {
                System.out.println("  iterating: " + value);
                sleepQuietly(30);
            }
            System.out.println("  iteration completed without exception (unexpected on this run)");
        } catch (ConcurrentModificationException e) {
            System.out.println("  ConcurrentModificationException thrown mid-iteration, as expected - a plain ArrayList's");
            System.out.println("  iterator fails fast on detecting a structural change instead of silently producing");
            System.out.println("  wrong results.");
        }
        modifier.join();

        System.out.println();
        System.out.println("=== Section 4: CopyOnWriteArrayList avoids this - safe concurrent iteration ===");
        CopyOnWriteArrayList<Integer> cowList = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 5; i++) {
            cowList.add(i);
        }

        Thread cowModifier = new Thread(() -> {
            sleepQuietly(50);
            cowList.add(999);
            System.out.println("  [modifier] added an element to the CopyOnWriteArrayList mid-iteration");
        }, "cow-modifier");
        cowModifier.start();

        for (Integer value : cowList) {
            System.out.println("  iterating: " + value);
            sleepQuietly(30);
        }
        System.out.println("  Iteration completed cleanly, no exception - the iterator worked off a fixed snapshot of the");
        System.out.println("  array taken when iteration began; the concurrent add() created a NEW internal array without");
        System.out.println("  touching the one this iterator already holds. Note this means the 999 just added is NOT seen");
        System.out.println("  in this iteration - only in a fresh one. Current size after the add: " + cowList.size());
        cowModifier.join();

        System.out.println();
        System.out.println("=== Section 5: BlockingQueue - producer/consumer handoff with no manual wait/notify ===");
        BlockingQueue<Integer> queue = new LinkedBlockingQueue<>();

        Thread producer = new Thread(() -> {
            for (int i = 1; i <= 3; i++) {
                sleepQuietly(200);
                System.out.println("  [producer] putting " + i);
                try {
                    queue.put(i);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "producer");

        Thread consumer = new Thread(() -> {
            for (int i = 1; i <= 3; i++) {
                try {
                    Integer value = queue.take();
                    System.out.println("  [consumer] took " + value
                            + " (blocked until it was available - no spin-loop, no wait()/notify())");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "consumer");

        consumer.start();
        producer.start();
        producer.join();
        consumer.join();
        System.out.println("(This is conceptually the same shape as Kafka's own poll(Duration) - a consumer blocking");
        System.out.println("until work is available, rather than busy-checking a shared collection in a loop.)");
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}