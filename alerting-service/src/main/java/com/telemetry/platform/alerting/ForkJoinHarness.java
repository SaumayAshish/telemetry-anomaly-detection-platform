package com.telemetry.platform.alerting;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class ForkJoinHarness {

    private static final long THRESHOLD = 10_000;

    public static void main(String[] args) {

        System.out.println("=== Section 1: RecursiveTask divide-and-conquer sum, correctness first ===");
        int size = 20_000_000;
        long[] data = new long[size];
        for (int i = 0; i < size; i++) {
            data[i] = 1;
        }
        long expected = size;

        ForkJoinPool pool = ForkJoinPool.commonPool();
        long actual = pool.invoke(new SumTask(data, 0, data.length));
        System.out.println("Expected sum: " + expected);
        System.out.println("Actual sum:   " + actual);
        System.out.println("(Must be exactly equal, every run - this is pure recursive decomposition, no shared mutable state.)");

        System.out.println();
        System.out.println("=== Section 2: fork one side, compute the other directly - why forking both wastes a thread ===");
        System.out.println("SumTask.compute() below forks the LEFT half (hands it to another worker) and computes the RIGHT");
        System.out.println("half directly on the CURRENT thread, then calls leftTask.join() to wait for the forked half.");
        System.out.println("Forking both halves would mean the current thread does no useful work while waiting on two");
        System.out.println("forked tasks instead of one - wasteful, since the current thread is a real worker thread too.");

        System.out.println();
        System.out.println("=== Section 3: work-stealing - more than one thread actually participates ===");
        System.out.println("Runtime.availableProcessors()=" + Runtime.getRuntime().availableProcessors()
                + ", commonPool parallelism=" + pool.getParallelism());
        Set<String> threadNamesUsed = ConcurrentHashMap.newKeySet();
        pool.invoke(new ThreadTrackingSumTask(data, 0, data.length, threadNamesUsed));
        System.out.println("Distinct threads that executed a leaf task: " + threadNamesUsed.size());
        System.out.println("(Each ForkJoinPool worker keeps its own double-ended queue of forked subtasks; a worker that runs");
        System.out.println("out of its own work steals from the OTHER end of a busier worker's queue instead of sitting idle.");
        System.out.println("This is what 'fork/join' really means: forked work is not assigned to a specific thread up front,");
        System.out.println("it is picked up by whichever worker is free. The exact count here scales with commonPool parallelism");
        System.out.println("(availableProcessors() - 1 by default) plus the calling thread itself, which also executes leaves");
        System.out.println("directly via the 'right side' of every split and via join()'s own helping mechanism - join() does not");
        System.out.println("just block, it can execute other pending forked work itself while waiting. On a low-core-count machine");
        System.out.println("this number can be surprisingly small (even 1: the caller thread finishing the whole tree itself before");
        System.out.println("the pool's one worker gets a chance to steal anything); on a machine with many more cores, expect");
        System.out.println("noticeably more distinct threads to show up here.)");

        System.out.println();
        System.out.println("=== Section 4: threshold sensitivity - splitting too small adds pure overhead ===");
        for (int i = 0; i < 2; i++) {
            pool.invoke(new SumTask(data, 0, data.length));
            pool.invoke(new TinyThresholdSumTask(data, 0, data.length));
        }

        long goodStart = System.nanoTime();
        pool.invoke(new SumTask(data, 0, data.length));
        long goodEnd = System.nanoTime();

        long tinyStart = System.nanoTime();
        pool.invoke(new TinyThresholdSumTask(data, 0, data.length));
        long tinyEnd = System.nanoTime();

        System.out.println("Threshold=" + THRESHOLD + " (sensible):     " + (goodEnd - goodStart) / 1_000_000.0 + "ms");
        System.out.println("Threshold=1 (splits to single elements): " + (tinyEnd - tinyStart) / 1_000_000.0 + "ms");
        System.out.println("(Exact numbers are machine-dependent, but threshold=1 should be substantially slower - splitting");
        System.out.println("20 million elements down to single-element tasks creates ~20 million RecursiveTask objects and");
        System.out.println("~20 million fork/join calls, and the bookkeeping overhead swamps the trivial per-leaf work.)");

        System.out.println();
        System.out.println("=== Section 5: this is the exact engine Phase 7 Step 7's parallel streams were already using ===");
        System.out.println("IntStream.range(...).parallel() and Arrays.stream(...).parallel() do not invent their own thread");
        System.out.println("pool - they submit a Spliterator-based recursive decomposition to this same ForkJoinPool.commonPool(),");
        System.out.println("using the identical fork/compute/join and work-stealing mechanics demonstrated above. The difference");
        System.out.println("is who writes the recursive split: a Stream's Spliterator does it for you automatically for");
        System.out.println("array- and collection-backed sources; SumTask above does it by hand. In practice, prefer a parallel");
        System.out.println("stream whenever the data is already array- or collection-shaped, exactly as Phase 7 Step 7 did -");
        System.out.println("reach for a raw RecursiveTask only when the recursive structure is NOT something Stream's built-in");
        System.out.println("Spliterators know how to split, e.g. a custom tree or graph traversal.");
    }

    static class SumTask extends RecursiveTask<Long> {
        private final long[] data;
        private final int start;
        private final int end;

        SumTask(long[] data, int start, int end) {
            this.data = data;
            this.start = start;
            this.end = end;
        }

        @Override
        protected Long compute() {
            int length = end - start;
            if (length <= THRESHOLD) {
                long sum = 0;
                for (int i = start; i < end; i++) {
                    sum += data[i];
                }
                return sum;
            }
            int mid = start + length / 2;
            SumTask leftTask = new SumTask(data, start, mid);
            SumTask rightTask = new SumTask(data, mid, end);
            leftTask.fork();
            long rightResult = rightTask.compute();
            long leftResult = leftTask.join();
            return leftResult + rightResult;
        }
    }

    static class ThreadTrackingSumTask extends RecursiveTask<Long> {
        private final long[] data;
        private final int start;
        private final int end;
        private final Set<String> threadNamesUsed;

        ThreadTrackingSumTask(long[] data, int start, int end, Set<String> threadNamesUsed) {
            this.data = data;
            this.start = start;
            this.end = end;
            this.threadNamesUsed = threadNamesUsed;
        }

        @Override
        protected Long compute() {
            int length = end - start;
            if (length <= THRESHOLD) {
                threadNamesUsed.add(Thread.currentThread().getName());
                long sum = 0;
                for (int i = start; i < end; i++) {
                    sum += data[i];
                }
                return sum;
            }
            int mid = start + length / 2;
            ThreadTrackingSumTask leftTask = new ThreadTrackingSumTask(data, start, mid, threadNamesUsed);
            ThreadTrackingSumTask rightTask = new ThreadTrackingSumTask(data, mid, end, threadNamesUsed);
            leftTask.fork();
            long rightResult = rightTask.compute();
            long leftResult = leftTask.join();
            return leftResult + rightResult;
        }
    }

    static class TinyThresholdSumTask extends RecursiveTask<Long> {
        private final long[] data;
        private final int start;
        private final int end;

        TinyThresholdSumTask(long[] data, int start, int end) {
            this.data = data;
            this.start = start;
            this.end = end;
        }

        @Override
        protected Long compute() {
            int length = end - start;
            if (length <= 1) {
                long sum = 0;
                for (int i = start; i < end; i++) {
                    sum += data[i];
                }
                return sum;
            }
            int mid = start + length / 2;
            TinyThresholdSumTask leftTask = new TinyThresholdSumTask(data, start, mid);
            TinyThresholdSumTask rightTask = new TinyThresholdSumTask(data, mid, end);
            leftTask.fork();
            long rightResult = rightTask.compute();
            long leftResult = leftTask.join();
            return leftResult + rightResult;
        }
    }
}
