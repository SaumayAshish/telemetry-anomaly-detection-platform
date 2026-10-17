package com.telemetry.platform.alerting;

public class ThreadBasicsHarness {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Section 1: Runnable vs extending Thread ===");
        Runnable printTask = () -> System.out.println("  Running on thread: " + Thread.currentThread().getName());
        Thread viaRunnable = new Thread(printTask, "runnable-thread");
        viaRunnable.start();
        viaRunnable.join();

        Thread viaSubclass = new PrintingThread();
        viaSubclass.start();
        viaSubclass.join();

        System.out.println();
        System.out.println("=== Section 2: calling run() directly does NOT start a new thread ===");
        Thread notStarted = new Thread(printTask, "never-started-thread");
        System.out.println("Calling notStarted.run() directly (a common mistake):");
        notStarted.run();
        System.out.println("Note: the line above printed \"" + Thread.currentThread().getName() + "\", not \"never-started-thread\" -");
        System.out.println("run() is just an ordinary method call on the current thread when invoked directly, not a thread launch.");

        System.out.println();
        System.out.println("=== Section 3: real concurrency - two threads interleaving, not running sequentially ===");
        Thread worker1 = new Thread(() -> countAndPrint("worker-1", 5), "worker-1");
        Thread worker2 = new Thread(() -> countAndPrint("worker-2", 5), "worker-2");
        worker1.start();
        worker2.start();
        worker1.join();
        worker2.join();
        System.out.println("Both workers finished. Compare the interleaving above across repeated runs - it will differ run to run,");
        System.out.println("because the OS scheduler, not your code, decides which thread runs at any given instant.");

        System.out.println();
        System.out.println("=== Section 4: Thread.State across a thread's lifecycle ===");
        Thread lifecycleThread = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "lifecycle-thread");
        System.out.println("Before start(): " + lifecycleThread.getState());
        lifecycleThread.start();
        Thread.sleep(50);
        System.out.println("Shortly after start(), while sleeping: " + lifecycleThread.getState());
        lifecycleThread.join();
        System.out.println("After join() returns: " + lifecycleThread.getState());

        System.out.println();
        System.out.println("=== Section 5: where this project already uses a second thread ===");
        System.out.println("AlertConsumer's shutdown hook (Phase 4 Step 2) is exactly this mechanism:");
        System.out.println("Runtime.getRuntime().addShutdownHook(new Thread(() -> { consumer.wakeup(); ... }));");
        System.out.println("The JVM itself invokes that hook's start() on a separate thread when shutdown begins, which is");
        System.out.println("precisely why consumer.wakeup() (not a shared boolean flag) is required to interrupt the blocking");
        System.out.println("poll() call running on the main thread - two real OS-level threads, not one thread doing two things.");
    }

    static void countAndPrint(String label, int times) {
        for (int i = 1; i <= times; i++) {
            System.out.println("  [" + label + "] count " + i);
        }
    }

    static class PrintingThread extends Thread {
        PrintingThread() {
            super("subclass-thread");
        }

        @Override
        public void run() {
            System.out.println("  Running on thread: " + Thread.currentThread().getName());
        }
    }
}