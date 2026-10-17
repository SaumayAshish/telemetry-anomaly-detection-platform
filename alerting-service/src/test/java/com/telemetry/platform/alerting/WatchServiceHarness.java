package com.telemetry.platform.alerting;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WatchServiceHarness {

    public static void main(String[] args) throws IOException, InterruptedException {

        Path watchedDir = Path.of("harness-scratch", "config");
        Files.createDirectories(watchedDir);

        System.out.println("=== Section 1: registering a directory for CREATE, MODIFY, and DELETE events ===");
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            WatchKey registrationKey = watchedDir.register(watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE);
            System.out.println("Registered " + watchedDir + " - watch key valid=" + registrationKey.isValid());
            System.out.println("(This subscribes to the OS's own native file-change notification mechanism for this one");
            System.out.println("directory - no polling loop checking timestamps, no periodic re-listing. The OS itself will");
            System.out.println("wake this JVM up the moment something changes.)");

            System.out.println();
            System.out.println("=== Section 2: triggering real events on a background thread, observing them arrive ===");
            Path configFile = watchedDir.resolve("app-config.properties");
            AtomicInteger eventsObserved = new AtomicInteger();

            Thread fileChanger = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    Files.writeString(configFile, "threshold=3.0\n", StandardOpenOption.CREATE);
                    Thread.sleep(200);
                    Files.writeString(configFile, "threshold=3.5\n", StandardOpenOption.TRUNCATE_EXISTING);
                    Thread.sleep(200);
                    Files.delete(configFile);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            fileChanger.start();

            long deadline = System.currentTimeMillis() + 3000;
            boolean deleteObserved = false;
            while (System.currentTimeMillis() < deadline && !deleteObserved) {
                WatchKey signalledKey = watchService.poll(1, TimeUnit.SECONDS);
                if (signalledKey == null) {
                    continue;
                }
                for (WatchEvent<?> event : signalledKey.pollEvents()) {
                    System.out.println("  Event: " + event.kind().name() + " on " + event.context());
                    eventsObserved.incrementAndGet();
                    if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
                        deleteObserved = true;
                    }
                }
                boolean stillValid = signalledKey.reset();
                if (!stillValid) {
                    System.out.println("  (key.reset() returned false - the directory is no longer accessible)");
                    break;
                }
            }
            fileChanger.join();
            System.out.println("Total events observed: " + eventsObserved.get());
            System.out.println("(Notice this loop never once checked a timestamp or re-listed the directory itself - every");
            System.out.println("event printed above arrived because the OS pushed a notification, and key.reset() after each");
            System.out.println("batch is what makes the key eligible to signal again. Skip reset() and this key would silently");
            System.out.println("stop reporting anything after its first batch, with no exception thrown. Also notice the loop");
            System.out.println("waits for the specific ENTRY_DELETE event, not a fixed event count - a single logical write can");
            System.out.println("legitimately produce more than one ENTRY_MODIFY notification depending on the OS, so counting");
            System.out.println("events instead of watching for the one that actually signals completion is the wrong design.)");
        }

        System.out.println();
        System.out.println("=== Section 3: where this project would actually use this - hot-reloading config ===");
        System.out.println("This project's Kafka broker address, alert thresholds, and DB connection settings currently");
        System.out.println("require a restart to change. A WatchService on the config directory, reacting to ENTRY_MODIFY");
        System.out.println("by re-parsing the changed file and swapping in new values (behind a thread-safe reference, e.g. an");
        System.out.println("AtomicReference to an immutable config object - never mutating shared config in place), is the");
        System.out.println("standard enterprise pattern for zero-downtime config changes. Real caveat worth naming: the JDK");
        System.out.println("docs warn that under heavy filesystem activity, a StandardWatchEventKinds.OVERFLOW event can be");
        System.out.println("delivered instead of the real event(s) if too many changes happen too fast for the internal queue");
        System.out.println("to keep up - production hot-reload code should treat OVERFLOW as 'something changed, re-scan the");
        System.out.println("whole config directory to be safe' rather than assuming every individual change was reported.");
        System.out.println("Section 2's own bug is a related lesson: even ordinary, non-overflow event delivery can carry more");
        System.out.println("or fewer notifications than a naive mental model predicts, so production code should key its logic");
        System.out.println("off which event KINDS matter, never off how many events are expected to arrive.");

        Files.deleteIfExists(watchedDir.resolve("app-config.properties"));
        Files.delete(watchedDir);
        Files.delete(watchedDir.getParent());
    }
}