package com.telemetry.platform.alerting;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

public class PathAndFilesHarness {

    public static void main(String[] args) throws IOException {

        System.out.println("=== Section 1: Path is just a description - creating one touches nothing on disk ===");
        Path baseDir = Path.of("harness-scratch", "alerts");
        System.out.println("Path created: " + baseDir);
        System.out.println("Does it exist on disk yet? " + Files.exists(baseDir));
        System.out.println("(Path.of(...) built this in memory only - no file, no directory, nothing on disk exists");
        System.out.println("because of this line. Compare this to 'new File(...)' from the old API: same story there too -");
        System.out.println("constructing a File object never touched the filesystem either. The difference starts at Files.*)");

        System.out.println();
        System.out.println("=== Section 2: Files actually touches the filesystem, and reports real failures ===");
        Files.createDirectories(baseDir);
        System.out.println("After Files.createDirectories(baseDir): exists=" + Files.exists(baseDir)
                + ", isDirectory=" + Files.isDirectory(baseDir));

        Path failedAlertsFile = baseDir.resolve("failed-alerts.jsonl");
        System.out.println("Resolved file path: " + failedAlertsFile.toAbsolutePath());

        System.out.println("(baseDir.resolve(\"failed-alerts.jsonl\") appended a segment onto baseDir - resolve() is how you");
        System.out.println("build a child path without ever touching a string with '/' or '\\\\' yourself, which is exactly");
        System.out.println("the kind of manual path-string-concatenation bug NIO.2 exists to eliminate.)");

        System.out.println();
        System.out.println("=== Section 3: writing and reading text - the modern one-liners ===");
        String simulatedFailedAlert = "{\"sensorId\":\"S-1001\",\"reason\":\"db-unreachable\",\"timestampMs\":"
                + System.currentTimeMillis() + "}";
        Files.writeString(failedAlertsFile, simulatedFailedAlert + System.lineSeparator(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        System.out.println("Wrote one JSON line to " + failedAlertsFile.getFileName());

        String readBack = Files.readString(failedAlertsFile);
        System.out.println("Read back: " + readBack.strip());
        System.out.println("(Files.writeString/readString are Java 11+ conveniences for whole-file text. StandardOpenOption.CREATE");
        System.out.println("means 'make the file if it doesn't exist', APPEND means 'add to the end, don't overwrite' - this is");
        System.out.println("the exact combination a fallback-log file needs: never lose a previous failed alert by overwriting it.)");

        System.out.println();
        System.out.println("=== Section 4: Files.list() is LAZY - it must be closed, and try-with-resources is not optional ===");
        Files.writeString(baseDir.resolve("another-file.txt"), "placeholder");
        try (Stream<Path> entries = Files.list(baseDir)) {
            List<String> names = entries.map(p -> p.getFileName().toString()).sorted().toList();
            System.out.println("Directory listing (lazy stream, closed via try-with-resources): " + names);
        }
        System.out.println("(Files.list()/Files.walk()/Files.lines() all return a Stream backed by a live OS resource - an open");
        System.out.println("directory handle or file handle. The stream's close() releases that OS resource. If you never close");
        System.out.println("it - assign it to a variable and forget, or just iterate it with a plain for-loop-style call and never");
        System.out.println("wrap it in try-with-resources - you leak a real OS file handle every single call. Do this enough times");
        System.out.println("in a long-running service and you eventually hit 'too many open files' and the JVM process itself");
        System.out.println("starts failing unrelated file and socket operations. This is the single most common real-world");
        System.out.println("mistake with this API.)");

        System.out.println();
        System.out.println("=== Section 5: cleanup - Files.delete() throws real, specific exceptions ===");
        Files.delete(failedAlertsFile);
        Files.delete(baseDir.resolve("another-file.txt"));
        try {
            Files.delete(baseDir);
            System.out.println("Deleted empty directory: " + baseDir);
        } catch (DirectoryNotEmptyException e) {
            System.out.println("Could not delete - directory not empty: " + e.getMessage());
        }
        System.out.println("(Contrast with the old API: file.delete() returning false here would have told you nothing. NIO.2's");
        System.out.println("DirectoryNotEmptyException tells you exactly what went wrong, as a real, specific, catchable type.)");

        System.out.println();
        System.out.println("=== Section 6: where this fits this project - a local-disk last resort for AlertConsumer ===");
        System.out.println("Phase 6 built retry-with-backoff and a same-database DLQ table for alerts that fail to persist with");
        System.out.println("a non-retryable SQLState. But both of those assume Postgres itself is reachable. If the database is");
        System.out.println("fully down - connection pool exhausted, network partition, DB host unreachable - even the DLQ write");
        System.out.println("fails, and today that alert is simply logged and lost. A genuinely last-resort fallback: append the");
        System.out.println("alert as one JSON line to a local file (exactly Section 3's pattern) when the DLQ write itself throws.");
        System.out.println("This does not replace Phase 6's DLQ - it is one rung further down the failure ladder, for the case");
        System.out.println("Phase 6 explicitly did not cover: total database unavailability, not just one bad record.");
    }
}
