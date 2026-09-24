package com.telemetry.platform.alerting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class RestClientCaseStudyHarness {

    static class DeviceMetadata {
        public String model;
        public String location;
        public String firmwareVersion;
    }

    static class EnrichedAlert {
        public String sensorId;
        public double reading;
        public DeviceMetadata device;
        public boolean usedCachedMetadata;
    }

    private static final Path CACHE_FILE = Path.of("harness-scratch", "device-metadata-cache.json");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    public static void main(String[] args) throws Exception {

        System.out.println("=== Section 1: an embedded device-metadata service - fails twice, then succeeds ===");
        Files.createDirectories(CACHE_FILE.getParent());
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        int port = server.getAddress().getPort();
        AtomicInteger callCount = new AtomicInteger(0);

        server.createContext("/device", exchange -> {
            int count = callCount.incrementAndGet();
            if (count <= 2) {
                exchange.sendResponseHeaders(503, -1);
                exchange.close();
                return;
            }
            String body = "{\"model\":\"EnviroSense-X200\",\"location\":\"Warehouse-A-Floor2\",\"firmwareVersion\":\"3.2.1\"}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        System.out.println("Embedded device-metadata service listening on port " + port
                + " - will return 503 for the first 2 calls, then succeed.");

        System.out.println();
        System.out.println("=== Section 2: enriching a reading - retry-with-backoff carries it through the transient failures ===");
        EnrichedAlert alert1 = enrichSensorReading(port, "S-1001", 71.2);
        System.out.println("Final enriched result: " + MAPPER.writeValueAsString(alert1));
        System.out.println("(Two 503 responses, absorbed by retry-with-backoff - exactly Phase 6's DB-resilience pattern,");
        System.out.println("applied here to an HTTP call instead of a database write. The successful device metadata was");
        System.out.println("also written to a local cache file - see Section 3.)");

        System.out.println();
        System.out.println("=== Section 3: the service goes fully down - the local disk cache carries the enrichment through ===");
        server.stop(0);
        System.out.println("Embedded service stopped entirely - not slow, not flaky, genuinely unreachable now.");
        EnrichedAlert alert2 = enrichSensorReading(port, "S-1001", 68.9);
        System.out.println("Final enriched result: " + MAPPER.writeValueAsString(alert2));
        System.out.println("(All 3 retry attempts failed with connection errors - a real, total outage, not a transient blip.");
        System.out.println("Rather than losing the enrichment entirely, the last successfully cached device metadata (Step 1's");
        System.out.println("local-disk-fallback idea, named back then, actually implemented now) was read back and combined");
        System.out.println("with the NEW reading. usedCachedMetadata=true makes this explicit and visible downstream, rather");
        System.out.println("than silently presenting stale data as if it were fresh.)");

        Files.deleteIfExists(CACHE_FILE);
        Files.deleteIfExists(CACHE_FILE.getParent());

        System.out.println();
        System.out.println("=== Phase 9 (File Handling) complete ===");
        System.out.println("Path & Files, Buffers & Channels, WatchService, Java Serialization, JSON with Jackson, XML basics,");
        System.out.println("CSV handling, Sockets, the java.net.http HTTP Client, and this closing case study combining retry-");
        System.out.println("with-backoff (Phase 6) with a disk-based fallback cache (Step 1) around a real REST call (Step 9)");
        System.out.println("and JSON serialization (Step 5) - a genuinely representative slice of real enterprise integration work.");
    }

    private static EnrichedAlert enrichSensorReading(int port, String sensorId, double reading) throws IOException, InterruptedException {
        EnrichedAlert alert = new EnrichedAlert();
        alert.sensorId = sensorId;
        alert.reading = reading;

        DeviceMetadata metadata = fetchDeviceMetadataWithRetry(port);
        if (metadata != null) {
            alert.device = metadata;
            alert.usedCachedMetadata = false;
            Files.writeString(CACHE_FILE, MAPPER.writeValueAsString(metadata),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            return alert;
        }

        if (Files.exists(CACHE_FILE)) {
            System.out.println("  WARN: device-metadata service unreachable after all retries - falling back to cached metadata");
            alert.device = MAPPER.readValue(Files.readString(CACHE_FILE), DeviceMetadata.class);
            alert.usedCachedMetadata = true;
            return alert;
        }

        throw new IOException("Device metadata service unreachable and no cached fallback available for " + sensorId);
    }

    private static DeviceMetadata fetchDeviceMetadataWithRetry(int port) throws InterruptedException {
        int maxAttempts = 3;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/device"))
                        .timeout(Duration.ofMillis(800))
                        .GET()
                        .build();
                HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    System.out.println("  Attempt " + attempt + ": succeeded (status 200)");
                    return MAPPER.readValue(response.body(), DeviceMetadata.class);
                }
                System.out.println("  Attempt " + attempt + ": failed with status " + response.statusCode());
            } catch (IOException e) {
                System.out.println("  Attempt " + attempt + ": failed - " + e.getClass().getSimpleName());
            }
            if (attempt < maxAttempts) {
                long backoffMs = 200L * attempt;
                System.out.println("  Backing off " + backoffMs + "ms before retry...");
                Thread.sleep(backoffMs);
            }
        }
        return null;
    }
}
