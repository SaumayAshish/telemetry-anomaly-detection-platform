package com.telemetry.platform.alerting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public class HttpClientHarness {

    static class ReadingResponse {
        public String sensorId;
        public double reading;
    }

    public static void main(String[] args) throws Exception {

        System.out.println("=== Section 1: an embedded test server - the JDK's own built-in HttpServer ===");
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        int port = server.getAddress().getPort();

        server.createContext("/reading", exchange -> {
            String responseBody = "{\"sensorId\":\"S-1001\",\"reading\":71.2}";
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        server.createContext("/alert", exchange -> {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                exchange.close();
                return;
            }
            byte[] requestBytes = exchange.getRequestBody().readAllBytes();
            String responseBody = "{\"status\":\"received\",\"bytesReceived\":" + requestBytes.length + "}";
            byte[] responseBytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(201, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        });

        server.createContext("/slow", exchange -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            byte[] bytes = "{\"status\":\"finally done\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        System.out.println("Embedded test server listening on port " + port + " (using Step 10's virtual-thread-per-task");
        System.out.println("executor to handle requests - exactly the high-concurrency-blocking-I/O workload named back then");
        System.out.println("as virtual threads' actual sweet spot.)");

        HttpClient client = HttpClient.newHttpClient();
        ObjectMapper mapper = new ObjectMapper();

        System.out.println();
        System.out.println("=== Section 2: a synchronous GET request - client.send() blocks until the response arrives ===");
        HttpRequest getRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/reading"))
                .GET()
                .build();
        HttpResponse<String> getResponse = client.send(getRequest, HttpResponse.BodyHandlers.ofString());
        System.out.println("Status code: " + getResponse.statusCode());
        System.out.println("Raw body: " + getResponse.body());
        ReadingResponse parsed = mapper.readValue(getResponse.body(), ReadingResponse.class);
        System.out.println("Parsed via Jackson (Step 5): sensorId=" + parsed.sensorId + ", reading=" + parsed.reading);
        System.out.println("(client.send() is fully synchronous - the calling thread blocks until the full response body has");
        System.out.println("arrived, exactly like Step 8's raw Socket readLine(). HttpClient is a much higher-level API built");
        System.out.println("on that same blocking-socket foundation, handling HTTP's own framing, headers, and status codes.)");

        System.out.println();
        System.out.println("=== Section 3: a synchronous POST request with a real request body ===");
        String alertJson = "{\"sensorId\":\"S-1001\",\"reading\":71.2,\"severity\":\"HIGH\"}";
        HttpRequest postRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/alert"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(alertJson))
                .build();
        HttpResponse<String> postResponse = client.send(postRequest, HttpResponse.BodyHandlers.ofString());
        System.out.println("Status code: " + postResponse.statusCode());
        System.out.println("Body: " + postResponse.body());

        System.out.println();
        System.out.println("=== Section 4: an asynchronous GET request - sendAsync() returns a CompletableFuture immediately ===");
        HttpRequest asyncRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/reading"))
                .GET()
                .build();
        CompletableFuture<HttpResponse<String>> futureResponse = client.sendAsync(asyncRequest, HttpResponse.BodyHandlers.ofString());
        System.out.println("sendAsync() call has returned - the calling thread was never blocked waiting for the network round trip.");
        CompletableFuture<Double> futureReading = futureResponse.thenApply(response -> {
            try {
                return mapper.readValue(response.body(), ReadingResponse.class).reading;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        System.out.println("Async result (blocking here only to print it, exactly like Phase 8 Step 9's own examples): "
                + futureReading.get());
        System.out.println("(sendAsync() is the exact same client and request as Section 2 - the only difference is the API");
        System.out.println("contract guarantees the calling thread doesn't block on the network call itself. This is Phase 8");
        System.out.println("Step 9's CompletableFuture applied directly to HTTP. The one new thing here is HttpResponse.");
        System.out.println("BodyHandlers, which decides how the response body gets consumed - ofString() here, but ofFile()/");
        System.out.println("ofInputStream() exist for large or streamed responses.)");

        System.out.println();
        System.out.println("=== Section 5: a request timeout - a real thrown exception, not a hang ===");
        HttpRequest timedRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/slow"))
                .timeout(Duration.ofMillis(500))
                .GET()
                .build();
        try {
            client.send(timedRequest, HttpResponse.BodyHandlers.ofString());
            System.out.println("  (this should NOT print - /slow takes 2000ms, the timeout is 500ms)");
        } catch (HttpTimeoutException e) {
            System.out.println("Request correctly timed out: " + e.getClass().getSimpleName());
        }
        System.out.println("(The /slow endpoint deliberately takes 2 seconds; the request declared a 500ms timeout. Without an");
        System.out.println("explicit timeout, a genuinely hung or extremely slow server can block a caller indefinitely - a");
        System.out.println("real, easy-to-forget production setting, not just a nice-to-have.)");

        System.out.println();
        System.out.println("=== Section 6: where this fits this project's future ===");
        System.out.println("This is the client-side counterpart to what Phase 11's Spring Boot REST controllers will build on");
        System.out.println("the server side. If this project ever exposed its own REST API, or this alerting-service needed to");
        System.out.println("call OUT to a notification/paging service for critical alerts, it would use exactly this HttpClient -");
        System.out.println("sync for a simple one-off call, async when composing multiple outbound calls without blocking a");
        System.out.println("thread per call.");

        server.stop(0);
    }
}