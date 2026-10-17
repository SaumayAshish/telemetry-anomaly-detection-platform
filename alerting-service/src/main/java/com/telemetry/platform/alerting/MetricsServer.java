package com.telemetry.platform.alerting;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Serves the Prometheus text of an AlertMetrics at GET /metrics. One daemon worker thread
 * answers scrapes, so a slow scraper can never hold up the consumer, and the socket is
 * bound on every interface because the port must be reachable from outside a container.
 */
final class MetricsServer implements AutoCloseable {

    private static final String METRICS_PATH = "/metrics";
    private static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private final HttpServer server;
    private final ExecutorService worker;
    private final AtomicBoolean closed = new AtomicBoolean();

    private MetricsServer(HttpServer server, ExecutorService worker) {
        this.server = server;
        this.worker = worker;
    }

    static MetricsServer start(int port, AlertMetrics metrics) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        ExecutorService worker = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "metrics-server");
            thread.setDaemon(true);
            return thread;
        });
        server.createContext("/", exchange -> handle(exchange, metrics));
        server.setExecutor(worker);
        server.start();
        return new MetricsServer(server, worker);
    }

    int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            server.stop(0);
            worker.shutdownNow();
        }
    }

    private static void handle(HttpExchange exchange, AlertMetrics metrics) throws IOException {
        try (exchange) {
            if (!METRICS_PATH.equals(exchange.getRequestURI().getPath())) {
                respond(exchange, 404, "text/plain; charset=utf-8", "not found\n");
            } else if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                respond(exchange, 405, "text/plain; charset=utf-8", "method not allowed\n");
            } else {
                respond(exchange, 200, CONTENT_TYPE, metrics.scrape());
            }
        }
    }

    private static void respond(HttpExchange exchange, int status, String contentType, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
