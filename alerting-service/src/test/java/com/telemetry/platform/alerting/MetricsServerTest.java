package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The scrape endpoint of the alerting consumer, on a free port so tests never collide. */
class MetricsServerTest {

    private final AlertMetrics metrics = AlertMetrics.create();
    private final HttpClient client =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private MetricsServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = MetricsServer.start(0, metrics);
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    private HttpResponse<String> send(int port, String method, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(5))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void servesThePrometheusTextAtMetrics() throws Exception {
        metrics.commitLost();

        HttpResponse<String> response = send(server.port(), "GET", "/metrics");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                type -> assertThat(type).startsWith("text/plain"));
        assertThat(response.body()).contains("alerting_commits_lost_total 1.0");
    }

    @Test
    void everyScrapeShowsTheCurrentValuesNotASnapshot() throws Exception {
        assertThat(send(server.port(), "GET", "/metrics").body())
                .contains("alerting_alerts_duplicate_total 0.0");

        metrics.alertDuplicate();

        assertThat(send(server.port(), "GET", "/metrics").body())
                .contains("alerting_alerts_duplicate_total 1.0");
    }

    @Test
    void anyOtherPathIsNotFound() throws Exception {
        assertThat(send(server.port(), "GET", "/").statusCode()).isEqualTo(404);
        assertThat(send(server.port(), "GET", "/metrics/extra").statusCode()).isEqualTo(404);
    }

    @Test
    void onlyGetIsAllowedAndTheAnswerNamesIt() throws Exception {
        HttpResponse<String> response = send(server.port(), "POST", "/metrics");

        assertThat(response.statusCode()).isEqualTo(405);
        assertThat(response.headers().firstValue("Allow")).hasValue("GET");
    }

    @Test
    void portZeroPicksAFreePort() {
        assertThat(server.port()).isPositive();
    }

    @Test
    void aClosedServerStopsAnswering() throws Exception {
        MetricsServer other = MetricsServer.start(0, metrics);
        int port = other.port();
        other.close();

        assertThatThrownBy(() -> send(port, "GET", "/metrics")).isInstanceOf(IOException.class);
    }

    @Test
    void closingFreesThePortForTheNextStart() throws Exception {
        MetricsServer first = MetricsServer.start(0, metrics);
        int port = first.port();
        first.close();

        try (MetricsServer second = MetricsServer.start(port, metrics)) {
            assertThat(second.port()).isEqualTo(port);
        }
    }
}
