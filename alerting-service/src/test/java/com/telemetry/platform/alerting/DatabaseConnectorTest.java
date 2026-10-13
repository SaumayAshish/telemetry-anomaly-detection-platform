package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// The timeout guards against a broken retry loop spinning forever.
@Timeout(10)
class DatabaseConnectorTest {

    private final List<Long> sleeps = new ArrayList<>();
    private final AtomicInteger attempts = new AtomicInteger();

    @Test
    void connectsImmediatelyWithoutSleeping() throws Exception {
        Connection connection = fakeConnection();
        DatabaseConnector connector = connectorThatYields(connection);

        assertThat(connector.connectWithRetry()).isSameAs(connection);
        assertThat(sleeps).isEmpty();
    }

    @Test
    void retriesWithDoublingBackoffUntilTheDatabaseIsUp() throws Exception {
        Connection connection = fakeConnection();
        DatabaseConnector connector = connectorThatYields(
                new SQLException("down", "08001"),
                new SQLException("down", "08001"),
                new SQLException("down", "08001"),
                connection);

        assertThat(connector.connectWithRetry()).isSameAs(connection);
        assertThat(sleeps).containsExactly(1_000L, 2_000L, 4_000L);
    }

    @Test
    void backoffIsCappedAtTheMaximum() throws Exception {
        Connection connection = fakeConnection();
        Object[] outcomes = new Object[8];
        for (int i = 0; i < 7; i++) {
            outcomes[i] = new SQLException("down", "08001");
        }
        outcomes[7] = connection;

        connectorThatYields(outcomes).connectWithRetry();

        assertThat(sleeps)
                .containsExactly(1_000L, 2_000L, 4_000L, 8_000L, 16_000L, 30_000L, 30_000L);
    }

    @Test
    void missingPasswordFailsFastWithoutRetrying() {
        DatabaseConnector connector =
                connectorThatYields(new SQLException("no password supplied", "08004"));

        assertThatThrownBy(connector::connectWithRetry)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("08004")
                .hasMessageContaining("DB_PASSWORD");
        assertThat(attempts).hasValue(1);
        assertThat(sleeps).isEmpty();
    }

    @Test
    void wrongPasswordFailsFastWithoutRetrying() {
        SQLException rejection = new SQLException("password authentication failed", "28P01");
        DatabaseConnector connector = connectorThatYields(rejection);

        assertThatThrownBy(connector::connectWithRetry)
                .isInstanceOf(IllegalStateException.class)
                .hasCause(rejection);
        assertThat(attempts).hasValue(1);
        assertThat(sleeps).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"08001", "08006", "53300", "57P01"})
    void transientFailuresAreRetried(String sqlState) throws Exception {
        Connection connection = fakeConnection();
        DatabaseConnector connector =
                connectorThatYields(new SQLException("down", sqlState), connection);

        assertThat(connector.connectWithRetry()).isSameAs(connection);
        assertThat(sleeps).containsExactly(1_000L);
    }

    @Test
    void shutdownDuringBackoffPropagatesTheInterruption() {
        DatabaseConnector connector = new DatabaseConnector(
                () -> {
                    throw new SQLException("down", "08001");
                },
                millis -> {
                    throw new InterruptedException("shutdown");
                },
                1_000, 30_000, 2);

        assertThatThrownBy(connector::connectWithRetry).isInstanceOf(InterruptedException.class);
    }

    private DatabaseConnector connectorThatYields(Object... outcomes) {
        Deque<Object> script = new ArrayDeque<>(List.of(outcomes));
        return new DatabaseConnector(
                () -> {
                    attempts.incrementAndGet();
                    Object next = script.removeFirst();
                    if (next instanceof SQLException failure) {
                        throw failure;
                    }
                    return (Connection) next;
                },
                sleeps::add,
                1_000, 30_000, 2);
    }

    private static Connection fakeConnection() {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> method.getName().equals("toString") ? "fakeConnection" : null);
    }
}