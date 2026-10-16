package com.telemetry.platform.alerting;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A scriptable stand-in for a JDBC database: every call on it succeeds with a harmless
 * default unless a test queues a different outcome. It records what the code under test did
 * (connections opened and closed, inserts attempted), so tests can assert on behaviour
 * without a real database.
 */
final class FakeDatabase {

    private static final Object UNHANDLED = new Object();

    private final Deque<Object> alertInserts = new ArrayDeque<>();
    private final Deque<Object> deadLetterInserts = new ArrayDeque<>();
    private final Deque<Boolean> validityChecks = new ArrayDeque<>();

    private final AtomicInteger connects = new AtomicInteger();
    private final AtomicInteger alertInsertAttempts = new AtomicInteger();
    private final AtomicInteger deadLetterInsertAttempts = new AtomicInteger();
    private final AtomicInteger closedStatements = new AtomicInteger();
    private final AtomicInteger validityChecksMade = new AtomicInteger();
    private final List<Integer> closedConnections = new ArrayList<>();

    /** The next alert insert reports this many rows (1 = inserted, 0 = ON CONFLICT ignored it). */
    void alertInsertReturns(int rows) {
        alertInserts.add(rows);
    }

    void alertInsertThrows(SQLException failure) {
        alertInserts.add(failure);
    }

    void deadLetterInsertReturns(int rows) {
        deadLetterInserts.add(rows);
    }

    void deadLetterInsertThrows(SQLException failure) {
        deadLetterInserts.add(failure);
    }

    /** The next isValid() call answers false (a stale connection). */
    void nextValidityCheckFails() {
        validityChecks.add(false);
    }

    AlertWriter.ConnectionSource source() {
        return this::newConnection;
    }

    int connectCount() {
        return connects.get();
    }

    int alertInsertAttempts() {
        return alertInsertAttempts.get();
    }

    int deadLetterInsertAttempts() {
        return deadLetterInsertAttempts.get();
    }

    int closedStatements() {
        return closedStatements.get();
    }

    int validityChecksMade() {
        return validityChecksMade.get();
    }

    /** Ids (1-based, in opening order) of the connections that were closed. */
    List<Integer> closedConnections() {
        return List.copyOf(closedConnections);
    }

    private Connection newConnection() {
        int id = connects.incrementAndGet();

        return proxy(Connection.class, "fakeConnection#" + id, (method, args) -> {
            switch (method.getName()) {
                case "isValid":
                    validityChecksMade.incrementAndGet();
                    return validityChecks.isEmpty() || validityChecks.removeFirst();
                case "prepareStatement":
                    return newStatement((String) args[0]);
                case "close":
                    closedConnections.add(id);
                    return null;
                default:
                    return UNHANDLED;
            }
        });
    }

    private PreparedStatement newStatement(String sql) {
        boolean isAlert = sql.equals(AlertStore.INSERT_ALERT_SQL);
        Deque<Object> script = isAlert ? alertInserts : deadLetterInserts;
        AtomicInteger attempts = isAlert ? alertInsertAttempts : deadLetterInsertAttempts;

        return proxy(PreparedStatement.class, isAlert ? "alertStatement" : "deadLetterStatement",
                (method, args) -> {
                    switch (method.getName()) {
                        case "executeUpdate":
                            attempts.incrementAndGet();
                            Object next = script.isEmpty() ? 1 : script.removeFirst();
                            if (next instanceof SQLException failure) {
                                throw failure;
                            }
                            return next;
                        case "close":
                            closedStatements.incrementAndGet();
                            return null;
                        default:
                            return UNHANDLED;
                    }
                });
    }

    @FunctionalInterface
    private interface Handler {
        Object handle(Method method, Object[] args) throws Throwable;
    }

    private static <T> T proxy(Class<T> type, String name, Handler handler) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> name;
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> null;
                        };
                    }

                    Object result = handler.handle(method, args == null ? new Object[0] : args);
                    return result == UNHANDLED ? defaultValue(method.getReturnType()) : result;
                }));
    }

    /** What a method returns when nobody scripted it: zero, false, or null. */
    private static Object defaultValue(Class<?> type) {
        if (type == void.class || !type.isPrimitive()) {
            return null;
        }
        return Array.get(Array.newInstance(type, 1), 0);
    }
}
