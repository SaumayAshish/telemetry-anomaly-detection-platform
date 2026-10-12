package com.telemetry.platform.alerting;

import java.sql.SQLException;
import java.util.Set;

/** Decides how the consumer reacts to a database failure, from its SQLState alone. */
final class SqlFailureClassifier {

    /* 08 connection exception, 53 insufficient resources, 57 operator intervention */
    private static final Set<String> RETRYABLE_SQLSTATE_CLASSES = Set.of("08", "53", "57");

    private SqlFailureClassifier() {}

    private static final String CONNECTION_REJECTED = "08004";

    static boolean isRetryable(SQLException e) {
        String sqlState = e.getSQLState();
        if (sqlState == null || sqlState.length() < 2) {
            return false;
        }
        if (CONNECTION_REJECTED.equals(sqlState)) {
            return false;   // server refused the connection (e.g. no password supplied): config, not weather
        }
        return RETRYABLE_SQLSTATE_CLASSES.contains(sqlState.substring(0, 2));
    }

    /** Connect failures where waiting cannot help: bad credentials, or a connection the server rejected. */
    static boolean isFatalConnectFailure(String sqlState) {
        return isCredentialRejection(sqlState) || CONNECTION_REJECTED.equals(sqlState);
    }    /** Class 28 = invalid authorization: retrying a wrong password only hammers the server. */
    static boolean isCredentialRejection(String sqlState) {
        return sqlState != null && sqlState.startsWith("28");
    }
}