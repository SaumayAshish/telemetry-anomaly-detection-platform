package com.telemetry.platform.alerting;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class SqlFailureClassifierTest {

    private static SQLException withState(String sqlState) {
        return new SQLException("test", sqlState);
    }

    @Test
    void connectionResourceAndOperatorFailuresAreRetryable() {
        for (String state : new String[]{"08001", "08006", "53300", "57P01"}) {
            assertThat(SqlFailureClassifier.isRetryable(withState(state)))
                    .as("SQLState %s", state).isTrue();
        }
    }

    @Test
    void dataConstraintSyntaxAndAuthFailuresArePermanent() {
        for (String state : new String[]{"23514", "23505", "22001", "42601", "28P01"}) {
            assertThat(SqlFailureClassifier.isRetryable(withState(state)))
                    .as("SQLState %s", state).isFalse();
        }
    }

    @Test
    void missingOrTooShortSqlStateIsTreatedAsPermanent() {
        for (String state : new String[]{null, "", "0"}) {
            assertThat(SqlFailureClassifier.isRetryable(withState(state)))
                    .as("SQLState %s", state).isFalse();
        }
    }

    @Test
    void onlyClass28IsACredentialRejection() {
        assertThat(SqlFailureClassifier.isCredentialRejection("28P01")).isTrue();
        assertThat(SqlFailureClassifier.isCredentialRejection("28000")).isTrue();
        assertThat(SqlFailureClassifier.isCredentialRejection("08004")).isFalse();
        assertThat(SqlFailureClassifier.isCredentialRejection("23514")).isFalse();
        assertThat(SqlFailureClassifier.isCredentialRejection(null)).isFalse();
    }
    @Test
    void aRejectedConnectionIsNotRetryableButOtherConnectionFailuresStillAre() {
        assertThat(SqlFailureClassifier.isRetryable(withState("08004"))).isFalse();

        assertThat(SqlFailureClassifier.isRetryable(withState("08001"))).isTrue();
        assertThat(SqlFailureClassifier.isRetryable(withState("08006"))).isTrue();
    }
    @Test
    void fatalConnectFailuresAreBadCredentialsAndRejectedConnections() {
        assertThat(SqlFailureClassifier.isFatalConnectFailure("28P01")).isTrue();
        assertThat(SqlFailureClassifier.isFatalConnectFailure("08004")).isTrue();

        assertThat(SqlFailureClassifier.isFatalConnectFailure("08001")).isFalse();
        assertThat(SqlFailureClassifier.isFatalConnectFailure("08006")).isFalse();
        assertThat(SqlFailureClassifier.isFatalConnectFailure(null)).isFalse();
    }
}