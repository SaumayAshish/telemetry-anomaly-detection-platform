package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.net.ServerSocket;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Records which SQLState the real PostgreSQL driver reports for each way a connection
 * can fail, and what SqlFailureClassifier does with it. If the driver changes, these
 * tests say so before production does.
 */
@Testcontainers
class DriverSqlStateContractTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16");

    @Test
    void wrongPasswordIs28P01AndFailsFast() {
        SQLException failure = connectionFailure(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), "wrong-password");

        assertThat(failure.getSQLState()).isEqualTo("28P01");
        assertThat(SqlFailureClassifier.isFatalConnectFailure(failure.getSQLState())).isTrue();
    }

    @Test
    void unknownUserIsIndistinguishableFromAWrongPassword() {
        SQLException failure = connectionFailure(POSTGRES.getJdbcUrl(), "nobody", "x");

        assertThat(failure.getSQLState()).isEqualTo("28P01");
        assertThat(SqlFailureClassifier.isFatalConnectFailure(failure.getSQLState())).isTrue();
    }

    @Test
    void absentPasswordIs08004AndFailsFast() {
        SQLException failure = connectionFailure(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), null);

        assertThat(failure.getSQLState()).isEqualTo("08004");
        assertThat(SqlFailureClassifier.isFatalConnectFailure(failure.getSQLState())).isTrue();
    }

    @Test
    void emptyPasswordAlsoFailsFast() {
        SQLException failure = connectionFailure(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), "");

        assertThat(SqlFailureClassifier.isFatalConnectFailure(failure.getSQLState())).isTrue();
    }

    // Documents current behavior, not an endorsement: a wrong database name is a
    // A typo in DB_URL is a configuration error: waiting cannot create the database.
    @Test
    void nonexistentDatabaseIs3D000AndIsFatal() {
        String url = POSTGRES.getJdbcUrl()
                .replace("/" + POSTGRES.getDatabaseName() + "?", "/no_such_db?");

        SQLException failure = connectionFailure(url, POSTGRES.getUsername(), POSTGRES.getPassword());

        assertThat(failure.getSQLState()).isEqualTo("3D000");
        assertThat(SqlFailureClassifier.isFatalConnectFailure(failure.getSQLState())).isTrue();
    }

    @Test
    void nothingListeningIs08001AndIsRetried() throws Exception {
        int unusedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            unusedPort = socket.getLocalPort();
        }

        SQLException failure = connectionFailure(
                "jdbc:postgresql://localhost:" + unusedPort + "/test", "u", "p");

        assertThat(failure.getSQLState()).isEqualTo("08001");
        assertThat(SqlFailureClassifier.isFatalConnectFailure(failure.getSQLState())).isFalse();
    }

    private static SQLException connectionFailure(String url, String user, String password) {
        Throwable thrown = catchThrowable(() -> DriverManager.getConnection(url, user, password).close());

        assertThat(thrown).as("the connection attempt should have failed").isInstanceOf(SQLException.class);
        return (SQLException) thrown;
    }
}
