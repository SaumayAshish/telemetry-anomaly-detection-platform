package com.telemetry.platform.api.security;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DevTokenToolTest {

    private static final Instant NOW = Instant.parse("2026-10-17T18:00:00Z");

    private static KeyPair pair;

    @BeforeAll
    static void generateKeyPair() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        pair = generator.generateKeyPair();
    }

    @Test
    void issuesTheFourTokenFiles() throws Exception {
        assertThat(DevTokenTool.tokenFiles(pair.getPrivate(), NOW)).containsOnlyKeys(
                "token-reader.txt", "token-guest.txt", "token-expired.txt", "token-metrics.txt");
    }

    @Test
    void theMetricsTokenCarriesOnlyTheMetricsReaderRole() throws Exception {
        assertThat(claims("token-metrics.txt").getStringListClaim("roles"))
                .containsExactly("METRICS_READER");
    }

    @Test
    void theReaderTokenStaysAnAlertReader() throws Exception {
        assertThat(claims("token-reader.txt").getStringListClaim("roles"))
                .containsExactly("ALERT_READER");
    }

    @Test
    void theGuestTokenStaysAGuest() throws Exception {
        assertThat(claims("token-guest.txt").getStringListClaim("roles"))
                .containsExactly("GUEST");
    }

    @Test
    void everyTokenIsSignedByTheDevKey() throws Exception {
        Map<String, String> all = DevTokenTool.tokenFiles(pair.getPrivate(), NOW);
        assertThat(all).hasSize(4);
        RSASSAVerifier verifier = new RSASSAVerifier((RSAPublicKey) pair.getPublic());
        for (String token : all.values()) {
            assertThat(SignedJWT.parse(token).verify(verifier)).isTrue();
        }
    }

    @Test
    void liveTokensLastAnHourAndTheExpiredOneAlreadyEnded() throws Exception {
        assertThat(claims("token-metrics.txt").getExpirationTime().toInstant()).isEqualTo(NOW.plusSeconds(3600));
        assertThat(claims("token-reader.txt").getExpirationTime().toInstant()).isEqualTo(NOW.plusSeconds(3600));
        assertThat(claims("token-guest.txt").getExpirationTime().toInstant()).isEqualTo(NOW.plusSeconds(3600));
        assertThat(claims("token-expired.txt").getExpirationTime().toInstant()).isEqualTo(NOW.minusSeconds(3600));
    }

    private static String token(String file) throws Exception {
        Map<String, String> all = DevTokenTool.tokenFiles(pair.getPrivate(), NOW);
        assertThat(all).containsKey(file);
        return all.get(file);
    }

    private static JWTClaimsSet claims(String file) throws Exception {
        return SignedJWT.parse(token(file)).getJWTClaimsSet();
    }
}
