package com.telemetry.platform.api.security;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.oauth2.jwt.JwtDecoder;

import java.time.Instant;
import java.util.Date;
import java.util.List;

public final class TestJwtSupport {

    public static final String ISSUER = "telemetry-test-issuer";
    public static final RSAKey SIGNING_KEY = newKey("test-key");
    public static final RSAKey OTHER_KEY = newKey("other-key");

    private TestJwtSupport() {
    }

    @TestConfiguration
    public static class Config {
        @Bean
        JwtDecoder jwtDecoder() throws JOSEException {
            return JwtDecoderFactory.create(SIGNING_KEY.toRSAPublicKey(), ISSUER);
        }
    }

    public static String token(RSAKey key, String issuer, List<String> roles, Instant expiresAt) {
        try {
            JWTClaimsSet.Builder claims = new JWTClaimsSet.Builder()
                    .subject("reader")
                    .issuer(issuer)
                    .issueTime(Date.from(Instant.now()))
                    .expirationTime(Date.from(expiresAt));

            if (roles != null) {
                claims.claim("roles", roles);
            }

            SignedJWT jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(),
                    claims.build());
            jwt.sign(new RSASSASigner(key));
            return jwt.serialize();
        } catch (JOSEException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String validToken(List<String> roles) {
        return token(SIGNING_KEY, ISSUER, roles, Instant.now().plusSeconds(3600));
    }

    private static RSAKey newKey(String id) {
        try {
            return new RSAKeyGenerator(2048).keyID(id).generate();
        } catch (JOSEException e) {
            throw new IllegalStateException(e);
        }
    }
}