package com.telemetry.platform.api.security;

import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

import java.security.interfaces.RSAPublicKey;

public final class JwtDecoderFactory {

    private JwtDecoderFactory() {
    }

    // Verifies the signature (RS256 with the issuer's public key), the expiry
    // and not-before times, and that the token names the expected issuer.
    public static JwtDecoder create(RSAPublicKey publicKey, String expectedIssuer) {
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withPublicKey(publicKey).build();
        decoder.setJwtValidator(JwtValidators.createDefaultWithIssuer(expectedIssuer));
        return decoder;
    }
}
