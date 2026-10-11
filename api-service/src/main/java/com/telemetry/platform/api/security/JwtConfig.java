package com.telemetry.platform.api.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.jwt.JwtDecoder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

@Configuration
public class JwtConfig {

    @Bean
    JwtDecoder jwtDecoder(@Value("${app.security.jwt.public-key-path}") String publicKeyPath,
                          @Value("${app.security.jwt.issuer}") String issuer) {
        return JwtDecoderFactory.create(readPublicKey(Path.of(publicKeyPath)), issuer);
    }

    static RSAPublicKey readPublicKey(Path path) {
        try {
            String pem = Files.readString(path, StandardCharsets.US_ASCII);
            String base64 = pem
                    .replace("-----BEGIN PUBLIC KEY-----", "")
                    .replace("-----END PUBLIC KEY-----", "")
                    .replaceAll("\\s", "");
            byte[] der = Base64.getDecoder().decode(base64);
            return (RSAPublicKey) KeyFactory.getInstance("RSA")
                    .generatePublic(new X509EncodedKeySpec(der));
        } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalStateException("Could not read an RSA public key (PEM, 'PUBLIC KEY' format) from "
                    + path + ": " + e.getClass().getSimpleName(), e);
        }
    }
}