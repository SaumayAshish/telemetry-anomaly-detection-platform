package com.telemetry.platform.api.security;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.List;

/**
 * Development tool, not part of the application. Run its main method from the IDE.
 * Writes a key pair (only if none exists) and three sample tokens into dev-keys/,
 * a folder that is git-ignored because it holds a private key.
 */
public final class DevTokenTool {

    private static final Path KEY_DIR = Path.of("dev-keys");
    private static final String ISSUER = "telemetry-dev-issuer";

    private DevTokenTool() {
    }

    public static void main(String[] args) throws Exception {
        Files.createDirectories(KEY_DIR);

        Path publicKeyFile = KEY_DIR.resolve("public.pem");
        Path privateKeyFile = KEY_DIR.resolve("private.pem");

        if (!Files.exists(privateKeyFile)) {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            KeyPair pair = generator.generateKeyPair();
            write(publicKeyFile, "PUBLIC KEY", pair.getPublic().getEncoded());
            write(privateKeyFile, "PRIVATE KEY", pair.getPrivate().getEncoded());
        }

        PrivateKey privateKey = readPrivateKey(privateKeyFile);

        Instant now = Instant.now();
        write(KEY_DIR.resolve("token-reader.txt"), sign(privateKey, List.of("ALERT_READER"), now.plusSeconds(3600)));
        write(KEY_DIR.resolve("token-guest.txt"), sign(privateKey, List.of("GUEST"), now.plusSeconds(3600)));
        write(KEY_DIR.resolve("token-expired.txt"), sign(privateKey, List.of("ALERT_READER"), now.minusSeconds(3600)));

        System.out.println("Wrote key pair and tokens to " + KEY_DIR.toAbsolutePath());
        System.out.println("Public key for APP_SECURITY_JWT_PUBLIC_KEY_PATH: " + publicKeyFile.toAbsolutePath());
    }

    private static String sign(PrivateKey key, List<String> roles, Instant expiresAt) throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("dev-user")
                .issuer(ISSUER)
                .issueTime(Date.from(Instant.now()))
                .expirationTime(Date.from(expiresAt))
                .claim("roles", roles)
                .build();

        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).build(), claims);
        jwt.sign(new RSASSASigner(key));
        return jwt.serialize();
    }

    private static PrivateKey readPrivateKey(Path file) throws Exception {
        String base64 = Files.readString(file, StandardCharsets.US_ASCII)
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        return KeyFactory.getInstance("RSA")
                .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(base64)));
    }

    private static void write(Path file, String pemType, byte[] der) throws Exception {
        String body = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der);
        write(file, "-----BEGIN " + pemType + "-----\n" + body + "\n-----END " + pemType + "-----\n");
    }

    private static void write(Path file, String content) throws Exception {
        Files.writeString(file, content, StandardCharsets.US_ASCII);
    }
}