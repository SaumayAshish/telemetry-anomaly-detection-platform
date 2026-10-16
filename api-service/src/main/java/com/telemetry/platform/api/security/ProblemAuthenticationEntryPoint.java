package com.telemetry.platform.api.security;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.server.resource.BearerTokenErrorCodes;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class ProblemAuthenticationEntryPoint implements AuthenticationEntryPoint {

    private static final String REALM = "Bearer realm=\"telemetry\"";

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response,
                         AuthenticationException authException) throws IOException {

        response.setHeader("WWW-Authenticate", challengeFor(authException));
        ProblemJson.write(response, 401, "Unauthorized",
                "Authentication is required to access this resource.", "UNAUTHENTICATED");
    }

    // RFC 6750: error="invalid_token" only when a token was presented and rejected.
    // A request without a token gets the bare challenge. The decoder's own message is
    // not echoed, because it carries timestamps and decoder internals.
    private static String challengeFor(AuthenticationException exception) {
        if (exception instanceof OAuth2AuthenticationException oauth
                && BearerTokenErrorCodes.INVALID_TOKEN.equals(oauth.getError().getErrorCode())) {
            return REALM + ", error=\"invalid_token\"";
        }
        return REALM;
    }
}
