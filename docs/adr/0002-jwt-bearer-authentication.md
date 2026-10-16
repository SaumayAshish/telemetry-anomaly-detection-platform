# ADR 0002: API Authentication with RS256 Bearer JWTs

## Status

Accepted

## Context

api-service serves alert data over HTTP and must only answer callers that have proven who they are and what they may read. The service is stateless, and it must not hold anything that would let an attacker who compromises it forge credentials.

## Decision

### 1. Token format and verification

Callers send `Authorization: Bearer <JWT>`. Tokens are signed with RS256. api-service holds only the public key: a PEM file whose path is given by `app.security.jwt.public-key-path` (environment variable `APP_SECURITY_JWT_PUBLIC_KEY_PATH`), read once at startup. A key that cannot be read or parsed stops the service from starting.

The decoder checks the signature, the expiry and not-before times, and that the issuer equals `app.security.jwt.issuer`.

### 2. Authorization

Roles come from the `roles` claim and are mapped to `ROLE_` authorities. `/api/alerts/**` requires `ALERT_READER`. `/actuator/health` is public. Every other path requires authentication.

### 3. Statelessness

There are no sessions and no cookies. CSRF protection is off because the API accepts only bearer tokens and only GET requests. HTTP Basic is not accepted.

### 4. Failure responses

Rejections use RFC 9457 problem+json with a stable `code` (`UNAUTHENTICATED` for 401, `FORBIDDEN` for 403) and the correlation id. A 401 carries `WWW-Authenticate: Bearer realm="telemetry"` and adds `error="invalid_token"` when a token was presented and rejected (RFC 6750), so a client can tell "log in" from "your token is bad". The decoder's own message is never returned to the client.

### 5. Issuing tokens

Issuing is outside api-service. For development, `DevTokenTool` (in the test sources) creates a throwaway key pair under `dev-keys/` (git-ignored) and signs one-hour tokens.

## Alternatives considered

- HS256 with a shared secret: every verifier would hold the key that can mint tokens, so compromising the API would allow forging tokens.
- Opaque tokens with introspection: adds a network dependency and a call per request.
- Server-side sessions: contradicts the stateless design.

## Consequences

- There is one static public key. Rotating it means redeploying with a new key; serving a JWKS set and rotating keys is not done.
- A valid token cannot be revoked before it expires, so lifetimes must stay short.
- Someone has to run the issuer. This repository only contains the development tool.
- In the compose pipeline the public key reaches the container through a named volume written by the `jwt-key-init` job, because bind-mounting project files does not work with the development Docker Desktop setup.
