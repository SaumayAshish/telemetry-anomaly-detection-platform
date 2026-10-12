# Real-Time Telemetry Anomaly Detection Platform

Streaming platform that ingests sensor telemetry, flags statistical anomalies in
real time, persists alerts durably, and serves them through a secured REST API.

![CI](https://github.com/SaumayAshish/telemetry-anomaly-detection-platform/actions/workflows/ci.yml/badge.svg)

## Architecture

```
 ingestion-service        stream-processing-service        alerting-service          api-service
 (Kafka producer)         (Kafka Streams)                  (Kafka consumer)          (Spring Boot)
        |                         |                               |                       |
        |  telemetry.sensor       |  telemetry.sensor             |                       |
        +--- .readings.v1 ------->+--- .anomalies.v1 ------------>+--- INSERT ---+       |
                                  |                               |              v       |
                       windowed z-score,                   retry + backoff   +----------+ |
                       Welford variance,                   idempotent insert | Postgres |<+ SELECT
                       hysteresis rebaseline               dead-letter table | (alerts) |   JWT (RS256)
                                                                             +----------+   RFC 9457 errors
```

| Module | Role |
|---|---|
| `common-models` | Shared event types (`AnomalyEvent`, `AnomalySeverity`) |
| `ingestion-service` | Produces sensor readings, keyed by sensor id (see ADR 0001) |
| `stream-processing-service` | Rolling per-sensor statistics; emits an event when z-score exceeds the threshold |
| `alerting-service` | Persists alerts; transient DB failures retried, permanent ones dead-lettered; duplicates ignored on redelivery |
| `api-service` | `GET /api/alerts` with filters; bearer-JWT auth, role `ALERT_READER`; correlation id on every response |

## Key design decisions
- Partition key = sensor id, so one sensor's readings stay ordered: `docs/adr/0001-telemetry-topic-partition-key.md`
- Baseline is not updated by anomalous readings (hysteresis) so outliers cannot hide themselves.
- Idempotency key = Kafka `(partition, offset)`; redelivery never creates duplicate rows.
- Logging rules and what must never be logged: `docs/logging-policy.md`

## Requirements
JDK 26, Docker. Maven is provided by the wrapper (`./mvnw`).

## Run locally
```bash
docker compose up -d                 # Kafka + TimescaleDB (host port 5433)
./mvnw -B clean verify               # build and test everything
```
`alerting-service` needs `KAFKA_BOOTSTRAP_SERVERS`, `DB_URL`, `DB_USER`, `DB_PASSWORD`.
`api-service` needs `APP_SECURITY_JWT_PUBLIC_KEY_PATH` (PEM public key); run with `--spring.profiles.active=dev`.
The dev token tool lives in `api-service/src/test/.../DevTokenTool.java`; its keys are throwaway and git-ignored.

## Status
Core pipeline, persistence, resilience and API security complete. In progress: CI, container images,
Testcontainers integration tests, metrics. See `docs/` for decisions.