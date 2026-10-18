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
- Decision records in `docs/adr/`: 0001 topic and partition key, 0002 JWT bearer authentication, 0003 alerting failure handling, 0004 idempotency key, 0005 baseline hysteresis and rebaselining.

## Requirements
JDK 26, Docker. Maven is provided by the wrapper (`./mvnw`).

## Run locally
```bash
docker compose up -d                 # Kafka + TimescaleDB (host port 5433)
./mvnw -B clean verify               # build and test everything
```
`alerting-service` needs `KAFKA_BOOTSTRAP_SERVERS`, `DB_URL`, `DB_USER`, `DB_PASSWORD`.
It also reads an optional `DLQ_FALLBACK_PATH` (default `dlq-fallback.jsonl`). `ingestion-service` and `stream-processing-service` read `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`).
`api-service` needs `APP_SECURITY_JWT_PUBLIC_KEY_PATH` (PEM public key); run with `--spring.profiles.active=dev`.
The dev token tool lives in `api-service/src/test/.../DevTokenTool.java`; its keys are throwaway and git-ignored.

## Run the whole pipeline in containers

The `pipeline` profile builds `ingestion`, `stream-processing`, `alerting` and `api` from one `Dockerfile` and starts them behind three one-shot jobs: `migrate` (the Flyway schema from `db-migrations`), `seed-sensors` (the five simulated sensors, dev only) and `jwt-key-init` (writes the JWT public key into a volume).

```bash
docker compose down                      # the dev stack and the pipeline use the same container names
export JWT_PUBLIC_KEY_BASE64=$(grep -v '^-----' dev-keys/public.pem | tr -d '\r\n')
docker compose -p telemetry-pipeline --profile pipeline up -d --build
```

- Use the project name `telemetry-pipeline`: the default project's volumes hold the hand-built dev database, which has no Flyway history, so `migrate` would refuse to run against it.
- `dev-keys/public.pem` comes from running `DevTokenTool` once; it also writes one-hour tokens next to it.
- The simulated readings stay inside the normal range, so no alert appears by itself. Give the stream app a couple of minutes to collect its first 30 readings per sensor, then send one outlier:

```bash
echo "S-1001|{\"sensorId\":\"S-1001\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"metricType\":\"temperature\",\"value\":500.0,\"sequenceNumber\":999999}" | docker compose -p telemetry-pipeline exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic telemetry.sensor.readings.v1 --property parse.key=true --property "key.separator=|"
```

- Clean up with `docker compose -p telemetry-pipeline --profile pipeline down -v`.

## Metrics

The alerting service serves Prometheus text at `http://127.0.0.1:9404/metrics` when run with the `pipeline` profile. The port is published on this machine only, and the endpoint has no authentication. `METRICS_PORT` changes the port; `METRICS_PORT=off` turns the endpoint off.

| Metric | Labels | Counts |
| --- | --- | --- |
| `alerting_alerts_persisted_total` | `severity` | Alerts written to the database |
| `alerting_alerts_duplicate_total` | none | Alerts already stored, skipped by the idempotent insert |
| `alerting_dead_letters_total` | `reason`: `DESERIALIZATION`, `VALIDATION`, `INSERT_REJECTED` | Records sent to the dead-letter table |
| `alerting_commits_lost_total` | none | Offset commits lost to a consumer-group rebalance |
| `alerting_db_retries_total` | `operation`: `persisting_alert`, `writing_dead_letter` | Retries after a transient database error; the attempt that gives up is not counted |

Labelled counters appear after their first event; the unlabelled ones read `0.0` from startup.

```
curl.exe http://127.0.0.1:9404/metrics
```

api-service serves `/actuator/prometheus` on port 8080, for tokens with the `METRICS_READER` role only. `DevTokenTool` also writes a one-hour `dev-keys/token-metrics.txt` for it; send it as the bearer token.

## API

`GET /api/alerts` (role `ALERT_READER`) takes optional `sensorId`, `severity`, and a `from`/`to` pair (ISO-8601 instants, not combined with the other filters), plus `page` (default 0) and `size` (default 50, 1 to 200). Results are newest first, the body is a JSON array and the total number of matches is in the `X-Total-Count` header. A database outage answers `503` with `Retry-After`; a rejected token gets `401` with `error="invalid_token"`.

## Status
Core pipeline, persistence, resilience, API security and paging are complete. CI runs the unit and Testcontainers tests, and the whole pipeline runs from `docker compose`. Not done yet: metrics and tracing, a realistic data generator, JWKS key rotation. See `docs/adr/` for the decisions.