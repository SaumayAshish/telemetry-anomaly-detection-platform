# Real-Time Telemetry Anomaly Detection Platform

![CI](https://github.com/SaumayAshish/telemetry-anomaly-detection-platform/actions/workflows/ci.yml/badge.svg)

Streaming platform that ingests sensor telemetry, flags statistical anomalies in real time, persists alerts durably, and serves them through a secured REST API. Java 26, Spring Boot 4.1, Kafka, TimescaleDB.

> **On a seeded 5-sensor scenario with 10 injected faults, the detector caught 9, missed 1 (a +2.5σ spike, below its 3σ threshold), and raised 9 false positives in 2,584 quiet readings (0.35%, against 0.27% expected from noise alone). Zero dead letters.** Every number below is measured from a real run of the containerised pipeline and can be reproduced with one command.

## What it does

- **Detects anomalies per sensor** with a streaming Welford baseline (running mean and variance) and a z-score threshold of 3. Scoring starts after 30 samples.
- **Resists self-masking.** Anomalous readings never update the baseline (hysteresis), so an outlier cannot hide itself. After 10 consecutive anomalies the baseline resets to the new level, so a permanent shift produces exactly 10 alerts instead of an endless stream.
- **Grades severity** from the z-score: LOW 3 to <5, MEDIUM 5 to <10, HIGH 10 to <15, CRITICAL 15 and above.
- **Never loses or duplicates an alert.** Transient database errors are retried with backoff, permanent failures go to a dead-letter table, and the Kafka `(partition, offset)` pair is the idempotency key, so redelivery cannot create a second row.
- **Serves alerts securely.** `GET /api/alerts` with filters and paging, RS256 bearer JWTs, role-based access, RFC 9457 error bodies and a correlation id on every response.
- **Proves itself against ground truth.** A seeded, stateless simulator injects spikes, level shifts and drifts at known timestamps and writes a `ground-truth.csv`, so detection can be scored rather than eyeballed.

## Sample output

Five sensors, 600 readings each, ten injected faults. Alerts are dots, shaded and sized by severity; the orange wash marks where faults were injected. Click any image for full size.

[![Five sensor panels with fault windows and alerts, 9 of 10 faults detected](https://raw.githubusercontent.com/SaumayAshish/telemetry-anomaly-detection-platform/main/docs/images/detection-overview.png)](https://github.com/SaumayAshish/telemetry-anomaly-detection-platform/blob/main/docs/images/detection-overview.png)

[![Bar chart of alerts raised per injected fault](https://raw.githubusercontent.com/SaumayAshish/telemetry-anomaly-detection-platform/main/docs/images/detection-scorecard.png)](https://github.com/SaumayAshish/telemetry-anomaly-detection-platform/blob/main/docs/images/detection-scorecard.png)

[![Z-score trace for sensor S-1001 showing the spike, the level shift and the rebaseline](https://raw.githubusercontent.com/SaumayAshish/telemetry-anomaly-detection-platform/main/docs/images/zscore-trace-s1001.png)](https://github.com/SaumayAshish/telemetry-anomaly-detection-platform/blob/main/docs/images/zscore-trace-s1001.png)

### Measured results

| Fault | Magnitude | Alerts | First alert |
|---|---|---|---|
| S-1001 spike | +12σ | 1 (HIGH) | first reading |
| S-1001 level shift | +6σ | 10 | first reading |
| S-1002 spike | −8σ | 1 (HIGH) | first reading |
| S-1002 drift | +6σ | 18 | 7.5 s in |
| S-1003 spike | +20σ | 1 (CRITICAL) | first reading |
| S-1003 level shift | −5σ | 10 | first reading |
| S-1004 level shift | +8σ | 10 | first reading |
| S-1004 spike | +4σ | 1 (LOW) | first reading |
| S-1005 drift | +5σ | 17 | 8 s in |
| S-1005 spike | +2.5σ | 0 | **missed** |

- **The miss is correct behaviour.** A 2.5σ excursion is below the 3σ threshold by design.
- **Drifts are caught late by nature.** The ramp must pass about 3σ before it is an anomaly.
- **False positives match the maths.** 9 alerts, all LOW, outside any fault window, across 2,584 quiet scored readings: 0.348%. Gaussian noise alone gives 0.27% at |z| > 3, about 7 expected.
- **Replay check.** An independent Python re-implementation of the detector reproduces all 78 stored alerts exactly. The raw run (`readings.jsonl`, `alerts.csv`, `ground-truth.csv`, `metrics.txt`) and the scoring and plotting scripts (`analyze.py`, `figs.py`; Python with pandas and matplotlib) are in `docs/demo/`.
- **Pipeline health.** 3,000 readings in, 78 alerts persisted (LOW 50, MEDIUM 24, HIGH 3, CRITICAL 1), 0 duplicates, 0 lost commits, 0 dead letters.

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
| `db-migrations` | Flyway schema for the alerts and dead-letter tables |
| `ingestion-service` | Produces sensor readings keyed by sensor id (ADR 0001); `ReadingSimulator` and `DemoScenario` generate the seeded fault scenario |
| `stream-processing-service` | Rolling per-sensor statistics; emits an event when the z-score exceeds the threshold |
| `alerting-service` | Persists alerts; transient DB failures retried, permanent ones dead-lettered; duplicates ignored on redelivery |
| `api-service` | `GET /api/alerts` with filters; bearer-JWT auth, role `ALERT_READER`; correlation id on every response |

### Key design decisions

- Partition key = sensor id, so one sensor's readings stay ordered: `docs/adr/0001-telemetry-topic-partition-key.md`.
- Baseline is not updated by anomalous readings (hysteresis), so outliers cannot hide themselves.
- Idempotency key = Kafka `(partition, offset)`; redelivery never creates duplicate rows.
- Logging rules and what must never be logged: `docs/logging-policy.md`.
- Decision records in `docs/adr/`: 0001 topic and partition key, 0002 JWT bearer authentication, 0003 alerting failure handling, 0004 idempotency key, 0005 baseline hysteresis and rebaselining.

## Getting started

Requirements: JDK 26 and Docker. Maven comes from the wrapper (`./mvnw`).

### Reproduce the results above

The demo runs in its own Compose project with fresh volumes. It is separate from the dev stack, but both use the same container names, so stop the dev stack first (without `-v`).

```bash
docker compose down
export JWT_PUBLIC_KEY_BASE64=$(grep -v '^-----' dev-keys/public.pem | tr -d '\r\n')
docker compose -p telemetry-demo --profile pipeline --profile demo up -d --build
```

`ingestion-demo` replays the 600-tick scenario at 500 ms per tick (about five minutes), writes `/data/ground-truth.csv`, then exits. When it has exited:

```bash
docker compose -p telemetry-demo cp ingestion-demo:/data/ground-truth.csv ./ground-truth.csv
curl http://127.0.0.1:9404/metrics | grep alerting_
docker compose -p telemetry-demo --profile pipeline --profile demo down -v
```

`SIMULATION_SEED` (default 42) and `SIMULATION_INTERVAL_MS` (default 500 in the demo service) change the scenario. The same seed always produces the same readings.

### Run locally

```bash
docker compose up -d                 # Kafka + TimescaleDB (host port 5433)
./mvnw -B clean verify               # build and test everything
```

`alerting-service` needs `KAFKA_BOOTSTRAP_SERVERS`, `DB_URL`, `DB_USER`, `DB_PASSWORD`, plus an optional `DLQ_FALLBACK_PATH` (default `dlq-fallback.jsonl`). `ingestion-service` and `stream-processing-service` read `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`). `api-service` needs `APP_SECURITY_JWT_PUBLIC_KEY_PATH` (PEM public key); run it with `--spring.profiles.active=dev`. The dev token tool lives in `api-service/src/test/.../DevTokenTool.java`; its keys are throwaway and git-ignored.

### Run the whole pipeline in containers

The `pipeline` profile builds `ingestion`, `stream-processing`, `alerting` and `api` from one `Dockerfile` and starts them behind three one-shot jobs: `migrate` (the Flyway schema), `seed-sensors` (the five simulated sensors, dev only) and `jwt-key-init` (writes the JWT public key into a volume).

```bash
docker compose down
export JWT_PUBLIC_KEY_BASE64=$(grep -v '^-----' dev-keys/public.pem | tr -d '\r\n')
docker compose -p telemetry-pipeline --profile pipeline up -d --build
```

- Use the project name `telemetry-pipeline`: the default project's volumes hold the hand-built dev database, which has no Flyway history, so `migrate` would refuse to run against it.
- `dev-keys/public.pem` comes from running `DevTokenTool` once; it also writes one-hour tokens next to it.
- The default simulated readings stay in the normal range, so no alert appears by itself. Give the stream app a couple of minutes to collect 30 readings per sensor, then send one outlier:

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

Labelled counters appear after their first event; the unlabelled ones read `0.0` from startup. api-service serves `/actuator/prometheus` on port 8080 for tokens with the `METRICS_READER` role only; `DevTokenTool` writes a one-hour `dev-keys/token-metrics.txt` for it.

## API

`GET /api/alerts` (role `ALERT_READER`) takes optional `sensorId`, `severity`, and a `from`/`to` pair (ISO-8601 instants, not combined with the other filters), plus `page` (default 0) and `size` (default 50, 1 to 200). Results are newest first, the body is a JSON array and the total number of matches is in the `X-Total-Count` header. A database outage answers `503` with `Retry-After`; a rejected token gets `401` with `error="invalid_token"`.

## Stack

Java 26, Spring Boot 4.1, Kafka (KRaft) and Kafka Streams, TimescaleDB on PostgreSQL 16, Flyway, RS256 JWT, Prometheus metrics, JUnit 5, Testcontainers, Docker Compose, GitHub Actions.

## Status

Complete and closed: the streaming pipeline, durable persistence with retry, dead-lettering and idempotency, API security and paging, Prometheus metrics for the alerting service, the seeded fault simulator with scored results, and CI (unit and Testcontainers tests) on every push.

Known limitations:

- JWT verification uses a single static public key; JWKS rotation is not implemented.
- No distributed tracing.
- No Grafana dashboard ships with the repo; the figures above are generated offline from the run's output.
- Retries that are finally given up are not counted by a metric.
- The api-service metrics scrape needs a bearer token that expires hourly.
- The baseline is cumulative per sensor, so it adapts slowly to a gradual process change; a windowed or exponentially weighted baseline is the obvious next step.
