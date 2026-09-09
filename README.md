# Real-Time Telemetry Anomaly Detection Platform

A streaming platform that ingests sensor telemetry, detects anomalies via a
served ML model, and lets an operator query alerts in plain English.

## Status
Phase 0 — Foundations (in progress)

## Architecture
See `docs/adr/` for architecture decision records as they're written.

## Local Development
Infra: `docker compose up -d` (Kafka + TimescaleDB — see docker-compose.yml)