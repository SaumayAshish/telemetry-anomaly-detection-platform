# Logging policy

## Levels

| Level | Use for | Examples in this project |
|---|---|---|
| INFO | Normal business operation and lifecycle | `ALERT_PERSISTED`, `DEAD_LETTER_DUPLICATE`, configuration loaded, shutdown |
| WARN | Abnormal but handled or recoverable | `DEAD_LETTERED`, database reconnects, validation failures |
| ERROR | Failure that needs attention or loses data | credentials rejected, dead-letter table write failed, record permanently lost, unhandled exception |

## Format

- Parameterized messages: `log.info("... {}", value)`. Never string concatenation.
- Every record-related line carries `topic`, `partition`, `offset` (the same key as the `alert_dlq` row), plus `sensorId` when known and a reason or exception class.
- API requests carry a correlation id (`X-Correlation-Id`, in MDC key `correlationId`) on every line.

## Never log

- Passwords, tokens, API keys, Kafka credentials; connection strings with credentials.
- Full raw payloads. They belong in the dead-letter table, where access is controlled.
- Exception messages from database insert paths (PostgreSQL error text can contain the failing row's values). Log the exception class and SQLState.
- Personal or sensitive data that is not needed to diagnose the problem.

## Untrusted input

- Values taken from Kafka messages or HTTP headers can contain control characters. Sanitize before logging (`safe()` in `AlertConsumer`); inbound correlation ids are accepted only if they match `[A-Za-z0-9._-]{1,64}`.
- Parameterized logging does not prevent log forging by itself.

## Not logs

- Counts and rates ("dead-lettered records per hour") belong in metrics (planned: Prometheus), not in log searches.