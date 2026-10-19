# Documentation index

| Document | What it covers |
|---|---|
| `adr/0001-telemetry-topic-partition-key.md` | Why readings are keyed by sensor id |
| `adr/0002-jwt-bearer-authentication.md` | Why the API uses RS256 bearer JWTs |
| `adr/0003-alerting-failure-handling.md` | What the alert writer retries, dead-letters and gives up on |
| `adr/0004-idempotency-key-for-alerts.md` | Why `(partition, offset)` is the idempotency key |
| `adr/0005-baseline-hysteresis-and-rebaselining.md` | How the detector avoids hiding its own outliers |
| `adr/0006-seeded-fault-simulator-and-scored-demo.md` | How detection is scored against injected faults |
| `logging-policy.md` | What may and must never be logged |
| `demo/` | Raw results of the scored demo run, and the scripts that score and plot it |
| `images/` | Figures used in the top-level README |

To reproduce the demo results, see "Reproduce the results above" in the top-level `README.md`.
