# ADR 0005: Baseline Hysteresis and Rebaselining

## Status

Accepted

## Context

stream-processing-service scores each reading against a per-sensor baseline (mean and standard deviation). If anomalous readings were folded into the baseline, a sustained fault would stretch the standard deviation until the fault stopped looking anomalous: the outliers would hide themselves. But a baseline that never moves cannot follow a sensor whose normal level really has changed.

## Decision

### 1. Per-sensor state

The baseline is kept per sensor in a persistent Kafka Streams key-value store (`sensor-baseline-store-v2`). The 30-second windowed statistics in the same application are only printed and take no part in scoring.

### 2. Decision order for one reading

`AnomalyDecisionEngine.decide` runs these steps:

1. Ask the baseline whether the reading is an anomaly, given the z-score threshold (3.0) and the minimum number of samples before scoring starts (30). The first readings of a sensor are therefore never scored.
2. Not an anomaly: the reading is added to the baseline and no event is emitted.
3. An anomaly: the consecutive-anomaly streak is incremented, and an event is emitted that carries the z-score, the baseline mean and standard deviation as they were before this reading, the streak length and a severity derived from the z-score.
4. The baseline itself is not updated by the anomalous reading (hysteresis): only the streak counter changes.

### 3. Rebaselining

When the streak reaches 10 consecutive anomalies, the baseline is discarded and restarted from the current reading, so a sensor whose normal level has moved is eventually judged against its new level. The event for the reading that triggers the rebaseline is still built from the old baseline.

### 4. Thresholds

The three numbers (3.0, 30 and 10) are constants in `AnomalyDetectionStreamApp`. They are not configurable at run time.

## Alternatives considered

- Updating the baseline with every reading: simple, but a sustained fault hides itself.
- Never rebaselining: a real level shift would raise an alert for every reading, forever.
- A fixed time window instead of a streak: the windowed statistics exist for display, but a window also contains the outliers it is meant to detect.

## Consequences

- A sustained fault produces up to 10 events, one per anomalous reading, and then the baseline moves and the alerts stop. The alerts table can therefore hold bursts of consecutive events for the same sensor.
- After a rebaseline the new baseline holds a single reading, and the minimum-sample parameter (30) is passed to the baseline's anomaly check, so that rule governs when scoring resumes.
- A sensor whose baseline has zero standard deviation (a constant signal) is never scored, and the test uses the absolute z-score, so a spike in either direction counts.
- Changing a threshold means a code change and a redeploy.
- The state store name carries a version suffix (`v2`). Changing the state format needs a new name, because Kafka Streams would otherwise read old state with the new format.
