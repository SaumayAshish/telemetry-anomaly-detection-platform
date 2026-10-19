# ADR 0006: Seeded Fault Simulator and Scored Demo

## Status

Accepted

## Context

The original simulated readings stayed inside the normal range, so the pipeline never raised an alert by itself, and "it alerts when I force an outlier" is weak evidence that the detector works. Detection quality has to be measured against faults whose position and size are known.

## Decision

### 1. A stateless, seeded signal

`ReadingSimulator` computes the value of any sensor at any tick from the seed, the sensor id and the tick number alone. There is no mutable state, so the same seed always produces the same readings, in any order and on any machine. Noise is Gaussian with a per-sensor mean and standard deviation.

### 2. Three fault types with known magnitude

SPIKE (one tick), LEVEL_SHIFT (constant offset for a duration) and DRIFT (linear ramp, full offset on the last tick). Magnitudes are expressed in standard deviations of the sensor's own noise. Faults on one sensor may not overlap.

### 3. Ground truth as timestamp windows

The scenario writes `ground-truth.csv` (sensor, fault type, from, to, magnitude in sigmas). Scoring compares alert timestamps with these windows, so it needs no knowledge of the detector's internals.

### 4. Demo mode is opt-in

`SIMULATION_SCENARIO=demo` switches `ingestion-service` to a 600-tick scenario that exits when finished. Unset, blank or any legacy value keeps the old producer loop. The `demo` compose profile is not part of the default stack and uses its own volume.

### 5. Results are verified by independent replay

`docs/demo/analyze.py` re-implements the detector (running Welford baseline, hysteresis, rebaseline after 10 consecutive anomalies) and must reproduce the stored alerts exactly before any figure is trusted.

## Alternatives considered

- Recorded real data: realistic, but there is no ground truth for what counts as a fault.
- Random fault injection at run time: not reproducible, so results cannot be compared between runs.

## Consequences

- On the committed run (seed 42): 9 of 10 faults detected. The miss is a +2.5 sigma spike, below the 3 sigma threshold, so it is correct behaviour. 9 false positives in 2,584 quiet scored readings (0.348%), against 0.27% expected from noise alone.
- The replay matches all 78 stored alerts only when the standard deviation divides by n (population), not n-1 (sample). Switching to the sample formula yields 77 alerts. This is the detector's real behaviour and is now recorded.
- A level shift always yields exactly 10 alerts, then scoring pauses for 30 readings while the new baseline warms up. Faults on one sensor therefore need at least 40 ticks of spacing to be scored independently.
- Drifts are detected late by nature: the ramp must exceed about 3 sigma first.
