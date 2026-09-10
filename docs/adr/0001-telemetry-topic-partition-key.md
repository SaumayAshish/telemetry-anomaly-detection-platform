# ADR 0001: Telemetry Topic Naming and Partition Key Strategy

## Status

Accepted

## Context

The platform receives continuous sensor telemetry through Kafka.

In Phase 2, Kafka Streams will process these readings to calculate per-sensor rolling-window statistics such as mean, standard deviation, and rate of change.

For this processing to work correctly, readings belonging to the same sensor need to remain ordered and processed on the same partition. Therefore, the topic name, partition key, and partition count need to be decided before implementing the producers and consumers.

## Decision

### 1. Topic Name

We will use:

`telemetry.sensor.readings.v1`

The naming follows a hierarchical structure:

`<domain>.<entity>.<purpose>.<version>`

- `telemetry` → domain
- `sensor` → entity
- `readings` → purpose
- `v1` → message contract version

The explicit version allows the message structure to evolve safely. If a breaking change is required later, we can introduce:

`telemetry.sensor.readings.v2`

instead of changing the existing `v1` contract while consumers are still using it.

### 2. Partition Key

The partition key will be:

`sensorId`

Kafka uses the key to determine the partition for a message. Therefore, readings with the same `sensorId` will be sent to the same partition.

This is important because Phase 2 performs windowed aggregation per sensor and depends on the readings for a sensor being processed in order.

### 3. Partition Count

The topic will have:

`6 partitions`

for local development.

Six partitions provide enough parallelism for the expected local workload without unnecessarily increasing development complexity.

The partition count also effectively limits useful consumer parallelism. For example, a consumer group with six active consumer instances can process the six partitions in parallel. Additional consumer instances would have no partition to process.

Increasing the partition count later is not simply a configuration change. Kafka's key-to-partition mapping can change when the partition count increases, meaning the same `sensorId` may map to a different partition for newly produced records.

Therefore, if the partition count needs to increase in the future, it should be treated as a planned migration/scaling decision, particularly where ordering and per-sensor processing are important.

### 4. Message Shape

The initial conceptual message will contain:

- `sensorId`
- `timestamp`
- `metricType`
- `value`
- `sequenceNumber`

Example:

```json
{
  "sensorId": "S-1001",
  "timestamp": "2026-09-10T20:15:30Z",
  "metricType": "temperature",
  "value": 72.4,
  "sequenceNumber": 10452
}