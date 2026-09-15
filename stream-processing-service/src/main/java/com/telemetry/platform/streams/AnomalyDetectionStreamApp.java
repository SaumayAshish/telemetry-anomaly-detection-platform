package com.telemetry.platform.streams;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import java.util.Set;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class AnomalyDetectionStreamApp {

    private static final String INPUT_TOPIC = "telemetry.sensor.readings.v1";
    static final String ANOMALY_TOPIC = "telemetry.sensor.anomalies.v1";
    static final double Z_SCORE_THRESHOLD = 3.0;
    static final long MIN_SAMPLES_BEFORE_SCORING = 30;
    static final long CONSECUTIVE_ANOMALIES_BEFORE_REBASELINE = 10;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-detection-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Serde<SensorReading> sensorReadingSerde = jsonSerde(SensorReading.class);
        Serde<RollingStats> rollingStatsSerde = jsonSerde(RollingStats.class);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, SensorReading> readings =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), sensorReadingSerde));

        KTable<Windowed<String>, RollingStats> windowedStats = readings
                .groupByKey(Grouped.with(Serdes.String(), sensorReadingSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(30), Duration.ofSeconds(5)))
                .aggregate(
                        RollingStats::initial,
                        (sensorId, reading, currentStats) -> currentStats.update(reading.value()),
                        Materialized.<String, RollingStats, WindowStore<Bytes, byte[]>>as("windowed-rolling-stats-store-v2")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(rollingStatsSerde)
                );

        windowedStats.toStream().foreach((windowedKey, stats) -> {
            String sensorId = windowedKey.key();
            Instant windowStart = windowedKey.window().startTime();
            Instant windowEnd = windowedKey.window().endTime();

            System.out.println("Sensor " + sensorId
                    + " | window=[" + windowStart + " -> " + windowEnd + "]"
                    + " | count=" + stats.count()
                    + " | mean=" + stats.mean()
                    + " | stdDev=" + stats.stdDev());
        });

        KStream<String, AnomalyEvent> anomalies =
                readings.processValues(
                        new AnomalyProcessorSupplier("sensor-baseline-store-v2")
                );

        anomalies.to(
                ANOMALY_TOPIC,
                Produced.with(
                        Serdes.String(),
                        jsonSerde(AnomalyEvent.class)
                )
        );

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static class AnomalyProcessorSupplier implements FixedKeyProcessorSupplier<String, SensorReading, AnomalyEvent> {

        private final String storeName;

        AnomalyProcessorSupplier(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public FixedKeyProcessor<String, SensorReading, AnomalyEvent> get() {
            return new AnomalyProcessor(storeName);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            StoreBuilder<KeyValueStore<String, RollingStats>> storeBuilder = Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(storeName),
                    Serdes.String(),
                    jsonSerde(RollingStats.class)
            );
            return Set.of(storeBuilder);
        }
    }

    private static class AnomalyProcessor implements FixedKeyProcessor<String, SensorReading, AnomalyEvent> {
        private final String storeName;
        private KeyValueStore<String, RollingStats> store;
        private FixedKeyProcessorContext<String, AnomalyEvent> context;

        AnomalyProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(FixedKeyProcessorContext<String, AnomalyEvent> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(FixedKeyRecord<String, SensorReading> record) {
            String sensorId = record.key();
            SensorReading reading = record.value();

            RollingStats currentStats = store.get(sensorId);
            if (currentStats == null) {
                currentStats = RollingStats.initial();
            }

            boolean anomaly = currentStats.isAnomaly(reading.value(), Z_SCORE_THRESHOLD, MIN_SAMPLES_BEFORE_SCORING);

            if (anomaly) {
                long newStreak = currentStats.consecutiveAnomalies() + 1;
                double zScore = currentStats.zScore(reading.value());
                double baselineMean = currentStats.mean();
                double baselineStdDev = currentStats.stdDev();

                RollingStats newStats;
                if (newStreak >= CONSECUTIVE_ANOMALIES_BEFORE_REBASELINE) {
                    newStats = RollingStats.initial().update(reading.value());
                    System.out.println("RE-BASELINING | sensor=" + sensorId + " after " + newStreak + " consecutive anomalies");
                } else {
                    newStats = currentStats.withAnomalyStreak();
                }
                store.put(sensorId, newStats);

                System.out.println("ANOMALY DETECTED | sensor=" + sensorId + " | value=" + reading.value()
                        + " | baselineMean=" + baselineMean + " | baselineStdDev=" + baselineStdDev
                        + " | zScore=" + zScore + " | consecutiveAnomalies=" + newStreak);

                AnomalyEvent event = new AnomalyEvent(
                        sensorId,
                        reading.timestamp(),
                        reading.value(),
                        baselineMean,
                        baselineStdDev,
                        zScore,
                        newStreak
                );

                context.forward(record.withValue(event));
            } else {
                RollingStats newStats = currentStats.update(reading.value());
                store.put(sensorId, newStats);
            }
        }
    }

    private static <T> Serde<T> jsonSerde(Class<T> type) {
        Serializer<T> serializer = (topic, value) -> {
            try {
                return OBJECT_MAPPER.writeValueAsBytes(value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize " + type.getSimpleName(), e);
            }
        };

        Deserializer<T> deserializer = (topic, bytes) -> {
            try {
                return OBJECT_MAPPER.readValue(bytes, type);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize " + type.getSimpleName(), e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}