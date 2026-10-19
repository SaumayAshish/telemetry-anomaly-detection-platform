package com.telemetry.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class SensorTelemetryProducer {

    private static final String TOPIC = "telemetry.sensor.readings.v1";

    private static final List<String> SENSOR_IDS = List.of(
            "S-1001", "S-1002", "S-1003", "S-1004", "S-1005"
    );

    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        String bootstrapServers = IngestionSettings.bootstrapServers();
        System.out.println("Kafka bootstrap servers: " + bootstrapServers);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        IngestionSettings.SimulationSettings simulation = IngestionSettings.simulation(System::getenv);
        if (simulation.demo()) {
            runDemo(props, objectMapper, simulation);
            return;
        }

        Map<String, AtomicLong> sequenceNumbers = new HashMap<>();
        for (String sensorId : SENSOR_IDS) {
            sequenceNumbers.put(sensorId, new AtomicLong(1));
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            while (true) {
                for (String sensorId : SENSOR_IDS) {

                    long sequenceNumber = sequenceNumbers.get(sensorId).getAndIncrement();
                    double simulatedTemperature = 65.0 + RANDOM.nextDouble() * 20.0;

                    SensorReading reading = new SensorReading(
                            sensorId,
                            Instant.now(),
                            "temperature",
                            simulatedTemperature,
                            sequenceNumber
                    );

                    String value = objectMapper.writeValueAsString(reading);

                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, reading.sensorId(), value);

                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                System.err.println("Failed to send reading for " + sensorId
                                        + ": " + exception.getMessage());
                            } else {
                                System.out.println("Sent " + sensorId
                                        + " to partition " + metadata.partition()
                                        + " at offset " + metadata.offset());
                            }
                        }
                    });
                }

                Thread.sleep(2000);
            }
        }
    }

    private static void runDemo(
            Properties props, ObjectMapper objectMapper, IngestionSettings.SimulationSettings settings)
            throws Exception {
        Instant start = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Duration interval = Duration.ofMillis(settings.intervalMillis());
        DemoScenario scenario = new DemoScenario(ReadingSimulator.demo(settings.seed()), start, interval);

        Path groundTruth = Path.of(settings.groundTruthPath()).toAbsolutePath();
        Files.createDirectories(groundTruth.getParent());
        Files.writeString(groundTruth, scenario.groundTruthCsv());
        System.out.println("Demo scenario: " + scenario.totalTicks() + " ticks every " + interval.toMillis()
                + " ms from " + start + ", seed " + settings.seed());
        System.out.println("Ground truth written to " + groundTruth);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (long tick = 0; tick < scenario.totalTicks(); tick++) {
                for (SensorReading reading : scenario.readingsAt(tick)) {
                    String value = objectMapper.writeValueAsString(reading);
                    producer.send(new ProducerRecord<>(TOPIC, reading.sensorId(), value), (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Failed to send reading for " + reading.sensorId()
                                    + ": " + exception.getMessage());
                        }
                    });
                }
                if ((tick + 1) % 50 == 0) {
                    System.out.println("Sent tick " + (tick + 1) + " of " + scenario.totalTicks());
                }
                long waitMillis = Duration.between(Instant.now(), start.plus(interval.multipliedBy(tick + 1))).toMillis();
                if (waitMillis > 0) {
                    Thread.sleep(waitMillis);
                }
            }
            producer.flush();
        }
        System.out.println("Demo scenario finished");
    }
}
