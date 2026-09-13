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

import java.time.Instant;
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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        Map<String, AtomicLong> sequenceNumbers = new HashMap<>();
        for (String sensorId : SENSOR_IDS) {
            sequenceNumbers.put(sensorId, new AtomicLong(1));
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            while (true) {
                for (String sensorId : SENSOR_IDS) {


                    double simulatedTemperature = 65.0 + RANDOM.nextDouble() * 20.0;
                    long sequenceNumber = sequenceNumbers.get(sensorId).getAndIncrement();

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
}