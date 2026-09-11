package com.telemetry.platform.ingestion;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SensorTelemetryProducer {

    private static final String TOPIC = "telemetry.sensor.readings.v1";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            String sensorId = "S-1001";
            String value = "{\"sensorId\":\"S-1001\",\"timestamp\":\"2026-09-10T20:00:00Z\","
                    + "\"metricType\":\"temperature\",\"value\":72.4,\"sequenceNumber\":1}";

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, sensorId, value);

            RecordMetadata metadata = producer.send(record).get();

            System.out.println("Sent to partition " + metadata.partition()
                    + " at offset " + metadata.offset());
        }
    }
}