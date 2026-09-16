package com.telemetry.platform.alerting;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.telemetry.platform.events.AnomalyEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class AlertConsumer {

    private static final String ANOMALY_TOPIC = "telemetry.sensor.anomalies.v1";
    private static final String GROUP_ID = "alerting-service-group";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Deserializer<AnomalyEvent> valueDeserializer = (topic, bytes) -> {
            try {
                return OBJECT_MAPPER.readValue(bytes, AnomalyEvent.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize AnomalyEvent", e);
            }
        };

        KafkaConsumer<String, AnomalyEvent> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), valueDeserializer);

        consumer.subscribe(List.of(ANOMALY_TOPIC));

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received - waking up consumer...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            while (true) {
                ConsumerRecords<String, AnomalyEvent> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, AnomalyEvent> record : records) {
                    AnomalyEvent event = record.value();
                    System.out.println("ALERT | sensor=" + event.sensorId()
                            + " | value=" + event.value()
                            + " | baselineMean=" + event.baselineMean()
                            + " | baselineStdDev=" + event.baselineStdDev()
                            + " | zScore=" + event.zScore()
                            + " | consecutiveAnomalies=" + event.consecutiveAnomalies()
                            + " | at=" + event.readingTimestamp()
                            + " | partition=" + record.partition()
                            + " | offset=" + record.offset());
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            System.out.println("Consumer loop interrupted for shutdown, as expected.");
        } finally {
            consumer.close();
            System.out.println("Consumer closed cleanly.");
        }
    }
}