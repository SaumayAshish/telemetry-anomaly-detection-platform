package com.telemetry.platform.alerting;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;

import java.time.Instant;

public class JsonHarness {

    static class PlainReading {
        public String sensorId;
        public double reading;

        public PlainReading() {
        }

        public PlainReading(String sensorId, double reading) {
            this.sensorId = sensorId;
            this.reading = reading;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class SensorReadingPayload {
        @JsonProperty("sensor_id")
        String sensorId;

        double reading;

        Instant recordedAt;

        @JsonIgnore
        String internalDebugFlag;

        public SensorReadingPayload() {
        }

        public SensorReadingPayload(String sensorId, double reading, Instant recordedAt, String internalDebugFlag) {
            this.sensorId = sensorId;
            this.reading = reading;
            this.recordedAt = recordedAt;
            this.internalDebugFlag = internalDebugFlag;
        }
    }

    public static void main(String[] args) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        ObjectMapper plainMapper = new ObjectMapper();

        System.out.println("=== Section 1: ObjectMapper basics - default field-name matching, no annotations ===");
        PlainReading originalPlain = new PlainReading("S-1001", 71.2);
        String plainJson = plainMapper.writeValueAsString(originalPlain);
        System.out.println("Serialized: " + plainJson);
        PlainReading restoredPlain = plainMapper.readValue(plainJson, PlainReading.class);
        System.out.println("Deserialized back: sensorId=" + restoredPlain.sensorId + ", reading=" + restoredPlain.reading);
        System.out.println("(No annotations needed here because the JSON keys happen to already match the Java field names");
        System.out.println("exactly - sensorId maps to \"sensorId\". Real external JSON contracts are rarely this convenient.)");

        System.out.println();
        System.out.println("=== Section 2: @JsonProperty for a real naming mismatch, @JsonIgnore to exclude a field ===");

        SensorReadingPayload originalPayload = new SensorReadingPayload("S-1001", 71.2, Instant.parse("2026-09-24T10:15:30Z"),
                "internal-only, never serialized");
        String payloadJson = mapper.writeValueAsString(originalPayload);
        System.out.println("Serialized: " + payloadJson);
        System.out.println("(Notice the JSON key is \"sensor_id\", not \"sensorId\" - @JsonProperty(\"sensor_id\") on the Java");
        System.out.println("field remapped it. And \"internalDebugFlag\" does not appear in the JSON at all - @JsonIgnore");
        System.out.println("excluded it from serialization entirely, unlike Step 4's transient, which excludes a field from");
        System.out.println("the byte stream but the field still physically exists on the class either way - same underlying");
        System.out.println("idea, different mechanism, because JSON and Java Serialization are solving this at different layers.)");

        System.out.println();
        System.out.println("=== Section 3: java.time support requires an explicitly registered module ===");
        System.out.println("(Section 2's Instant field already proved this works: \"recordedAt\":\"2026-09-24T10:15:30Z\" is a");
        System.out.println("clean ISO-8601 string, not Jackson's undecorated default of a numeric epoch-seconds array, because");
        System.out.println("registerModule(new JavaTimeModule()) plus disabling WRITE_DATES_AS_TIMESTAMPS was done explicitly");
        System.out.println("before this ObjectMapper was ever used. Skip that registration and Instant serialization either");
        System.out.println("fails outright or produces an unreadable numeric array - a real, easy-to-forget setup step.)");

        System.out.println();
        System.out.println("=== Section 4: @JsonIgnoreProperties(ignoreUnknown = true) - JSON's forgiving evolution, live ===");
        String jsonWithExtraField = "{\"sensor_id\":\"S-1002\",\"reading\":68.9,\"recordedAt\":\"2026-09-24T10:16:00Z\","
                + "\"firmwareVersion\":\"2.3.1\"}";
        System.out.println("Incoming JSON has a field this class has never heard of: \"firmwareVersion\"");
        SensorReadingPayload lenientResult = mapper.readValue(jsonWithExtraField, SensorReadingPayload.class);
        System.out.println("Deserialized successfully anyway: sensorId=" + lenientResult.sensorId
                + ", reading=" + lenientResult.reading + ", recordedAt=" + lenientResult.recordedAt);
        System.out.println("(This succeeded ONLY because SensorReadingPayload is annotated @JsonIgnoreProperties(ignoreUnknown");
        System.out.println("= true). This is the direct, concrete payoff Step 4 promised: a producer adds a new field to its");
        System.out.println("payload, and every consumer still running the OLD class definition keeps working without any");
        System.out.println("error - the exact opposite of Step 4's InvalidClassException-on-rolling-deployment failure mode.");
        System.out.println("Without this annotation, Jackson's actual default behavior is to THROW - proven next.)");

        System.out.println();
        System.out.println("=== Section 5: without that annotation, and on genuinely malformed JSON - real thrown exceptions ===");
        ObjectMapper strictMapper = new ObjectMapper();
        strictMapper.registerModule(new JavaTimeModule());
        try {
            strictMapper.readValue(plainJson.replace("}", ",\"unexpectedField\":123}"), PlainReading.class);
            System.out.println("  (this should NOT print - PlainReading has no @JsonIgnoreProperties annotation)");
        } catch (JsonProcessingException e) {
            System.out.println("Strict deserialization of an unrecognized field threw: " + e.getClass().getSimpleName());
        }
        try {
            mapper.readValue("{not valid json at all", SensorReadingPayload.class);
            System.out.println("  (this should NOT print)");
        } catch (JsonProcessingException e) {
            System.out.println("Genuinely malformed JSON threw: " + e.getClass().getSimpleName());
        }

        System.out.println();
        System.out.println("=== Section 6: this is exactly what Phase 2's Kafka Streams Serde has been doing all along ===");
        System.out.println("The custom Serde built back in Phase 2 for typed Kafka Streams deserialization is, underneath, this");
        System.out.println("exact same ObjectMapper.writeValueAsString()/readValue() pair, wrapped to satisfy Kafka's Serializer/");
        System.out.println("Deserializer interface contract instead of being called directly. Nothing new is being introduced");
        System.out.println("here - this step exists to make the mechanism you've already been running since Phase 2 fully");
        System.out.println("explicit, including the exact annotations and forward-compatibility behavior it depends on.");
    }
}