package com.telemetry.platform.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * kafka-streams brings an older jackson-annotations than the databind this project pins,
 * and Maven picks the older one unless every Jackson artifact is managed together. The app
 * dies at startup in ObjectMapper's static initializer when that happens.
 */
class JacksonClasspathTest {

    @Test
    void jacksonAnnotationsMatchTheDatabindRelease() {
        assertThatCode(() -> Class.forName("com.fasterxml.jackson.annotation.JsonSerializeAs"))
                .doesNotThrowAnyException();
    }

    @Test
    void anObjectMapperRoundTripsASensorReading() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        SensorReading reading =
                new SensorReading("S-1001", Instant.parse("2026-10-15T10:15:30Z"), "temperature", 71.5, 42L);

        String json = mapper.writeValueAsString(reading);

        assertThat(mapper.readValue(json, SensorReading.class)).isEqualTo(reading);
    }
}