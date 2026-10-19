package com.telemetry.platform.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.telemetry.platform.ingestion.ReadingSimulator.Fault;
import com.telemetry.platform.ingestion.ReadingSimulator.SensorProfile;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class DemoScenarioTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration INTERVAL = Duration.ofMillis(500);
    private static final String HEADER = "sensor_id,fault_type,from,to,magnitude_sigmas";

    private static DemoScenario demo() {
        return new DemoScenario(ReadingSimulator.demo(1L), START, INTERVAL);
    }

    @Test
    void readingsAtATickUseTheStartTimeAndTheInterval() {
        List<SensorReading> readings = demo().readingsAt(3);

        assertThat(readings).extracting(SensorReading::sensorId)
                .containsExactly("S-1001", "S-1002", "S-1003", "S-1004", "S-1005");
        assertThat(readings).extracting(SensorReading::timestamp).containsOnly(START.plusMillis(1500));
        assertThat(readings).extracting(SensorReading::metricType).containsOnly("temperature");
    }

    @Test
    void sequenceNumbersStartAtOne() {
        assertThat(demo().readingsAt(0)).extracting(SensorReading::sequenceNumber).containsOnly(1L);
        assertThat(demo().readingsAt(3)).extracting(SensorReading::sequenceNumber).containsOnly(4L);
    }

    @Test
    void readingValuesComeFromTheSimulator() {
        ReadingSimulator simulator = ReadingSimulator.demo(1L);
        List<SensorReading> readings = new DemoScenario(simulator, START, INTERVAL).readingsAt(7);

        assertThat(readings).hasSize(5);
        for (SensorReading reading : readings) {
            assertThat(reading.value()).isEqualTo(simulator.valueAt(reading.sensorId(), 7));
        }
    }

    @Test
    void theScenarioLastsTheDemoHorizon() {
        assertThat(demo().totalTicks()).isEqualTo(ReadingSimulator.DEMO_TICKS);
    }

    @Test
    void groundTruthCsvStartsWithAHeader() {
        assertThat(demo().groundTruthCsv().lines().findFirst()).contains(HEADER);
    }

    @Test
    void groundTruthCsvHasOneLinePerFault() {
        List<Fault> faults = ReadingSimulator.demo(1L).faults();

        assertThat(faults).hasSize(10);
        assertThat(demo().groundTruthCsv().lines().count()).isEqualTo(faults.size() + 1L);
    }

    @Test
    void groundTruthCsvWritesTimesFromTheStartAndTheInterval() {
        List<String> lines = demo().groundTruthCsv().lines().toList();

        assertThat(lines).contains(
                "S-1001,SPIKE,2026-01-01T00:00:40Z,2026-01-01T00:00:40Z,12.0",
                "S-1001,LEVEL_SHIFT,2026-01-01T00:02:05Z,2026-01-01T00:02:12Z,6.0");
    }

    @Test
    void groundTruthCsvHasOnlyAHeaderWithoutFaults() {
        ReadingSimulator quiet = new ReadingSimulator(1L, List.of(new SensorProfile("S-A", 70.0, 1.5)), List.of());

        assertThat(new DemoScenario(quiet, START, INTERVAL).groundTruthCsv().lines().toList())
                .isEqualTo(List.of(HEADER));
    }

    @Test
    void rejectsATickOutsideTheScenario() {
        assertThatThrownBy(() -> demo().readingsAt(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> demo().readingsAt(ReadingSimulator.DEMO_TICKS))
                .isInstanceOf(IllegalArgumentException.class);
    }
}