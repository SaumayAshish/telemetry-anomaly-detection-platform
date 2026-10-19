package com.telemetry.platform.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import com.telemetry.platform.ingestion.ReadingSimulator.Fault;
import com.telemetry.platform.ingestion.ReadingSimulator.FaultType;
import com.telemetry.platform.ingestion.ReadingSimulator.GroundTruthWindow;
import com.telemetry.platform.ingestion.ReadingSimulator.SensorProfile;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class ReadingSimulatorTest {

    private static final double SIGMA = 1.5;
    private static final double EXACT = 1e-9;
    private static final SensorProfile A = new SensorProfile("S-A", 70.0, SIGMA);
    private static final SensorProfile B = new SensorProfile("S-B", 80.0, 2.0);

    private static ReadingSimulator quiet(long seed) {
        return new ReadingSimulator(seed, List.of(A, B), List.of());
    }

    private static ReadingSimulator with(Fault... faults) {
        return new ReadingSimulator(42L, List.of(A, B), List.of(faults));
    }

    private static List<Double> values(ReadingSimulator simulator, String sensorId, int count) {
        return LongStream.range(0, count).mapToObj(tick -> simulator.valueAt(sensorId, tick)).toList();
    }

    private static double diff(ReadingSimulator faulty, String sensorId, long tick) {
        return faulty.valueAt(sensorId, tick) - quiet(42L).valueAt(sensorId, tick);
    }

    private static double mean(List<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
    }

    private static double standardDeviation(List<Double> values) {
        double mean = mean(values);
        double squares = values.stream().mapToDouble(v -> (v - mean) * (v - mean)).sum();
        return Math.sqrt(squares / (values.size() - 1));
    }

    @Test
    void sameSeedGivesIdenticalValues() {
        List<Double> first = values(quiet(7L), "S-A", 50);
        List<Double> second = values(quiet(7L), "S-A", 50);

        assertThat(first).isEqualTo(second);
        assertThat(new HashSet<>(first)).hasSizeGreaterThan(40);
    }

    @Test
    void differentSeedsGiveDifferentValues() {
        assertThat(values(quiet(1L), "S-A", 50)).isNotEqualTo(values(quiet(2L), "S-A", 50));
    }

    @Test
    void aValueDoesNotDependOnTheOrderOfCalls() {
        ReadingSimulator busy = quiet(3L);
        double fresh = quiet(3L).valueAt("S-A", 5);

        busy.valueAt("S-A", 100);
        busy.valueAt("S-B", 5);
        busy.valueAt("S-A", 4);

        assertThat(busy.valueAt("S-A", 5)).isEqualTo(fresh);
        assertThat(fresh).isBetween(55.0, 85.0);
    }

    @Test
    void quietTicksCenterOnTheBaselineMean() {
        assertThat(mean(values(quiet(11L), "S-A", 5000))).isCloseTo(70.0, within(0.1));
    }

    @Test
    void quietTicksHaveTheConfiguredSpread() {
        assertThat(standardDeviation(values(quiet(11L), "S-A", 5000))).isCloseTo(SIGMA, within(0.075));
    }

    @Test
    void aSpikeChangesOnlyItsOwnTick() {
        ReadingSimulator faulty = with(Fault.spike("S-A", 100, 8.0));

        assertThat(diff(faulty, "S-A", 100)).isCloseTo(8.0 * SIGMA, within(EXACT));
        for (long tick : new long[] {0, 99, 101, 500}) {
            assertThat(diff(faulty, "S-A", tick)).isCloseTo(0.0, within(EXACT));
        }
    }

    @Test
    void aLevelShiftChangesEveryTickInItsWindow() {
        ReadingSimulator faulty = with(Fault.levelShift("S-A", 50, 10, 6.0));

        for (long tick = 50; tick <= 59; tick++) {
            assertThat(diff(faulty, "S-A", tick)).isCloseTo(6.0 * SIGMA, within(EXACT));
        }
        assertThat(diff(faulty, "S-A", 49)).isCloseTo(0.0, within(EXACT));
        assertThat(diff(faulty, "S-A", 60)).isCloseTo(0.0, within(EXACT));
    }

    @Test
    void aDriftRampsLinearlyToFullMagnitudeOnItsLastTick() {
        ReadingSimulator faulty = with(Fault.drift("S-A", 50, 4, 8.0));
        double full = 8.0 * SIGMA;

        assertThat(diff(faulty, "S-A", 50)).isCloseTo(full * 1 / 4, within(EXACT));
        assertThat(diff(faulty, "S-A", 51)).isCloseTo(full * 2 / 4, within(EXACT));
        assertThat(diff(faulty, "S-A", 52)).isCloseTo(full * 3 / 4, within(EXACT));
        assertThat(diff(faulty, "S-A", 53)).isCloseTo(full, within(EXACT));
        assertThat(diff(faulty, "S-A", 49)).isCloseTo(0.0, within(EXACT));
        assertThat(diff(faulty, "S-A", 54)).isCloseTo(0.0, within(EXACT));
    }

    @Test
    void aNegativeMagnitudeLowersTheValue() {
        ReadingSimulator faulty = with(Fault.spike("S-A", 100, -8.0));

        assertThat(diff(faulty, "S-A", 100)).isCloseTo(-8.0 * SIGMA, within(EXACT));
    }

    @Test
    void aFaultOnOneSensorLeavesTheOthersAlone() {
        ReadingSimulator faulty = with(Fault.spike("S-A", 100, 8.0));

        assertThat(diff(faulty, "S-A", 100)).isNotZero();
        for (long tick = 0; tick < 200; tick++) {
            assertThat(diff(faulty, "S-B", tick)).isCloseTo(0.0, within(EXACT));
        }
    }

    @Test
    void groundTruthMapsTicksToTimestamps() {
        ReadingSimulator simulator = with(
                Fault.spike("S-A", 100, 8.0),
                Fault.levelShift("S-B", 200, 10, 6.0));
        Instant start = Instant.parse("2026-01-01T00:00:00Z");

        List<GroundTruthWindow> windows = simulator.groundTruth(start, Duration.ofSeconds(2));

        assertThat(windows).containsExactly(
                new GroundTruthWindow("S-A", FaultType.SPIKE,
                        start.plusSeconds(200), start.plusSeconds(200), 8.0),
                new GroundTruthWindow("S-B", FaultType.LEVEL_SHIFT,
                        start.plusSeconds(400), start.plusSeconds(418), 6.0));
    }

    @Test
    void groundTruthIsEmptyWithoutFaults() {
        assertThat(quiet(1L).groundTruth(Instant.parse("2026-01-01T00:00:00Z"), Duration.ofSeconds(2))).isEmpty();
    }

    @Test
    void rejectsAFaultForAnUnknownSensor() {
        assertThatThrownBy(() -> with(Fault.spike("S-Z", 10, 5.0))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsANegativeStartTick() {
        assertThatThrownBy(() -> with(Fault.spike("S-A", -1, 5.0))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsAZeroDuration() {
        assertThatThrownBy(() -> with(Fault.levelShift("S-A", 10, 0, 5.0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveNoise() {
        assertThatThrownBy(() -> new ReadingSimulator(1L, List.of(new SensorProfile("S-A", 70.0, 0.0)), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNoProfiles() {
        assertThatThrownBy(() -> new ReadingSimulator(1L, List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsOverlappingFaultsOnOneSensor() {
        assertThatThrownBy(() -> with(Fault.levelShift("S-A", 10, 10, 5.0), Fault.spike("S-A", 15, 5.0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsDuplicateSensorProfiles() {
        assertThatThrownBy(() -> new ReadingSimulator(1L, List.of(A, new SensorProfile("S-A", 71.0, 1.0)), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsAValueRequestForAnUnknownSensor() {
        assertThatThrownBy(() -> quiet(1L).valueAt("S-Q", 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sensorsHaveIndependentNoise() {
        ReadingSimulator simulator = quiet(5L);
        List<Double> a = LongStream.range(0, 50).mapToObj(t -> (simulator.valueAt("S-A", t) - 70.0) / SIGMA).toList();
        List<Double> b = LongStream.range(0, 50).mapToObj(t -> (simulator.valueAt("S-B", t) - 80.0) / 2.0).toList();

        long different = LongStream.range(0, 50).filter(i -> Math.abs(a.get((int) i) - b.get((int) i)) > 0.01).count();

        assertThat(different).isGreaterThanOrEqualTo(45);
    }

    @Test
    void rejectsFaultsThatTouch() {
        assertThatThrownBy(() -> with(Fault.levelShift("S-A", 10, 10, 5.0), Fault.spike("S-A", 19, 5.0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsFaultsThatDoNotTouch() {
        assertThat(with(Fault.levelShift("S-A", 10, 10, 5.0), Fault.spike("S-A", 20, 5.0)).faults()).hasSize(2);
    }

    @Test
    void demoUsesTheFiveSeededSensors() {
        assertThat(ReadingSimulator.demo(1L).sensorIds())
                .containsExactly("S-1001", "S-1002", "S-1003", "S-1004", "S-1005");
    }

    @Test
    void demoCoversEveryFaultType() {
        Set<FaultType> types = ReadingSimulator.demo(1L).faults().stream()
                .map(Fault::type).collect(Collectors.toSet());

        assertThat(types).containsExactlyInAnyOrder(FaultType.values());
    }

    @Test
    void demoStartsFaultsAfterWarmUpAndLeavesRoomForTheBaselineToRecover() {
        ReadingSimulator demo = ReadingSimulator.demo(1L);
        List<Fault> faults = demo.faults();

        assertThat(faults).isNotEmpty();
        assertThat(faults).allSatisfy(f -> assertThat(f.startTick()).isGreaterThanOrEqualTo(40));
        for (String sensorId : demo.sensorIds()) {
            List<Fault> mine = faults.stream()
                    .filter(f -> f.sensorId().equals(sensorId))
                    .sorted(Comparator.comparingLong(Fault::startTick))
                    .toList();
            for (int i = 1; i < mine.size(); i++) {
                assertThat(mine.get(i).startTick() - mine.get(i - 1).lastTick()).isGreaterThanOrEqualTo(40);
            }
        }
    }

    @Test
    void demoFitsInsideTheDemoHorizon() {
        List<Fault> faults = ReadingSimulator.demo(1L).faults();

        assertThat(faults).isNotEmpty();
        assertThat(faults).allSatisfy(f -> assertThat(f.lastTick()).isLessThan(ReadingSimulator.DEMO_TICKS));
    }

    @Test
    void demoCoversEverySeverityBand() {
        List<Double> sizes = ReadingSimulator.demo(1L).faults().stream()
                .map(f -> Math.abs(f.magnitudeSigmas())).toList();

        assertThat(sizes).anyMatch(m -> m >= 15.0);
        assertThat(sizes).anyMatch(m -> m >= 10.0 && m < 15.0);
        assertThat(sizes).anyMatch(m -> m >= 5.0 && m < 10.0);
        assertThat(sizes).anyMatch(m -> m >= 3.5 && m < 5.0);
    }
}