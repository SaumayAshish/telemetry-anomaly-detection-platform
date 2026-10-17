package com.telemetry.platform.alerting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CollectorBasicsHarness {

    record Reading(String deviceId, String severity, double zScore) {}

    public static void main(String[] args) {

        List<Reading> readings = List.of(
                new Reading("sensor-A1", "CRITICAL", 21.5),
                new Reading("sensor-A1", "WARNING", 6.9),
                new Reading("sensor-B2", "CRITICAL", 25.48),
                new Reading("sensor-B2", "NORMAL", 1.2),
                new Reading("sensor-C3", "WARNING", -12.2),
                new Reading("sensor-C3", "NORMAL", 0.3),
                new Reading("sensor-A1", "NORMAL", -0.8),
                new Reading("sensor-B2", "WARNING", 19.3)
        );

        System.out.println("=== Step 1: why collect() exists - reduce() for a mutable container is the wrong tool ===");
        ArrayList<String> viaReduceAntiPattern = readings.stream()
                .map(Reading::deviceId)
                .reduce(new ArrayList<String>(),
                        (ArrayList<String> list, String id) -> {
                            ArrayList<String> copy = new ArrayList<>(list);
                            copy.add(id);
                            return copy;
                        },
                        (ArrayList<String> list1, ArrayList<String> list2) -> {
                            ArrayList<String> merged = new ArrayList<>(list1);
                            merged.addAll(list2);
                            return merged;
                        });
        System.out.println("Built via reduce() (copies a new list every step - O(n^2) total work): " + viaReduceAntiPattern);

        List<String> viaCollect = readings.stream()
                .map(Reading::deviceId)
                .collect(Collectors.toList());
        System.out.println("Built via collect(toList()) (mutates ONE list in place - O(n) total work): " + viaCollect);

        System.out.println();
        System.out.println("=== Step 2: toSet() and joining() ===");
        var distinctDeviceIds = readings.stream()
                .map(Reading::deviceId)
                .collect(Collectors.toSet());
        System.out.println("Distinct device IDs (Set, no guaranteed order), size: " + distinctDeviceIds.size());

        String joined = readings.stream()
                .map(Reading::deviceId)
                .distinct()
                .sorted()
                .collect(Collectors.joining(", ", "[", "]"));
        System.out.println("Joined, sorted, distinct: " + joined);

        System.out.println();
        System.out.println("=== Step 3: groupingBy() - single level ===");
        Map<String, List<Reading>> byDevice = readings.stream()
                .collect(Collectors.groupingBy(Reading::deviceId));
        new TreeMap<>(byDevice).forEach((deviceId, list) ->
                System.out.println("  " + deviceId + " -> " + list.size() + " readings"));

        System.out.println();
        System.out.println("=== Step 4: groupingBy() with a downstream collector ===");
        Map<String, Long> countPerDevice = readings.stream()
                .collect(Collectors.groupingBy(Reading::deviceId, Collectors.counting()));
        System.out.println("Count per device: " + new TreeMap<>(countPerDevice));

        Map<String, Double> avgZScorePerSeverity = readings.stream()
                .collect(Collectors.groupingBy(Reading::severity, Collectors.averagingDouble(Reading::zScore)));
        System.out.println("Average z-score per severity: " + new TreeMap<>(avgZScorePerSeverity));

        System.out.println();
        System.out.println("=== Step 5: partitioningBy() - always exactly 2 groups, true and false ===");
        Map<Boolean, List<Reading>> partitioned = readings.stream()
                .collect(Collectors.partitioningBy(r -> Math.abs(r.zScore()) >= 5.0));
        System.out.println("Anomalous (|z| >= 5.0): " + partitioned.get(true).size() + " readings");
        System.out.println("Normal    (|z| <  5.0): " + partitioned.get(false).size() + " readings");

        System.out.println();
        System.out.println("=== Step 6: toMap() with a merge function for duplicate keys ===");
        Map<String, Double> maxZScorePerDevice = readings.stream()
                .collect(Collectors.toMap(
                        Reading::deviceId,
                        Reading::zScore,
                        Double::max));
        System.out.println("Max z-score per device (merge function resolves key collisions): " + new TreeMap<>(maxZScorePerDevice));

        System.out.println();
        System.out.println("=== Step 7: toMap() WITHOUT a merge function throws on duplicate keys ===");
        try {
            Map<String, Double> broken = readings.stream()
                    .collect(Collectors.toMap(Reading::deviceId, Reading::zScore));
            System.out.println("This line never runs: " + broken);
        } catch (IllegalStateException e) {
            System.out.println("Caught expected exception: " + e.getMessage());
        }
    }
}


