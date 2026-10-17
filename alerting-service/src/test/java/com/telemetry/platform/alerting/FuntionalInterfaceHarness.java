package com.telemetry.platform.alerting;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;

public class FuntionalInterfaceHarness {
    public static void main(String[] args) {

        System.out.println("=== Step 1: Function composition - andThen() vs compose() ===");
        Function<Double, Double> addFivePercentMargin = z -> z * 1.05;
        Function<Double, Double> roundToTwoDecimals = z -> Math.round(z * 100.0) / 100.0;

        Function<Double, Double> marginThenRound = addFivePercentMargin.andThen(roundToTwoDecimals);
        Function<Double, Double> roundThenMargin = addFivePercentMargin.compose(roundToTwoDecimals);

        double rawZScore = 21.486;
        System.out.println("Raw z-score: " + rawZScore);
        System.out.println("addFivePercentMargin.andThen(roundToTwoDecimals) -> margin first, then round: " + marginThenRound.apply(rawZScore));
        System.out.println("addFivePercentMargin.compose(roundToTwoDecimals) -> round first (as the argument), then margin: " + roundThenMargin.apply(rawZScore));

        System.out.println();
        System.out.println("=== Step 2: Predicate composition - and(), or(), negate() ===");
        Predicate<Double> isAboveMediumThreshold = z -> Math.abs(z) >= 5.0;
        Predicate<Double> isAboveCriticalThreshold = z -> Math.abs(z) >= 15.0;
        Predicate<Double> isMediumOnly = isAboveMediumThreshold.and(isAboveCriticalThreshold.negate());

        double[] sampleScores = {2.1, 6.9, 19.3};
        for (double z : sampleScores) {
            System.out.println("z=" + z
                    + " | aboveMedium=" + isAboveMediumThreshold.test(z)
                    + " | aboveCritical=" + isAboveCriticalThreshold.test(z)
                    + " | mediumOnly(and+negate)=" + isMediumOnly.test(z));
        }

        System.out.println();
        System.out.println("=== Step 3: Consumer.andThen() - chaining side effects, not values ===");
        Consumer<String> printToConsole = line -> System.out.println("  [console] " + line);
        Consumer<String> printToAuditLog = line -> System.out.println("  [audit]   " + line);
        Consumer<String> logEverywhere = printToConsole.andThen(printToAuditLog);
        logEverywhere.accept("ALERT sensor=S-1001 severity=CRITICAL");

        System.out.println();
        System.out.println("=== Step 4: BiFunction, Supplier, UnaryOperator, BinaryOperator - the rest of the catalog ===");
        BiFunction<Double, Double, Double> zScoreOf = (value, mean) -> (value - mean) / 6.46;
        System.out.println("BiFunction zScoreOf.apply(92.0, 75.09): " + zScoreOf.apply(92.0, 75.09));

        Supplier<String> defaultSeverity = () -> "LOW";
        System.out.println("Supplier defaultSeverity.get(): " + defaultSeverity.get());

        UnaryOperator<Double> clampToPositive = z -> Math.max(z, 0.0);
        System.out.println("UnaryOperator clampToPositive.apply(-3.5): " + clampToPositive.apply(-3.5));

        BinaryOperator<Double> pickMoreSevere = (z1, z2) -> Math.abs(z1) >= Math.abs(z2) ? z1 : z2;
        System.out.println("BinaryOperator pickMoreSevere.apply(6.9, -19.3): " + pickMoreSevere.apply(6.9, -19.3));

        System.out.println();
        System.out.println("=== Step 5: primitive specializations exist to avoid autoboxing ===");
        Predicate<Double> boxedIsAnomalous = z -> Math.abs(z) >= 5.0;
        DoublePredicate primitiveIsAnomalous = z -> Math.abs(z) >= 5.0;
        System.out.println("Predicate<Double> takes a boxed Double - every call allocates a Double object on the heap.");
        System.out.println("DoublePredicate takes a primitive double - zero boxing, zero extra allocation.");
        System.out.println("boxedIsAnomalous.test(6.9):     " + boxedIsAnomalous.test(6.9));
        System.out.println("primitiveIsAnomalous.test(6.9): " + primitiveIsAnomalous.test(6.9));

        ToDoubleFunction<String> parseZScore = Double::parseDouble;
        System.out.println("ToDoubleFunction parseZScore.applyAsDouble(\"6.9\"): " + parseZScore.applyAsDouble("6.9"));

        DoubleUnaryOperator squareRootOfAbs = z -> Math.sqrt(Math.abs(z));
        System.out.println("DoubleUnaryOperator squareRootOfAbs.applyAsDouble(-16.0): " + squareRootOfAbs.applyAsDouble(-16.0));

        System.out.println();
        System.out.println("=== Step 6: the boxed-comparison trap - a real correctness bug hiding in Step 5's boxed version ===");
        Double firstBoxedZScore = 6.9;
        Double secondBoxedZScore = 6.9;
        System.out.println("firstBoxedZScore == secondBoxedZScore (reference equality on two boxed Doubles): " + (firstBoxedZScore == secondBoxedZScore));
        System.out.println("firstBoxedZScore.equals(secondBoxedZScore) (value equality): " + firstBoxedZScore.equals(secondBoxedZScore));

        Integer firstSmallBoxedInt = 100;
        Integer secondSmallBoxedInt = 100;
        Integer firstLargeBoxedInt = 200;
        Integer secondLargeBoxedInt = 200;
        System.out.println("Integer 100 == 100 (within the cached -128..127 range): " + (firstSmallBoxedInt == secondSmallBoxedInt));
        System.out.println("Integer 200 == 200 (outside the cached range):          " + (firstLargeBoxedInt == secondLargeBoxedInt));
    }
}
