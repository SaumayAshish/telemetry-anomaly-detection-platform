package com.telemetry.platform.alerting;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Guards what ships in the alerting image: the only runnable entry point among the production
 * classes is {@link AlertConsumer}. Learning harnesses belong under src/test.
 */
class ProductionClassesTest {

    private static final Path PRODUCTION_CLASSES = Path.of("target", "classes");

    @Test
    void onlyAlertConsumerHasAMainMethodInTheProductionClasses() throws IOException {
        List<String> withMain;
        try (Stream<Path> files = Files.walk(PRODUCTION_CLASSES)) {
            withMain = files
                    .filter(file -> file.toString().endsWith(".class"))
                    .map(ProductionClassesTest::className)
                    .filter(ProductionClassesTest::hasMainMethod)
                    .sorted()
                    .toList();
        }

        assertThat(withMain).containsExactly("com.telemetry.platform.alerting.AlertConsumer");
    }

    private static String className(Path classFile) {
        String relative = PRODUCTION_CLASSES.relativize(classFile).toString();
        return relative.substring(0, relative.length() - ".class".length())
                .replace(File.separatorChar, '.');
    }

    private static boolean hasMainMethod(String className) {
        try {
            Class.forName(className, false, ProductionClassesTest.class.getClassLoader())
                    .getDeclaredMethod("main", String[].class);
            return true;
        } catch (NoSuchMethodException noMain) {
            return false;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot load production class " + className, e);
        }
    }
}
