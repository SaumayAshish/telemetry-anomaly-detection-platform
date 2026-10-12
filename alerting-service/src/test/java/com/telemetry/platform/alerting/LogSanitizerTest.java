package com.telemetry.platform.alerting;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogSanitizerTest {

    @Test
    void nullBecomesTheLiteralWordNull() {
        assertThat(LogSanitizer.safe(null)).isEqualTo("null");
    }

    @Test
    void asciiControlCharactersAreReplacedWithUnderscores() {
        assertThat(LogSanitizer.safe("a\nb")).isEqualTo("a_b");
        assertThat(LogSanitizer.safe("a\r\nb")).isEqualTo("a__b");
        assertThat(LogSanitizer.safe("a\tb")).isEqualTo("a_b");
    }

    @Test
    void aForgedLogLineCannotSpanLines() {
        String hostile = "S-1\n2026-10-12 10:00:00 ERROR fake admin login";
        assertThat(LogSanitizer.safe(hostile)).doesNotContain("\n").doesNotContain("\r");
    }

    @Test
    void exactly64CharactersIsKeptWhole() {
        String sixtyFour = "x".repeat(64);
        assertThat(LogSanitizer.safe(sixtyFour)).isEqualTo(sixtyFour);
    }

    @Test
    void sixtyFiveCharactersIsCutToSixtyFourPlusEllipsis() {
        assertThat(LogSanitizer.safe("x".repeat(65))).isEqualTo("x".repeat(64) + "...");
    }

    @Test
    void ordinaryNonAsciiTextIsKept() {
        assertThat(LogSanitizer.safe("सेंसर-१")).isEqualTo("सेंसर-१");
    }

    @Test
    void unicodeLineSeparatorsAreAlsoStripped() {
        // NEL (U+0085), line separator (U+2028), paragraph separator (U+2029)
        assertThat(LogSanitizer.safe("a\u0085b\u2028c\u2029d")).isEqualTo("a_b_c_d");
    }
}