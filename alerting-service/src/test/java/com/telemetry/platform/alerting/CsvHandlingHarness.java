package com.telemetry.platform.alerting;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

public class CsvHandlingHarness {

    public static void main(String[] args) throws Exception {

        String sensorId = "S-1001";
        String reading = "71.2";
        String notes = "flagged, review: value \"exceeds\" threshold\nsecond line of notes";

        System.out.println("=== Section 1: the naive way - String.join, and why it corrupts real data ===");
        String naiveRow = String.join(",", sensorId, reading, notes);
        System.out.println("Naive CSV row:");
        System.out.println(naiveRow);
        System.out.println("(The notes field itself contains a comma, embedded double quotes, and a literal newline. Written");
        System.out.println("naively, this single logical row is now indistinguishable from multiple rows with extra columns -");
        System.out.println("there is no way for a naive reader to know where this field actually ends.)");

        System.out.println();
        System.out.println("=== Section 2: the naive read - String.split falls apart on the exact same row ===");
        String[] naiveFields = naiveRow.split(",");
        System.out.println("Naive split produced " + naiveFields.length + " fields (expected 3):");
        for (int i = 0; i < naiveFields.length; i++) {
            System.out.println("  [" + i + "] " + naiveFields[i].replace("\n", "\\n"));
        }
        System.out.println("(The comma inside \"flagged, review\" was treated as a field separator, not part of the data - the");
        System.out.println("row silently split into the wrong number of fields, with no exception thrown anywhere.)");

        System.out.println();
        System.out.println("=== Section 3: the correct way - Apache Commons CSV handles RFC 4180 quoting automatically ===");
        StringWriter csvWriter = new StringWriter();
        try (CSVPrinter printer = new CSVPrinter(csvWriter, CSVFormat.DEFAULT)) {
            printer.printRecord(sensorId, reading, notes);
        }
        String correctCsv = csvWriter.toString();
        System.out.println("Correctly-quoted CSV output:");
        System.out.println(correctCsv);
        System.out.println("(Notice the notes field is now wrapped in double quotes, its internal double quotes are doubled");
        System.out.println("(\"\" instead of \"), and the embedded newline is preserved INSIDE the quoted field rather than");
        System.out.println("breaking it into a new row. This is RFC 4180's actual quoting rule, applied automatically - no");
        System.out.println("code here manually decided when to add quotes.)");

        System.out.println();
        System.out.println("=== Section 4: reading it back correctly - the exact original 3 fields recovered ===");
        try (CSVParser parser = CSVParser.parse(new StringReader(correctCsv), CSVFormat.DEFAULT)) {
            List<CSVRecord> records = parser.getRecords();
            CSVRecord record = records.get(0);
            System.out.println("Field count: " + record.size() + " (expected 3)");
            System.out.println("sensorId: " + record.get(0));
            System.out.println("reading: " + record.get(1));
            System.out.println("notes matches original exactly: " + record.get(2).equals(notes));
        }
        System.out.println("(CSVParser correctly reconstructed exactly 3 fields, with the notes field's embedded comma, quotes,");
        System.out.println("and newline all recovered byte-for-byte - the parser understood the quoting, not just the commas.)");

        System.out.println();
        System.out.println("=== Section 5: where this project would actually use CSV ===");
        System.out.println("This project's real data lives in Kafka (JSON) and Postgres, and CSV has no role in its core data");
        System.out.println("path. But a genuinely common, realistic enterprise need this project could plausibly grow into: a");
        System.out.println("business stakeholder - not an engineer - asking for a CSV export of recent anomaly alerts to open");
        System.out.println("directly in Excel. That is exactly CSV's actual enterprise niche: a lowest-common-denominator");
        System.out.println("interchange format for humans and spreadsheet tools, never for a system's own internal data path.");
    }
}