package com.telemetry.platform.alerting;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;

public class SerializationHarness {

    static class AlertSnapshot implements Serializable {
        private static final long serialVersionUID = 1L;

        String sensorId;
        double reading;
        long timestampMs;
        transient String debugNotes;

        AlertSnapshot(String sensorId, double reading, long timestampMs, String debugNotes) {
            this.sensorId = sensorId;
            this.reading = reading;
            this.timestampMs = timestampMs;
            this.debugNotes = debugNotes;
        }
    }

    static class VersionWithoutExtraMethod implements Serializable {
        String sensorId;
        double reading;
    }

    static class VersionWithExtraMethod implements Serializable {
        String sensorId;
        double reading;

        void someUnrelatedHelperAddedLater() {
            // deliberately touches no serialized field at all
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        System.out.println("=== Section 1: Serializable round-trip through ObjectOutputStream/ObjectInputStream ===");
        AlertSnapshot original = new AlertSnapshot("S-1001", 71.2, System.currentTimeMillis(), "raised by threshold breach");

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOut = new ObjectOutputStream(byteStream)) {
            objectOut.writeObject(original);
        }
        byte[] serializedBytes = byteStream.toByteArray();
        System.out.println("Serialized to " + serializedBytes.length + " bytes");

        AlertSnapshot restored;
        try (ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(serializedBytes))) {
            restored = (AlertSnapshot) objectIn.readObject();
        }
        System.out.println("sensorId matches: " + original.sensorId.equals(restored.sensorId));
        System.out.println("reading matches: " + (original.reading == restored.reading));
        System.out.println("timestampMs matches: " + (original.timestampMs == restored.timestampMs));
        System.out.println("original debugNotes: \"" + original.debugNotes + "\"");
        System.out.println("restored debugNotes: " + restored.debugNotes);
        System.out.println("(sensorId, reading, and timestampMs round-tripped correctly through pure reflection-based field");
        System.out.println("copying - no getters/setters, no manual encoding logic written anywhere. But debugNotes is marked");
        System.out.println("transient, so it was deliberately EXCLUDED from the byte stream entirely - it comes back as null,");
        System.out.println("the field's default value, not the original string. transient exists for exactly this: fields that");
        System.out.println("should never be persisted - a cached/derived value, a non-serializable handle like a Socket or");
        System.out.println("Thread, or genuinely sensitive data that has no business in a stored byte stream.)");

        System.out.println();
        System.out.println("=== Section 2: the actual binary format - a real, inspectable magic number ===");
        StringBuilder hex = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            hex.append(String.format("%02X ", serializedBytes[i]));
        }
        System.out.println("First 4 bytes of the stream: " + hex.toString().trim());
        System.out.println("(0xACED is the STREAM_MAGIC constant, 0x0005 is STREAM_VERSION - every single object Java has ever");
        System.out.println("serialized, going back to Java 1.1, starts with exactly these 4 bytes. Everything after this header");
        System.out.println("is class metadata (name, serialVersionUID, field descriptors) followed by the actual field data -");
        System.out.println("this is what makes the format self-describing enough to reconstruct an object via reflection alone,");
        System.out.println("and also exactly what ties it so tightly to the Java class that produced it.)");

        System.out.println();
        System.out.println("=== Section 3: the auto-computed serialVersionUID is fragile to more than just field changes ===");
        long uidWithoutMethod = ObjectStreamClass.lookup(VersionWithoutExtraMethod.class).getSerialVersionUID();
        long uidWithMethod = ObjectStreamClass.lookup(VersionWithExtraMethod.class).getSerialVersionUID();
        System.out.println("Auto-computed UID, no declared serialVersionUID, identical fields:");
        System.out.println("  Without the extra method: " + uidWithoutMethod);
        System.out.println("  With an unrelated extra method added: " + uidWithMethod);
        System.out.println("  Same UID? " + (uidWithoutMethod == uidWithMethod));
        System.out.println("(Both classes have IDENTICAL serializable state - same fields, same types. The only difference is");
        System.out.println("one has an extra method that never touches a single field. Yet their auto-computed UIDs differ,");
        System.out.println("because the hash factors in method signatures too, not just fields. In a real system: if a class is");
        System.out.println("serialized and stored somewhere (a cache, a session store) WITHOUT an explicit serialVersionUID,");
        System.out.println("then later that class gains so much as an unrelated helper method, every previously-stored instance");
        System.out.println("becomes unreadable - ObjectInputStream throws InvalidClassException, refusing to believe it's still");
        System.out.println("the same class. This is precisely why AlertSnapshot above declares 'serialVersionUID = 1L' explicitly");
        System.out.println("- it pins compatibility to a number YOU control, so the class can evolve without silently breaking");
        System.out.println("every already-serialized instance.)");

        System.out.println();
        System.out.println("=== Section 4: why this project uses Jackson/JSON instead, not this ===");
        System.out.println("This project has used Jackson for Kafka message payloads since Phase 2, and that was the right call,");
        System.out.println("not an oversight to revisit. Two concrete reasons, beyond just following convention: SECURITY -");
        System.out.println("ObjectInputStream.readObject() on untrusted bytes is a well-documented, historically exploited RCE");
        System.out.println("vector (real 'gadget chain' attacks, e.g. the Apache Commons Collections deserialization CVEs, have");
        System.out.println("achieved full remote code execution purely from a crafted byte stream) - never deserialize this way");
        System.out.println("from an untrusted source, such as an inbound Kafka message from outside this system's trust boundary.");
        System.out.println("PORTABILITY - this binary format is Java-only and version-fragile even within Java, as Section 3 just");
        System.out.println("proved live; JSON is readable by any language, any service, and evolves far more forgivingly (an");
        System.out.println("added field is just ignored by an old reader, not a hard version-mismatch failure).");
    }
}