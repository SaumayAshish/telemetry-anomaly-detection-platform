package com.telemetry.platform.alerting;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class BuffersAndChannelsHarness {

    public static void main(String[] args) throws IOException {

        Path scratchDir = Path.of("harness-scratch");
        Files.createDirectories(scratchDir);
        Path dataFile = scratchDir.resolve("buffer-demo.txt");

        System.out.println("=== Section 1: writing through a FileChannel, one small buffer, reused across a loop ===");
        String[] linesToWrite = {
                "sensor=S-1001,reading=71.2\n",
                "sensor=S-1002,reading=68.9\n",
                "sensor=S-1003,reading=200.4\n"
        };
        try (FileChannel writeChannel = FileChannel.open(dataFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer writeBuffer = ByteBuffer.allocate(16);
            for (String line : linesToWrite) {
                byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
                int offset = 0;
                while (offset < lineBytes.length) {
                    writeBuffer.clear();
                    int chunkSize = Math.min(writeBuffer.remaining(), lineBytes.length - offset);
                    writeBuffer.put(lineBytes, offset, chunkSize);
                    writeBuffer.flip();
                    writeChannel.write(writeBuffer);
                    offset += chunkSize;
                }
            }
        }
        System.out.println("Wrote " + linesToWrite.length + " lines using a single reused 16-byte buffer");
        System.out.println("(Notice the buffer is only 16 bytes, smaller than most of these lines - the while loop proves");
        System.out.println("this isn't cheating by just being big enough to hold everything in one shot. clear() resets it to");
        System.out.println("'fill mode' before each chunk is put() in; flip() switches it to 'drain mode' so write() reads from");
        System.out.println("position 0 of what was just put in, not from wherever put() left position pointing.)");

        System.out.println();
        System.out.println("=== Section 2: reading it back through a FileChannel, same small buffer, reused again ===");
        StringBuilder rebuilt = new StringBuilder();
        try (FileChannel readChannel = FileChannel.open(dataFile, StandardOpenOption.READ)) {
            ByteBuffer readBuffer = ByteBuffer.allocate(16);
            int bytesRead;
            int readCalls = 0;
            while ((bytesRead = readChannel.read(readBuffer)) != -1) {
                readCalls++;
                readBuffer.flip();
                byte[] chunk = new byte[readBuffer.remaining()];
                readBuffer.get(chunk);
                rebuilt.append(new String(chunk, StandardCharsets.UTF_8));
                readBuffer.clear();
            }
            System.out.println("Reassembled content across " + readCalls + " channel.read() calls into one 16-byte buffer:");
            System.out.print(rebuilt);
        }
        System.out.println("(channel.read(buffer) returns the number of bytes actually read into the buffer, or -1 at true");
        System.out.println("end-of-file - never assume it fills the buffer completely just because the buffer has room; a real");
        System.out.println("filesystem or socket can hand back fewer bytes than you asked for on any given call. flip() here");
        System.out.println("switches from 'just filled by read()' to 'ready to be drained via get()', and clear() prepares the");
        System.out.println("SAME buffer object to be filled again next iteration - no new buffer allocated per chunk.)");

        System.out.println();
        System.out.println("=== Section 3: forgetting flip() - the single most common ByteBuffer bug, shown deliberately ===");
        ByteBuffer mistakeBuffer = ByteBuffer.allocate(16);
        mistakeBuffer.put("hello".getBytes(StandardCharsets.UTF_8));
        System.out.println("After put(\"hello\"): position=" + mistakeBuffer.position() + ", limit=" + mistakeBuffer.limit());
        byte[] wrongRead = new byte[mistakeBuffer.remaining()];
        mistakeBuffer.get(wrongRead);
        System.out.println("Reading WITHOUT flip() first: got " + wrongRead.length + " bytes, content=\""
                + new String(wrongRead, StandardCharsets.UTF_8) + "\" (garbage/empty - reading from position 5 onward, not from 0)");

        ByteBuffer correctBuffer = ByteBuffer.allocate(16);
        correctBuffer.put("hello".getBytes(StandardCharsets.UTF_8));
        correctBuffer.flip();
        byte[] correctRead = new byte[correctBuffer.remaining()];
        correctBuffer.get(correctRead);
        System.out.println("Reading WITH flip() first: got " + correctRead.length + " bytes, content=\""
                + new String(correctRead, StandardCharsets.UTF_8) + "\"");
        System.out.println("(Without flip(), position was already at 5 (right after the 5 bytes of \"hello\" were put in) and");
        System.out.println("limit was still at capacity (16) - so remaining() reported 11 bytes of uninitialized zero-bytes from");
        System.out.println("positions 5-15, not the \"hello\" you just wrote. This is exactly the bug that makes a real read-back");
        System.out.println("silently return empty/garbage instead of throwing - it compiles, it runs, it just reads the wrong");
        System.out.println("region of the buffer.)");

        System.out.println();
        System.out.println("=== Section 4: heap buffer vs. direct buffer - what actually differs ===");
        ByteBuffer heapBuffer = ByteBuffer.allocate(1024);
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024);
        System.out.println("heapBuffer.isDirect()=" + heapBuffer.isDirect() + ", directBuffer.isDirect()=" + directBuffer.isDirect());
        System.out.println("(A heap buffer lives in ordinary, movable, garbage-collected JVM heap memory - a native I/O call");
        System.out.println("must copy its bytes to a temporary native buffer first, since the OS cannot safely read directly out");
        System.out.println("of memory the GC might relocate mid-call. A direct buffer lives in native memory outside the JVM");
        System.out.println("heap from the start, so the OS can read/write it with no extra copy - at the cost of slower");
        System.out.println("allocation and no ordinary GC visibility into that native memory. Worth using directly only for a");
        System.out.println("buffer reused across MANY I/O calls, such as this section's writeBuffer/readBuffer if this project");
        System.out.println("were streaming large volumes of data rather than three short demo lines.)");

        Files.delete(dataFile);
        Files.delete(scratchDir);
        System.out.println();
        System.out.println("=== Section 5: where this project would actually reach for this layer ===");
        System.out.println("Files.readString()/writeString() from Step 1 are correct and sufficient for this project's current");
        System.out.println("needs - config files and a proposed local fallback log are all small, whole-file operations. This");
        System.out.println("raw ByteBuffer/Channel layer earns its complexity only when reading/writing something whose size");
        System.out.println("isn't known or bounded up front - a large export of historical anomaly data to disk, or (Step 8,");
        System.out.println("later in this phase) raw bytes arriving over a Socket, where there is no 'whole file' to ask for at");
        System.out.println("all, only a stream of chunks arriving over time.");
    }
}