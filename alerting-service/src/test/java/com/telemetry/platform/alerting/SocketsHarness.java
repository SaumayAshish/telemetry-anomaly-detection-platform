package com.telemetry.platform.alerting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class SocketsHarness {

    private static final double ANOMALY_THRESHOLD = 100.0;

    public static void main(String[] args) throws Exception {

        System.out.println("=== Section 1: a raw TCP server - ServerSocket, accept(), blocking I/O ===");
        ServerSocket serverSocket = new ServerSocket(0);
        int port = serverSocket.getLocalPort();
        System.out.println("Server listening on port " + port);
        System.out.println("(ServerSocket.accept() below will BLOCK the server thread - it does nothing at all, using no");
        System.out.println("CPU, until a client actually connects. This is the defining trait of blocking I/O.)");

        Thread serverThread = new Thread(() -> runServer(serverSocket));
        serverThread.setDaemon(true);
        serverThread.start();

        System.out.println();
        System.out.println("=== Section 2: a client - Socket, connecting, writing a line, reading the response ===");
        try (Socket clientSocket = new Socket("localhost", port);
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true, StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8))) {

            out.println("S-1001,71.2");
            String response = in.readLine();
            System.out.println("Client sent: S-1001,71.2");
            System.out.println("Client received: " + response);
        }
        System.out.println("(This looks almost exactly like Step 1/2's file I/O style - a stream to write to, a stream to");
        System.out.println("read from. A network socket and a file are both just a stream of bytes at this API level; the OS");
        System.out.println("and the network stack handle the actual delivery underneath.)");

        System.out.println();
        System.out.println("=== Section 3: proving the block is real - the client's readLine() genuinely waits ===");
        try (Socket clientSocket = new Socket("localhost", port);
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true, StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8))) {

            long start = System.nanoTime();
            out.println("SLOW,150.0");
            String response = in.readLine();
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            System.out.println("Client received: " + response + " after " + elapsedMs + "ms");
            System.out.println("(The server deliberately sleeps 500ms before responding to sensorId \"SLOW\" - the elapsed time");
            System.out.println("above should land close to 500ms. in.readLine() was genuinely blocked that whole time, not");
            System.out.println("polling or spinning - exactly why a server handling many simultaneous slow clients with blocking");
            System.out.println("I/O needs one thread per connection rather than one thread serving everyone in sequence.)");
        }

        System.out.println();
        System.out.println("=== Section 4: two sequential clients - the server's single accept() loop serves one at a time ===");
        for (int i = 1; i <= 2; i++) {
            try (Socket clientSocket = new Socket("localhost", port);
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true, StandardCharsets.UTF_8);
                 BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8))) {
                out.println("S-100" + i + "," + (60 + i));
                String response = in.readLine();
                System.out.println("Client " + i + " received: " + response);
            }
        }
        System.out.println("(Both clients were served correctly, one after another, because the server loop calls accept()");
        System.out.println("again immediately after finishing each connection. A real production server would spawn a new");
        System.out.println("thread (or virtual thread) per accepted connection instead of handling each one inline on the");
        System.out.println("single accept loop thread, so one slow client can never delay every other client behind it.)");

        System.out.println();
        System.out.println("=== Section 5: this is the actual foundation Kafka's own client, and JDBC, are built on ===");
        System.out.println("KafkaProducer/KafkaConsumer maintain their own pool of raw sockets (NIO SocketChannels, for the");
        System.out.println("scalability reasons Phase 8 covered) to each broker, framing requests and responses in Kafka's own");
        System.out.println("binary wire protocol - conceptually the exact client/server relationship just demonstrated by hand,");
        System.out.println("with Kafka's own protocol instead of this harness's comma-separated line format. The Postgres JDBC");
        System.out.println("driver this project already uses does the same thing to the database - opens a raw socket to");
        System.out.println("Postgres's listening port and speaks Postgres's own wire protocol over it.");

        serverSocket.close();
    }

    private static void runServer(ServerSocket serverSocket) {
        while (!serverSocket.isClosed()) {
            try (Socket clientConnection = serverSocket.accept();
                 BufferedReader in = new BufferedReader(new InputStreamReader(clientConnection.getInputStream(), StandardCharsets.UTF_8));
                 PrintWriter out = new PrintWriter(clientConnection.getOutputStream(), true, StandardCharsets.UTF_8)) {

                String line = in.readLine();
                if (line == null) {
                    continue;
                }
                String[] parts = line.split(",");
                String sensorId = parts[0];
                double reading = Double.parseDouble(parts[1]);

                if ("SLOW".equals(sensorId)) {
                    Thread.sleep(500);
                }

                String status = reading > ANOMALY_THRESHOLD ? "ANOMALY" : "OK";
                out.println(sensorId + "," + reading + "," + status);
            } catch (IOException e) {
                if (!serverSocket.isClosed()) {
                    System.out.println("Server error: " + e.getMessage());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}