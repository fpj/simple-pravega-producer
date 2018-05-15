package com.dataartisans;

import io.pravega.client.stream.EventStreamWriter;

import javax.annotation.Nonnull;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class EventProducer implements Runnable, AutoCloseable {

    @Nonnull
    private final EventStreamWriter<String> writer;

    @Nonnull
    private final CompletableFuture<Void> terminationFuture;

    private volatile int ratePerSecond;

    private volatile boolean running = true;

    public EventProducer(@Nonnull EventStreamWriter<String> writer, int ratePerSecond) {
        this.writer = writer;
        this.terminationFuture = new CompletableFuture<>();
        this.ratePerSecond = ratePerSecond;

        printCurrentRate();
    }

    @Override
    public void run() {
        try {
            while (running) {
                final long start = System.nanoTime();
                sendEvents();
                final long stop = System.nanoTime();

                Thread.sleep(1000L - (stop - start) / 1_000_000);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        terminationFuture.complete(null);
    }

    private void sendEvents() {
        for (int i = 0; i < ratePerSecond; i++) {
            final String message = UUID.randomUUID().toString();
            writer.writeEvent(message);
        }
    }

    public int getRatePerSecond() {
        return ratePerSecond;
    }

    public void setRatePerSecond(int ratePerSecond) {
        this.ratePerSecond = ratePerSecond;

        printCurrentRate();
    }

    private void printCurrentRate() {
        System.out.println(String.format("Producing %d events per second", ratePerSecond));
    }

    @Override
    public void close() throws Exception {
        running = false;
        terminationFuture.get();
    }
}
