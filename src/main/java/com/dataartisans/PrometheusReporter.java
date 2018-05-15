package com.dataartisans;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;

import javax.annotation.Nonnull;

import java.io.IOException;

public class PrometheusReporter implements AutoCloseable {

    @Nonnull
    private final HTTPServer httpServer;

    public PrometheusReporter(int prometheusPort) throws IOException {
        this.httpServer = new HTTPServer(prometheusPort);
    }

    public void registerMetric(@Nonnull String metricName, @Nonnull String helpMessage, Gauge.Child childToRegister) {
        final Gauge eventRateGauge = Gauge.build().name(metricName).help(helpMessage).create();

        eventRateGauge.register();

        eventRateGauge.setChild(childToRegister);
    }

    @Override
    public void close() {
        httpServer.stop();
    }
}
