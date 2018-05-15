package com.dataartisans;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import sun.dc.pr.PRError;

import java.net.URI;
import java.util.Scanner;

/**
 * Pravega data producer
 */
public class PravegaProducer {

    private static final String DEFAULT_SCOPE = "flinkScope";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://localhost:9090";
    private static final String DEFAULT_STREAM_NAME = "flinkStream";
    private static final int DEFAULT_PROMETHEUS_PORT = 5001;
    private static final String PRAVEGA_PRODUCER_EVENT_RATE_PER_SECOND = "pravega_producer_event_rate_per_second";

    private static final Option scopeOption = new Option("s", "scope", true, "The scope (namespace) of the Stream to write to.");
    private static final Option streamOption = new Option("n", "name", true, "The name of the Stream to write to.");
    private static final Option controllerOption = new Option("u", "uri", true, "The URI to the Pravega controller in the form tcp://host:port");
    private static final Option prometheusPortOptions = new Option("p", "port", true, "The Prometheus port");

    public static void main( String[] args ) throws Exception {
        final CommandLine commandLine = parseCommandLineArgs(getOptions(), args);

        final URI controllerURI = new URI(commandLine.hasOption(controllerOption.getOpt()) ? commandLine.getOptionValue(controllerOption.getOpt()) : DEFAULT_CONTROLLER_URI);
        final String scope = commandLine.hasOption(scopeOption.getOpt()) ? commandLine.getOptionValue(scopeOption.getOpt()) : DEFAULT_SCOPE;
        final String streamName = commandLine.hasOption(streamOption.getOpt()) ? commandLine.getOptionValue(streamOption.getOpt()) : DEFAULT_STREAM_NAME;
        final int prometheusPort = commandLine.hasOption(prometheusPortOptions.getOpt()) ? Integer.parseInt(commandLine.getOptionValue(prometheusPortOptions.getOpt())) : DEFAULT_PROMETHEUS_PORT;

        StreamManager streamManager = StreamManager.create(controllerURI);

        final boolean scopeIsNew = streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(10, 4, 1))
                .build();
        final boolean streamIsNew = streamManager.createStream(
                scope,
                streamName,
                streamConfig);

        try (PrometheusReporter prometheusReporter = new PrometheusReporter(prometheusPort);
             ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                     new JavaSerializer<>(),
                     EventWriterConfig.builder().build())) {

            final EventProducer eventProducer = new EventProducer(writer, 2);

            prometheusReporter.registerMetric(
                    PRAVEGA_PRODUCER_EVENT_RATE_PER_SECOND,
                    "The event rate of the Pravega producer.",
                    createEventRateChild(eventProducer));

            Thread producerThread = new Thread(eventProducer);

            producerThread.start();

            final Scanner scanner = new Scanner(System.in);

            while (true) {
                if (scanner.hasNextInt()) {
                    final int newRatePerSecond = scanner.nextInt();
                    eventProducer.setRatePerSecond(newRatePerSecond);
                } else {
                    Thread.sleep(100L);
                }
            }
        }
    }

    private static Gauge.Child createEventRateChild(EventProducer eventProducer) {
        return new Gauge.Child() {
            @Override
            public double get() {
                return eventProducer.getRatePerSecond();
            }
        };
    }

    private static Options getOptions() {
        final Options options = new Options();

        options.addOption(scopeOption);
        options.addOption(streamOption);
        options.addOption(controllerOption);
        options.addOption(prometheusPortOptions);
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
}
