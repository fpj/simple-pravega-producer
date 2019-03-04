package io.pravega;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.*;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.SegmentNotification;
import io.prometheus.client.Gauge;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.net.URI;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pravega data producer
 */
public class PravegaProducer {

    private static final String DEFAULT_SCOPE = "flinkScope";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://localhost:9090";
    private static final String DEFAULT_STREAM_NAME = "flinkStream";
    private static final int DEFAULT_PROMETHEUS_PORT = 9093;
    private static final String PRAVEGA_PRODUCER_EVENT_RATE_PER_SECOND = "pravega_producer_event_rate_per_second";
    private static final String PRAVEGA_NUM_OF_SEGMENTS = "pravega_num_of_segments";

    private static final Option scopeOption = new Option("s", "scope", true, "The scope (namespace) of the Stream to write to.");
    private static final Option streamOption = new Option("n", "name", true, "The name of the Stream to write to.");
    private static final Option controllerOption = new Option("u", "uri", true, "The URI to the Pravega controller in the form tcp://host:port");
    private static final Option prometheusPortOptions = new Option("p", "port", true, "The Prometheus port");

    private static final AtomicInteger currentNumberOfSegments = new AtomicInteger(1);

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
        final ClientConfig config = ClientConfig.builder().controllerURI(controllerURI).build();

        try (PrometheusReporter prometheusReporter = new PrometheusReporter(prometheusPort);
             ConnectionFactory connectionFactory = new ConnectionFactoryImpl(config);
             ControllerImpl controller = new ControllerImpl(
                     ControllerImplConfig.builder().clientConfig(config).build(),
                     connectionFactory.getInternalExecutor());
             ClientFactory clientFactory = new ClientFactoryImpl(scope, controller);
             EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                     new JavaSerializer<>(),
                     EventWriterConfig.builder().build())) {
            final EventProducer eventProducer = new EventProducer(writer, 2);

            prometheusReporter.registerMetric(
                    PRAVEGA_PRODUCER_EVENT_RATE_PER_SECOND,
                    "The event rate of the Pravega producer.",
                    createEventRateChild(eventProducer));

            prometheusReporter.registerMetric(PRAVEGA_NUM_OF_SEGMENTS,
                    "Number of segments of stream",
                    createNumOfSegmentsChild(scope, streamName, controller));

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(eventProducer);

            final Scanner scanner = new Scanner(System.in);
            boolean stop = false;

            while (!stop) {
                if (scanner.hasNextInt()) {
                    final int newRatePerSecond = scanner.nextInt();
                    eventProducer.setRatePerSecond(newRatePerSecond);
                } else if (scanner.hasNext("exit")) {
                    stop = true;
                } else {
                    Thread.sleep(100L);
                }
            }

            executor.shutdown();
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

    private static Gauge.Child createNumOfSegmentsChild(String scope, String streamName, Controller controller) {
        return new Gauge.Child() {
            @Override
            public double get() {
                int streamSize = 0;
                try {
                    streamSize = controller.getCurrentSegments(scope, streamName).get().getSegments().size();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

                return streamSize;
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

    private static class ListenerImpl implements Listener<SegmentNotification> {

        @Override
        public void onNotification(SegmentNotification notification) {
            currentNumberOfSegments.set(notification.getNumOfSegments());
            System.out.println("Num of segments updated: " + notification.getNumOfSegments());
        }
    }
}
