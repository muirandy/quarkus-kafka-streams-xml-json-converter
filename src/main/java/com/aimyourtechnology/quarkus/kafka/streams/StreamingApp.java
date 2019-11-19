package com.aimyourtechnology.quarkus.kafka.streams;

import brave.Tracing;
import brave.kafka.streams.KafkaStreamsTracing;
import brave.sampler.Sampler;
import io.quarkus.runtime.StartupEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.util.Optional;
import java.util.Properties;

@ApplicationScoped
public class StreamingApp {
    @ConfigProperty(name = "mode")
    String mode;

    @ConfigProperty(name = "xmlOuterNode", defaultValue = "")
    String xmlOuterNode;

    @ConfigProperty(name = "inputKafkaTopic")
    String inputKafkaTopic;

    @ConfigProperty(name = "outputKafkaTopic")
    String outputKafkaTopic;

    @ConfigProperty(name = "quarkus.kafka-streams.application-id")
    String appName;

    @ConfigProperty(name = "quarkus.kafka-streams.bootstrap-servers")
    String bootstrapServers;

    @ConfigProperty(name = "zipkin.endpoint")
    String zipkinEndpoint;

    private ConverterStream converterStream;
    private KafkaStreamsTracing kafkaStreamsTracing;
    private KafkaStreams streams;

    void onStart(@Observes StartupEvent event) {
        runStream();
    }

    private void runStream() {
//        kafkaStreamsTracing = configureTracing();
        Topology topology = buildTopology();
        streams = new KafkaStreamsFactory(topology).createStream();

        streams.start();
    }

    private KafkaStreamsTracing configureTracing() {
        AsyncReporter.builder(URLConnectionSender.create(zipkinEndpoint)).build();
        AsyncReporter<Span> asyncReporter = AsyncReporter.create(URLConnectionSender.create(zipkinEndpoint));
        Tracing tracing = Tracing.newBuilder()
                                 .localServiceName(appName)
                                 .sampler(Sampler.ALWAYS_SAMPLE)
                                 .spanReporter(asyncReporter)
                                 .build();
        return KafkaStreamsTracing.create(tracing);
    }

    private Topology buildTopology() {
        System.out.println("app: " + appName);
        System.out.println("mode: " + mode);
        System.out.println("inputKafkaTopic: " + inputKafkaTopic);
        System.out.println("outputKafkaTopic: " + outputKafkaTopic);
        if (xmlOuterNode != null)
            System.out.println("xmlOuterNode: " + xmlOuterNode);

        ConverterConfiguration converterConfiguration = new ConverterConfiguration();
        converterConfiguration.inputKafkaTopic = inputKafkaTopic;
        converterConfiguration.outputKafkaTopic = outputKafkaTopic;
        converterConfiguration.mode = Mode.modeFor(mode);
        converterConfiguration.xmlOuterNode = Optional.ofNullable(xmlOuterNode);

        converterStream = getConverterStream(converterConfiguration);
        return converterStream.buildTopology();
    }

    private ConverterStream getConverterStream(ConverterConfiguration converterConfiguration) {
        if (Mode.XML_TO_JSON.equals(converterConfiguration.mode))
            return new XmlToJsonConverterStream(converterConfiguration, kafkaStreamsTracing);
        else if (Mode.JSON_TO_XML.equals(converterConfiguration.mode))
            return new JsonToXmlConverterStream(converterConfiguration, kafkaStreamsTracing);
        return new ActiveMqConnectorToJsonConverterStream(converterConfiguration, kafkaStreamsTracing);
    }

    private class KafkaStreamsFactory {
        private Topology topology;

        KafkaStreamsFactory(Topology topology) {
            this.topology = topology;
        }

        KafkaStreams createStream() {
            if (useOpenTracing())
                return new KafkaStreams(topology, createStreamingConfig());
            kafkaStreamsTracing = configureTracing();
            return kafkaStreamsTracing.kafkaStreams(topology, createStreamingConfig());
        }

        private boolean useOpenTracing() {
            return zipkinEndpoint == null || zipkinEndpoint.trim().isEmpty();
        }

        private Properties createStreamingConfig() {
            Properties streamingConfig = new Properties();
            streamingConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
            streamingConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            streamingConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamingConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamingConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
            return streamingConfig;
        }
    }
}