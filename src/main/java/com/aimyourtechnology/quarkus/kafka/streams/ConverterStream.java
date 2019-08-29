package com.aimyourtechnology.quarkus.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.Properties;

abstract class ConverterStream {

    private final Properties streamingConfig;
//    private final KafkaStreamsTracing kafkaStreamsTracing;
    protected ConverterConfiguration converterConfiguration;
    private String bootstrapServers;
    private KafkaStreams streams;

    ConverterStream(ConverterConfiguration converterConfiguration) {
        this.converterConfiguration = converterConfiguration;
        bootstrapServers = converterConfiguration.kafkaBrokerServer + ":" + converterConfiguration.kafkaBrokerPort;
        streamingConfig = createStreamingConfig();
//        kafkaStreamsTracing = configureTracing();
    }

    private Properties createStreamingConfig() {
        Properties streamingConfig;
        streamingConfig = new Properties();
        streamingConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, converterConfiguration.appName);
        streamingConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamingConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamingConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamingConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return streamingConfig;
    }

//    KafkaStreamsTracing configureTracing() {
//        KafkaSender kafkaSender = KafkaSender.newBuilder().bootstrapServers(bootstrapServers).build();
//        AsyncReporter<Span> asyncReporter = AsyncReporter.builder(kafkaSender).build();
//        Tracing tracing = Tracing.newBuilder().localServiceName(converterConfiguration.appName).sampler(Sampler.ALWAYS_SAMPLE).spanReporter(asyncReporter).build();
//        return KafkaStreamsTracing.create(tracing);
//    }

    void runTopology() {
        Topology topology = buildTopology();
//        streams = kafkaStreamsTracing.kafkaStreams(topology, streamingConfig);
        streams = new KafkaStreams(topology, streamingConfig);
        streams.start();
    }

    void shutdown() {
        streams.close();
    }

    abstract Topology buildTopology();

}
