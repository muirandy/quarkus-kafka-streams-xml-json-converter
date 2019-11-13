package com.aimyourtechnology.quarkus.kafka.streams;

import org.apache.kafka.streams.Topology;

abstract class ConverterStream {

//    private final KafkaStreamsTracing kafkaStreamsTracing;
    protected ConverterConfiguration converterConfiguration;

    ConverterStream(ConverterConfiguration converterConfiguration) {
        this.converterConfiguration = converterConfiguration;
//        kafkaStreamsTracing = configureTracing();
    }

//    KafkaStreamsTracing configureTracing() {
//        KafkaSender kafkaSender = KafkaSender.newBuilder().bootstrapServers(bootstrapServers).build();
//        AsyncReporter<Span> asyncReporter = AsyncReporter.builder(kafkaSender).build();
//        Tracing tracing = Tracing.newBuilder().localServiceName(converterConfiguration.appName).sampler(Sampler.ALWAYS_SAMPLE).spanReporter(asyncReporter).build();
//        return KafkaStreamsTracing.create(tracing);
//    }

    abstract Topology buildTopology();
}
