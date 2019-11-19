package com.aimyourtechnology.quarkus.kafka.streams;

import brave.kafka.streams.KafkaStreamsTracing;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

abstract class ConverterStream {

    protected ConverterConfiguration converterConfiguration;
    protected KafkaStreamsTracing kafkaStreamsTracing;

    ConverterStream(ConverterConfiguration converterConfiguration, KafkaStreamsTracing kafkaStreamsTracing) {
        this.converterConfiguration = converterConfiguration;
        this.kafkaStreamsTracing = kafkaStreamsTracing;
    }

    abstract Topology buildTopology();

}
