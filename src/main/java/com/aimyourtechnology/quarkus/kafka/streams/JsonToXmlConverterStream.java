package com.aimyourtechnology.quarkus.kafka.streams;

import brave.kafka.streams.KafkaStreamsTracing;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.function.BiFunction;
import java.util.function.Function;

class JsonToXmlConverterStream extends ConverterStream {
    JsonToXmlConverterStream(ConverterConfiguration converterConfiguration, KafkaStreamsTracing kafkaStreamsTracing) {
        super(converterConfiguration, kafkaStreamsTracing);
    }

    @Override
    Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        ValueMapper<String, String> jsonToXmlMapper = createValueMapper();
        KStream<String, String> inputStream = builder.stream(converterConfiguration.inputKafkaTopic, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> xmlStream = createXmlStream(jsonToXmlMapper, inputStream);
        xmlStream.to(converterConfiguration.outputKafkaTopic);
        return builder.build();
    }

    private ValueMapper<String, String> createValueMapper() {
        if (shouldUseXmlOuterNode())
            return createValueMapperWithXmlOuterNode();
        return createSimpleValueWrapper();
    }

    private KStream<String, String> createXmlStream(ValueMapper<String, String> jsonToXmlMapper, KStream<String, String> inputStream) {
        if (null == kafkaStreamsTracing)
            return inputStream.mapValues(jsonToXmlMapper);
        return inputStream.transformValues(kafkaStreamsTracing.mapValues("json_to_xml", jsonToXmlMapper));
    }

    private boolean shouldUseXmlOuterNode() {
        return converterConfiguration.xmlOuterNode.isPresent()
                && !converterConfiguration.xmlOuterNode.get().trim().isEmpty();
    }

    private ValueMapper<String, String> createValueMapperWithXmlOuterNode() {
        BiFunction<String, String, String> jsonToXml = XmlJsonConverter::convertJsonToXmlWithXmlOuterNode;
        return xmlString -> jsonToXml.apply(xmlString, converterConfiguration.xmlOuterNode.get());
    }

    private ValueMapper<String, String> createSimpleValueWrapper() {
        Function<String, String> jsonToXml = XmlJsonConverter::convertJsonToXml;
        return xmlString -> jsonToXml.apply(xmlString);
    }

}
