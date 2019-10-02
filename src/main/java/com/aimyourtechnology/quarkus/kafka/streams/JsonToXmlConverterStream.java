package com.aimyourtechnology.quarkus.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.function.BiFunction;
import java.util.function.Function;

class JsonToXmlConverterStream extends ConverterStream {
    JsonToXmlConverterStream(ConverterConfiguration converterConfiguration) {
        super(converterConfiguration);
    }

    @Override
    Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        ValueMapper<String, String> jsonToXmlMapper = createValueMapper();
        KStream<String, String> inputStream = builder.stream(converterConfiguration.inputKafkaTopic, Consumed.with(Serdes.String(), Serdes.String()));
        //        KStream<String, String> jsonStream = inputStream.transformValues(kafkaStreamsTracing.mapValues("xml_to_json", xmlToJsonMapper));
        KStream<String, String> xmlStream = inputStream.mapValues(jsonToXmlMapper);
        xmlStream.to(converterConfiguration.outputKafkaTopic);
        return builder.build();
    }

    private ValueMapper<String, String> createValueMapper() {
        if (shouldUseXmlOuterNode())
            return createValueMapperWithXmlOuterNode();
        return createSimpleValueWrapper();
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
