package com.aimyourtechnology.quarkus.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.function.Function;

class JsonToXmlConverterStream extends ConverterStream {
    JsonToXmlConverterStream(ConverterConfiguration converterConfiguration) {
        super(converterConfiguration);
    }

    @Override
    Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Function<String, String> jsonToXml = XmlJsonConverter::convertJsonToXml;
        ValueMapper<String, String> jsonToXmlMapper = xmlString -> jsonToXml.apply(xmlString);
        KStream<String, String> inputStream = builder.stream(converterConfiguration.inputKafkaTopic, Consumed.with(Serdes.String(), Serdes.String()));
        //        KStream<String, String> jsonStream = inputStream.transformValues(kafkaStreamsTracing.mapValues("xml_to_json", xmlToJsonMapper));
        KStream<String, String> xmlStream = inputStream.mapValues(jsonToXmlMapper);
        xmlStream.to(converterConfiguration.outputKafkaTopic);
        return builder.build();
    }
}
