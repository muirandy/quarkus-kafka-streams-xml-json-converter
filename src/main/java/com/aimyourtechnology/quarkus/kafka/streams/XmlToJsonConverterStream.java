package com.aimyourtechnology.quarkus.kafka.streams;

import brave.kafka.streams.KafkaStreamsTracing;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.function.Function;

class XmlToJsonConverterStream extends ConverterStream {

    XmlToJsonConverterStream(ConverterConfiguration converterConfiguration, KafkaStreamsTracing kafkaStreamsTracing) {
        super(converterConfiguration, kafkaStreamsTracing);
    }


    @Override
    Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Function<String, String> xmlToJson = XmlJsonConverter::convertXmlToJson;
        ValueMapper<String, String> xmlToJsonMapper = xmlString -> xmlToJson.apply(xmlString);
        KStream<String, String> inputStream = builder.stream(converterConfiguration.inputKafkaTopic, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> jsonStream = createJsonStream(xmlToJsonMapper, inputStream);
        jsonStream.to(converterConfiguration.outputKafkaTopic);
        return builder.build();
    }

    private KStream<String, String> createJsonStream(ValueMapper<String, String> xmlToJsonMapper, KStream<String, String> inputStream) {
        if (null == kafkaStreamsTracing)
            return inputStream.mapValues(xmlToJsonMapper);
        return inputStream.transformValues(kafkaStreamsTracing.mapValues("xml_to_json", xmlToJsonMapper));
    }
}
