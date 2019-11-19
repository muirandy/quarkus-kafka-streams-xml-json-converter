package com.aimyourtechnology.quarkus.kafka.streams;

import brave.kafka.streams.KafkaStreamsTracing;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONObject;

import java.util.function.Function;

class ActiveMqConnectorToJsonConverterStream extends ConverterStream {
    ActiveMqConnectorToJsonConverterStream(ConverterConfiguration converterConfiguration, KafkaStreamsTracing kafkaStreamsTracing) {
        super(converterConfiguration, kafkaStreamsTracing);
    }

    @Override
    Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        Function<String, String> mqToJson = ActiveMqConnectorToJsonConverterStream::convertMqToJson;
        ValueMapper<String, String> xmlToJsonMapper = mqString -> mqToJson.apply(mqString);
        KStream<String, String> inputStream = builder.stream(converterConfiguration.inputKafkaTopic, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> jsonStream = inputStream.transformValues(kafkaStreamsTracing.mapValues("mqXml_to_json", xmlToJsonMapper));
        //        KStream<String, String> jsonStream = inputStream.mapValues(xmlToJsonMapper);
        jsonStream.to(converterConfiguration.outputKafkaTopic);
        return builder.build();
    }

    private static String convertMqToJson(String payload) {
        String xml = XmlJsonConverter.readXmlFieldFromJson("XML", payload);
        String traceyId = XmlJsonConverter.readXmlFieldFromJson("TRACEY_ID", payload);

        String json = XmlJsonConverter.convertXmlToJson(xml);
        return JsonAppender.append(json, traceyId);
    }

    private static class JsonAppender {
        private static final int PRETTY_PRINT_INDENT_FACTOR = 4;

        static String append(String jsonString, String traceyId) {
            JSONObject json = new JSONObject(jsonString);
            json.put("traceId", traceyId);
            return json.toString(PRETTY_PRINT_INDENT_FACTOR);
        }
    }

}
