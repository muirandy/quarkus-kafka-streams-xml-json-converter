package com.aimyourtechnology.quarkus.kafka.streams;

import org.junit.jupiter.api.Test;
import org.xmlunit.assertj.XmlAssert;

import java.util.Random;

import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;

public class XmlAvroConverterTest {

    private String orderId = "" + new Random().nextInt();
    private String traceyId = "" + new Random().nextInt();

    @Test
    void convertsXmlToAvro() {
        String jsonString = XmlJsonConverter.convertXmlToJson(xmlValue());
        assertJsonEquals(jsonValue(), jsonString);

        String avroString = JsonAvroConverter.convertJsonToAvro(jsonValue());

        assertJsonEquals(avroValue(), avroString);
    }

    @Test
    void convertsJsonToXml() {
        String xmlString = XmlJsonConverter.convertJsonToXml(jsonValue());

        XmlAssert.assertThat(xmlValue()).and(xmlString).ignoreWhitespace().areIdentical();
    }

    @Test
    void readsXmlFromJsonPayload() {
        String xmlString = XmlJsonConverter.readXmlFieldFromJson("XML", activeMqConnectorJson());

        XmlAssert.assertThat(xmlValue()).and(xmlString).ignoreWhitespace().areIdentical();
    }

    private String activeMqConnectorJson() {
        return String.format("{"
                + "\"ORDER_ID\":\"%s\","
                + "\"TRACEY_ID\":\"%s\","
                + "\"XML\":\"%s\""
                + "}", orderId, traceyId, createXmlMessage());
    }

    private String createXmlMessage() {
        return String.format(
                "<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>" +
                        "<order>" +
                        "<orderId>%s</orderId>" +
                        "</order>", orderId
        );
    }

    private String xmlValue() {
        return String.format(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<order>" +
                        "<orderId>%s</orderId>\n" +
                        "</order>", orderId
        );
    }

    private String jsonValue() {
        return String.format(
                "{" +
                        "  \"order\":{" +
                        "    \"orderId\":\"%s\"" +
                        "  }" +
                        "}",
                orderId
        );
    }

    private String avroValue() {
        return String.format(
                "{" +
                        "  \"order\":{" +
                        "    \"orderId\":\"%s\"" +
                        "  }" +
                        "}",
                orderId
        );
    }
}
