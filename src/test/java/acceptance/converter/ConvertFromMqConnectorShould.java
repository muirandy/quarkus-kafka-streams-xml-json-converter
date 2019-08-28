package acceptance.converter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;

public class ConvertFromMqConnectorShould extends ConverterShould {

    private static final String INPUT_TOPIC = "activeMqInput";
    private static final String OUTPUT_TOPIC = "jsonOutput";

    private String traceyId = generateRandomString();

    @Override
    protected Map<String, String> createCustomEnvProperties() {
        Map<String, String> envProperties = new HashMap<>();
        envProperties.put("INPUT_KAFKA_TOPIC", getInputTopic());
        envProperties.put("OUTPUT_KAFKA_TOPIC", getOutputTopic());
        envProperties.put("APP_NAME", "activeMqToJsonConverter");
        envProperties.put("MODE", "mqConnector");
        return envProperties;
    }

    @Override
    public String getInputTopic() {
        return INPUT_TOPIC;
    }

    @Override
    protected String getOutputTopic() {
        return OUTPUT_TOPIC;
    }

    @Test
    void convertActiveMqConnectorJsonWithXmlToJson() throws ExecutionException, InterruptedException {
        writeMessageToInputTopic(() -> createMqConnectorMessage());

        assertKafkaMqMessageEquals();
    }

    private void assertKafkaMqMessageEquals() {
        Consumer<ConsumerRecord<String, String>> consumerRecordConsumer = cr -> assertRecordValueMqJson(cr);

        assertKafkaMessage(consumerRecordConsumer);
    }

    private void assertRecordValueMqJson(ConsumerRecord<String, String> consumerRecord) {
        String value = consumerRecord.value();
        assertJsonEquals(createJsonMessage(), value);
    }

    @Override
    String createJsonMessage() {
        return String.format("{\"order\":{\"orderId\":\"%s\"" +
                        "},\"traceId\":\"%s\"}",
                orderId, traceyId, "${json-unit.ignore}"
        );
    }

    private String createMqConnectorMessage() {
        return String.format("{"
                + "\"ORDER_ID\":\"%s\","
                + "\"TRACEY_ID\":\"%s\","
                + "\"XML\":\"%s\""
                + "}", orderId, traceyId, createEscapedXmlMessage());
    }

    private String createEscapedXmlMessage() {
        return String.format(
                "<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>" +
                        "<order>" +
                        "<orderId>%s</orderId>" +
                        "</order>", orderId
        );
    }

}
