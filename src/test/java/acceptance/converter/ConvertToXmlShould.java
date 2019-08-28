package acceptance.converter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.xmlunit.assertj.XmlAssert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;


public class ConvertToXmlShould extends ConverterShould {

    private static final String INPUT_TOPIC = "jsonInput";
    private static final String OUTPUT_TOPIC = "xmlOutput";

    @Override
    protected Map<String, String> createCustomEnvProperties() {
        Map<String, String> envProperties = new HashMap<>();
        envProperties.put("INPUT_KAFKA_TOPIC", getInputTopic());
        envProperties.put("OUTPUT_KAFKA_TOPIC", getOutputTopic());
        envProperties.put("APP_NAME", "JsonToXmlConverter");
        envProperties.put("MODE", "jsonToXml");
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
    public void convertsAnyJsonToXml() throws ExecutionException, InterruptedException {
        writeMessageToInputTopic(() -> createJsonMessage());

        assertKafkaXmlMessage();
    }

    private void assertKafkaXmlMessage() {
        Consumer<ConsumerRecord<String, String>> consumerRecordConsumer = cr -> assertRecordValueXml(cr);

        assertKafkaMessage(consumerRecordConsumer);
    }

    private void assertRecordValueXml(ConsumerRecord<String, String> consumerRecord) {
        String value = consumerRecord.value();
        String expectedValue = createXmlMessage();
        XmlAssert.assertThat(expectedValue).and(value).ignoreWhitespace().areIdentical();
    }
}
