package acceptance.converter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;

public class ConvertToJsonShould extends ConverterShould {

    private static final String INPUT_TOPIC = "xmlInput";
    private static final String OUTPUT_TOPIC = "jsonOutput";

    @Override
    protected Map<String, String> createCustomEnvProperties() {
        Map<String, String> envProperties = new HashMap<>();
        envProperties.put("INPUT_KAFKA_TOPIC", getInputTopic());
        envProperties.put("OUTPUT_KAFKA_TOPIC", getOutputTopic());
        envProperties.put("APP_NAME", this.getClass().getName());
        envProperties.put("MODE", "xmlToJson");
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
    public void convertsAnyXmlToJson() throws ExecutionException, InterruptedException {
        writeMessageToInputTopic(() -> createXmlMessage());

        assertKafkaJsonMessage();
    }

    private void assertKafkaJsonMessage() {
        Consumer<ConsumerRecord<String, String>> consumerRecordConsumer = cr -> assertRecordValueJson(cr);

        assertKafkaMessage(consumerRecordConsumer);
    }

    private void assertRecordValueJson(ConsumerRecord<String, String> consumerRecord) {
        String value = consumerRecord.value();
        String expectedValue = createJsonMessage();
        assertJsonEquals(expectedValue, value);
    }
}
