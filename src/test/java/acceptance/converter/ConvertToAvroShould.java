package acceptance.converter;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class ConvertToAvroShould extends ConverterShould {

    private static final String INPUT_TOPIC = "xmlInput";
    private static final String OUTPUT_TOPIC = "avroOutput";
    private static final String SUBJECT = OUTPUT_TOPIC + "-value";
    private static final int SUBJECT_VERSION_1 = 1;
    protected String amount = generateRandomStringFromDouble();

    private Schema schema;

    @Container
    protected GenericContainer schemaRegistryContainer =
            new GenericContainer("confluentinc/cp-schema-registry:5.3.0")
            .withNetwork(KAFKA_CONTAINER.getNetwork())
            .withNetworkAliases("schema-registry")
            .withEnv(calculateSchemaRegistryEnvProperties());

    private Map<String, String> calculateSchemaRegistryEnvProperties() {
        Map<String, String> envProperties = new HashMap<>();
        envProperties.put("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        envProperties.put("SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL", KAFKA_CONTAINER.getNetworkAliases().get(0) + ":2181");
        envProperties.put("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", KAFKA_CONTAINER.getNetworkAliases().get(0) + ":9092");
        return envProperties;
    }

    @Override
    protected Map<String, String> createCustomEnvProperties() {
        Map<String, String> envProperties = new HashMap<>();
        envProperties.put("INPUT_KAFKA_TOPIC", getInputTopic());
        envProperties.put("OUTPUT_KAFKA_TOPIC", getOutputTopic());
        envProperties.put("APP_NAME", "XmlToAvroConverter");
        envProperties.put("MODE", "xmlToAvro");
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
    public void convertsAnyXmlToAvro() throws ExecutionException, InterruptedException {
        registerPaymentSchema();
        writeMessageToInputTopic(() -> createPaymentXmlMessage());

        assertAvroMessageHasBeenWrittenToOutputTopic();
    }

    private void registerPaymentSchema() {
        Schema.Parser parser = new Schema.Parser();

        try {
            Schema schema = parser.parse(new File(
                    getClass().getClassLoader().getResource("avro/Payment.avsc").getFile()
            ));
            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(getSchemaRegistryUrl(), 20);

            schemaRegistryClient.register(SUBJECT, schema);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertAvroMessageHasBeenWrittenToOutputTopic() {
        obtainSchema();

        Consumer<ConsumerRecord<String, GenericRecord>> consumerRecordConsumer = cr -> assertRecordValueAvro(cr);

        assertAvroKafkaMessage(consumerRecordConsumer);
    }

    String createPaymentXmlMessage() {
        return String.format(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<Payment>" +
                        "<id>%s</id>\n" +
                        "<amount>%s</amount>" +
                        "</Payment>", orderId, amount
        );
    }

    private void obtainSchema() {
        try {
            readSchemaFromSchemaRegistry();
        } catch (IOException e) {
            System.err.println(e);
            throw new SchemaRegistryIoException(e);
        } catch (RestClientException e) {
            System.err.println(e);
            throw new SchemaRegistryClientException(e);
        }
    }

    private void readSchemaFromSchemaRegistry() throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(getSchemaRegistryUrl(), 20);
        SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(SUBJECT, SUBJECT_VERSION_1);
        schema = schemaRegistryClient.getById(schemaMetadata.getId());
    }

    private String getSchemaRegistryUrl() {
        String host = "http://schema-registry:" + findExposedPortForInternalPort(schemaRegistryContainer, 8081);
        return host;
    }

    protected String generateRandomStringFromDouble() {
        return String.valueOf(new Random().nextDouble());
    }

    private class SchemaRegistryIoException extends RuntimeException {
        public SchemaRegistryIoException(IOException e) {
        }
    }

    private class SchemaRegistryClientException extends RuntimeException {
        public SchemaRegistryClientException(RestClientException e) {
        }
    }

    private void assertRecordValueAvro(ConsumerRecord<String, GenericRecord> consumerRecord) {
        GenericRecord value = consumerRecord.value();
        GenericRecord expectedValue = createAvroMessage();
        assertEquals(expectedValue, value);
    }

    private GenericRecord createAvroMessage() {
        GenericRecord message = new GenericData.Record(schema);
        message.put("id", orderId);
        message.put("amount", 1000.00d);
        return message;
    }
}
