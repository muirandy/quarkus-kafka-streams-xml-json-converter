package acceptance.converter;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ConvertToAvroShould extends TestHelper {

    private static final String INPUT_TOPIC = "xmlInput";
    private static final String OUTPUT_TOPIC = "avroOutput";
    private static final String SUBJECT = OUTPUT_TOPIC + "-value";
    private static final int SUBJECT_VERSION_1 = 1;
    private static final String SCHEMA_REGISTRY = "schema-registry";
    private static final int SCHEMA_REGISTRY_PORT = 8081;
    private static final File dockerComposeFile = getFileFromPath("docker-compose-convert-to-avro.yml");

    private String amount = generateRandomStringFromDouble();
    private Schema schema;

    @ClassRule
    public static DockerComposeContainer environment =
            new DockerComposeContainer(dockerComposeFile)
                    .waitingFor("broker", Wait.forLogMessage(".*started.*\\n", 1))
                    .withExposedService(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT)
                    .withLocalCompose(true);

    @BeforeEach
    public void setup() {
        createTopics();
    }

    @Test
    public void convertsAnyXmlToAvro() throws ExecutionException, InterruptedException {
        registerPaymentSchema();
        writeMessageToInputTopic(() -> createPaymentXmlMessage());

        assertAvroMessageHasBeenWrittenToOutputTopic();
    }

    public String getInputTopic() {
        return INPUT_TOPIC;
    }

    protected String getOutputTopic() {
        return OUTPUT_TOPIC;
    }

    protected Properties getKafkaProperties() {
        Properties props = new Properties();

        String bootstrapServers = "http://broker:9092";
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, TestHelper.class.getName());
        return props;
    }

    private void registerPaymentSchema() {
        Schema.Parser parser = new Schema.Parser();

        try {
            Schema schema = parser.parse(getFileFromPath("avro/Payment.avsc"));
            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(getSchemaRegistryUrl(), 20);
            schemaRegistryClient.register(SUBJECT, schema);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    private static File getFileFromPath(String path) {
        return new File(ConvertToAvroShould.class.getClassLoader().getResource(path).getFile());
    }

    private String getSchemaRegistryUrl() {
        return "http://localhost:8081";
    }

    private void assertAvroMessageHasBeenWrittenToOutputTopic() {
        obtainSchema();

        Consumer<ConsumerRecord<String, GenericRecord>> consumerRecordConsumer = cr -> assertRecordValueAvro(cr);

        assertAvroKafkaMessage(consumerRecordConsumer);
    }

    private String createPaymentXmlMessage() {
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


    @Override
    Properties getProperties() {
        return getKafkaProperties();
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

    private KafkaConsumer<String, GenericRecord> createKafkaAvroConsumer(Properties props) {
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(getOutputTopic()));
        Duration immediately = Duration.ofSeconds(0);
        consumer.poll(immediately);
        return consumer;
    }

    private ConsumerRecords<String, GenericRecord> pollForAvroResults() {
        KafkaConsumer<String, GenericRecord> consumer = createKafkaAvroConsumer(getProperties());
        Duration duration = Duration.ofSeconds(4);
        return consumer.poll(duration);
    }

    boolean foundExpectedRecord(String key) {
        return orderId.equals(key);
    }

    private void assertAvroKafkaMessage(Consumer<ConsumerRecord<String, GenericRecord>> consumerRecordConsumer) {
        ConsumerRecords<String, GenericRecord> recs = pollForAvroResults();
        assertFalse(recs.isEmpty());

        Spliterator<ConsumerRecord<String, GenericRecord>> spliterator = Spliterators.spliteratorUnknownSize(recs.iterator(), 0);
        Stream<ConsumerRecord<String, GenericRecord>> consumerRecordStream = StreamSupport.stream(spliterator, false);
        Optional<ConsumerRecord<String, GenericRecord>> expectedConsumerRecord = consumerRecordStream.filter(cr -> foundExpectedRecord(cr.key()))
                .findAny();
        expectedConsumerRecord.ifPresent(consumerRecordConsumer);
        if (!expectedConsumerRecord.isPresent())
            fail("Did not find expected record");
    }
}
