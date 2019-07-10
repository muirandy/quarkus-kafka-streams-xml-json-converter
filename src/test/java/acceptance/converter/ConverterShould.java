package acceptance.converter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
public abstract class ConverterShould {
    private static final String ENV_KEY_KAFKA_BROKER_SERVER = "KAFKA_BROKER_SERVER";
    private static final String ENV_KEY_KAFKA_BROKER_PORT = "KAFKA_BROKER_PORT";


    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer("5.2.1").withEmbeddedZookeeper()
                                                                                     .waitingFor(Wait.forLogMessage(".*Launching kafka.*\\n", 1))
                                                                                     .waitingFor(Wait.forLogMessage(".*started.*\\n", 1));
    private static final String KAFKA_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String KAFKA_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    @Container
    private GenericContainer converterContainer = new GenericContainer("quarkus/quarkus-kafka-streams-xml-json-converter:latest")
            .withNetwork(KAFKA_CONTAINER.getNetwork())
            .withEnv(calculateEnvProperties())
            .waitingFor(Wait.forLogMessage(".*Stream manager initializing.*\\n", 1))
            .waitingFor(Wait.forLogMessage(".*Quarkus .* started.*\\n", 1));

    protected String randomValue = generateRandomString();
    protected String orderId = generateRandomString();

    private Map<String, String> calculateEnvProperties() {
        Map<String, String> envProperties = new HashMap<>();
        String bootstrapServers = KAFKA_CONTAINER.getNetworkAliases().get(0);
        envProperties.put(ENV_KEY_KAFKA_BROKER_SERVER, bootstrapServers);
        envProperties.put(ENV_KEY_KAFKA_BROKER_PORT, "" + 9092);
        envProperties.putAll(createCustomEnvProperties());
        return envProperties;
    }

    protected abstract Map<String, String> createCustomEnvProperties();

    @BeforeEach
    public void setup() {
        assertTrue(KAFKA_CONTAINER.isRunning());
        assertTrue(converterContainer.isRunning());
        waitForDockerEnvironment();
    }

    private void waitForDockerEnvironment() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterEach
    public void tearDown() {
        System.out.println("Kafka Logs = " + KAFKA_CONTAINER.getLogs());
        System.out.println("XmlJsonConverter Logs = " + converterContainer.getLogs());
    }

    ProducerRecord createKafkaProducerRecord(Supplier<String> stringGenerator) {
        return new ProducerRecord(getInputTopic(), orderId, stringGenerator.get());
    }

    ConsumerRecords<String, String> pollForResults() {
        KafkaConsumer<String, String> consumer = createKafkaConsumer(getProperties());
        Duration duration = Duration.ofSeconds(4);
        return consumer.poll(duration);
    }

    private KafkaConsumer<String, String> createKafkaConsumer(Properties props) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(getOutputTopic()));
        Duration immediately = Duration.ofSeconds(0);
        consumer.poll(immediately);
        return consumer;
    }

    protected abstract String getInputTopic();
    protected abstract String getOutputTopic();

    Properties getProperties() {
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        //        String bootstrapServers = KAFKA_CONTAINER.getNetworkAliases().get(0) + ":9092";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getName());
        return props;
    }

    boolean foundExpectedRecord(String key) {
        return orderId.equals(key);
    }

    String createXmlMessage() {
        return String.format(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<order>" +
                        "<orderId>%s</orderId>\n" +
                        "<randomValue>%s</randomValue>" +
                        "</order>", orderId, randomValue
        );
    }

    String createJsonMessage() {
        return String.format(
                "{" +
                        "  \"order\":{" +
                        "    \"orderId\":%s," +
                        "    \"randomValue\":%s" +
                        "  }" +
                        "}",
                orderId, randomValue
        );
    }

    protected String generateRandomString() {
        return String.valueOf(new Random().nextLong());
    }

    protected void writeMessageToInputTopic(Supplier<String> stringSupplier) throws ExecutionException, InterruptedException {
        new KafkaProducer<String, String>(getProperties()).send(createKafkaProducerRecord(stringSupplier)).get();
    }

    protected void assertKafkaMessage(Consumer<ConsumerRecord<String, String>> consumerRecordConsumer) {
        ConsumerRecords<String, String> recs = pollForResults();
        assertFalse(recs.isEmpty());

        Spliterator<ConsumerRecord<String, String>> spliterator = Spliterators.spliteratorUnknownSize(recs.iterator(), 0);
        Stream<ConsumerRecord<String, String>> consumerRecordStream = StreamSupport.stream(spliterator, false);
        Optional<ConsumerRecord<String, String>> expectedConsumerRecord = consumerRecordStream.filter(cr -> foundExpectedRecord(cr.key()))
                                                                                              .findAny();
        expectedConsumerRecord.ifPresent(consumerRecordConsumer);
        if (!expectedConsumerRecord.isPresent())
            fail("Did not find expected record");
    }
}
