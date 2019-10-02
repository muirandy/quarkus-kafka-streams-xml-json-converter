package acceptance.converter;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class TestHelper {
    private static final String ENV_KEY_KAFKA_BROKER_SERVER = "KAFKA_BROKER_SERVER";
    private static final String ENV_KEY_KAFKA_BROKER_PORT = "KAFKA_BROKER_PORT";


    static final String KAFKA_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    static final String KAFKA_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";



    protected String randomValue = generateRandomString();
    protected String orderId = generateRandomString();

//    private Map<String, String> calculateEnvProperties() {
//        createTopics();
//        Map<String, String> envProperties = new HashMap<>();
//        String bootstrapServers = KAFKA_CONTAINER.getNetworkAliases().get(0);
//        envProperties.put(ENV_KEY_KAFKA_BROKER_SERVER, bootstrapServers);
//        envProperties.put(ENV_KEY_KAFKA_BROKER_PORT, "" + 9092);
//        envProperties.putAll(createCustomEnvProperties());
//        return envProperties;
//    }

//    private void createTopics() {
//        AdminClient adminClient = AdminClient.create(getKafkaProperties());
//
//        CreateTopicsResult createTopicsResult = adminClient.createTopics(getTopics(), new CreateTopicsOptions().timeoutMs(1000));
//        Map<String, KafkaFuture<Void>> futureResults = createTopicsResult.values();
//        futureResults.values().forEach(f -> {
//            try {
//                f.get(1000, TimeUnit.MILLISECONDS);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            } catch (TimeoutException e) {
//                e.printStackTrace();
//            }
//        });
//        adminClient.close();
//    }

//    protected static Properties getKafkaProperties() {
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
//        props.put("acks", "all");
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, ConverterShould.class.getName());
//        return props;
//    }

    protected List<NewTopic> getTopics() {
        return getTopicNames().stream()
                .map(n -> new NewTopic(n, 1, (short) 1))
                .collect(Collectors.toList());
    }

    protected List<String> getTopicNames() {
        List<String> topicNames = new ArrayList<>();
        topicNames.add(getInputTopic());
        topicNames.add(getOutputTopic());
        return topicNames;
    }

    ProducerRecord createKafkaProducerRecord(Supplier<String> stringGenerator) {
        return new ProducerRecord(getInputTopic(), orderId, stringGenerator.get());
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

    String generateRandomString() {
        return String.valueOf(new Random().nextLong());
    }

    protected void writeMessageToInputTopic(Supplier<String> stringSupplier) throws ExecutionException, InterruptedException {
        new KafkaProducer<String, String>(getProperties()).send(createKafkaProducerRecord(stringSupplier)).get();
    }

    abstract Properties getProperties();

    void createTopics() {
        AdminClient adminClient = AdminClient.create(getKafkaProperties());

        CreateTopicsResult createTopicsResult = adminClient.createTopics(getTopics(), new CreateTopicsOptions().timeoutMs(1000));
        Map<String, KafkaFuture<Void>> futureResults = createTopicsResult.values();
        futureResults.values().forEach(f -> {
            try {
                f.get(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        });
        adminClient.close();
    }
    abstract Properties getKafkaProperties();

    String generateRandomStringFromDouble() {
        return String.valueOf(new Random().nextDouble());
    }

}
