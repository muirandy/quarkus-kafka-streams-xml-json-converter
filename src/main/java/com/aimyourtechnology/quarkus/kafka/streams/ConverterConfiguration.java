package com.aimyourtechnology.quarkus.kafka.streams;

import java.util.Optional;

class ConverterConfiguration {
    String appName;
    String kafkaBrokerServer;
    String kafkaBrokerPort;
    String inputKafkaTopic;
    String outputKafkaTopic;
    Optional<String> xmlOuterNode;
    Mode mode;
}
