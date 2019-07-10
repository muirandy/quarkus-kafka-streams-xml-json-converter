package com.aimyourtechnology.quarkus.kafka.streams;

class ConverterConfiguration {
    String appName;
    String kafkaBrokerServer;
    String kafkaBrokerPort;
    String inputKafkaTopic;
    String outputKafkaTopic;
    Mode mode;
}
