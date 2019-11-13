package com.aimyourtechnology.quarkus.kafka.streams;

import java.util.Optional;

class ConverterConfiguration {
    String inputKafkaTopic;
    String outputKafkaTopic;
    Optional<String> xmlOuterNode;
    Mode mode;
}
