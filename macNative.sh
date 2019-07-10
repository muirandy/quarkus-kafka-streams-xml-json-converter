#!/usr/bin/env bash

export KAFKA_BROKER_SERVER="localhost"
export KAFKA_BROKER_PORT="9092"
export INPUT_KAFKA_TOPIC="incoming.op.msgs"
export OUTPUT_KAFKA_TOPIC="modify.op.msgs"
export APP_NAME="sns-incoming-operator-messages-converter4"
export MODE="xlToJson"

./target/quarkusKafkaStreamsXmlJsonConverter-1.0-SNAPSHOT-runner -Xmx48M