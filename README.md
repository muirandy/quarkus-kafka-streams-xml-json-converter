# Prerequisites:
https://quarkus.io/get-started/
gu install native-image


# Xml <-> Json Converter for Kafka Streams - Quarkus & Kafka Streams Implementation

## JVM (least beneficial!)
### Build
```
mvn clean package 
```

### Run
```
./macJdk.sh
```
## JVM Image on Docker
### Build
```
mvn clean package
docker build -f src/main/docker/Dockerfile.jvm2 -t quarkus/quarkus-kafka-streams-xml-json-converter
```

## Native Image on macOS
### Build
```
mvn clean package -Pnative
```

### Run
```
./macNative.sh
```

## Native Image on Docker
### Build
```
./mvnw clean package -Pnative -Dquarkus.native.container-build=true -Dnative-image.docker-build=true
docker build -f src/main/docker/Dockerfile.native -t quarkus/quarkus-kafka-streams-xml-json-converter .

```

### Run
```
./dockerNative.sh
```

## Acceptance Tests
Run any of the tests from `test/java/acceptance/converter`. You will need a Docker environment, but the tests will spin 
up Kafka (so make sure its not already running!).

#Update hosts file with values below
```
sudo vi /etc/hosts

127.0.0.1	schema-registry
127.0.0.1	broker
```