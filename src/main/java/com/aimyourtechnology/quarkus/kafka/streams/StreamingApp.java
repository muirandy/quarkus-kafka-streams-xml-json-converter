package com.aimyourtechnology.quarkus.kafka.streams;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.util.Optional;

@ApplicationScoped
public class StreamingApp {
    @ConfigProperty(name = "mode")
    String mode;

    @ConfigProperty(name = "xmlOuterNode", defaultValue = "")
    String xmlOuterNode;

    @ConfigProperty(name = "appName")
    String appName;

    @ConfigProperty(name = "kafkaBrokerServer")
    String kafkaBrokerServer;

    @ConfigProperty(name = "kafkaBrokerPort")
    String kafkaBrokerPort;

    @ConfigProperty(name = "inputKafkaTopic")
    String inputKafkaTopic;

    @ConfigProperty(name = "outputKafkaTopic")
    String outputKafkaTopic;


    private ConverterStream converterStream;

    void onStart(@Observes StartupEvent event) {
        loadSomeClasses();

        System.out.println("mode: " + mode);
        System.out.println("appName: " + appName);
        System.out.println("kafkaBrokerServer: " + kafkaBrokerServer);
        System.out.println("kafkaBrokerPort: " + kafkaBrokerPort);
        System.out.println("inputKafkaTopic: " + inputKafkaTopic);
        System.out.println("outputKafkaTopic: " + outputKafkaTopic);
        if (xmlOuterNode != null)
            System.out.println("xmlOuterNode: " + xmlOuterNode);


        ConverterConfiguration converterConfiguration = new ConverterConfiguration();
        converterConfiguration.appName = appName;
        converterConfiguration.inputKafkaTopic = inputKafkaTopic;
        converterConfiguration.outputKafkaTopic = outputKafkaTopic;
        converterConfiguration.kafkaBrokerServer = kafkaBrokerServer;
        converterConfiguration.kafkaBrokerPort = kafkaBrokerPort;
        converterConfiguration.mode = Mode.modeFor(mode);
        converterConfiguration.xmlOuterNode = Optional.ofNullable(xmlOuterNode);

        converterStream = getConverterStream(converterConfiguration);

        converterStream.runTopology();
    }

    private void loadSomeClasses() {
        Serdes.StringSerde stringSerde = new Serdes.StringSerde();
        System.out.println(stringSerde);
    }

    private ConverterStream getConverterStream(ConverterConfiguration converterConfiguration) {
        if (Mode.XML_TO_JSON.equals(converterConfiguration.mode))
            return new XmlToJsonConverterStream(converterConfiguration);
        else if (Mode.JSON_TO_XML.equals(converterConfiguration.mode))
            return new JsonToXmlConverterStream(converterConfiguration);
        return new ActiveMqConnectorToJsonConverterStream(converterConfiguration);
    }

    void onStop(@Observes ShutdownEvent event) {
        converterStream.shutdown();
    }
}
