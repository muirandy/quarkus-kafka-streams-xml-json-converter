package com.aimyourtechnology.quarkus.kafka.streams;

import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.Optional;

@ApplicationScoped
public class StreamingApp {
    @ConfigProperty(name = "mode")
    String mode;

    @ConfigProperty(name = "xmlOuterNode", defaultValue = "")
    String xmlOuterNode;

    @ConfigProperty(name = "inputKafkaTopic")
    String inputKafkaTopic;

    @ConfigProperty(name = "outputKafkaTopic")
    String outputKafkaTopic;


    private ConverterStream converterStream;

    @Produces
    public Topology buildTopology() {
        System.out.println("mode: " + mode);
        System.out.println("inputKafkaTopic: " + inputKafkaTopic);
        System.out.println("outputKafkaTopic: " + outputKafkaTopic);
        if (xmlOuterNode != null)
            System.out.println("xmlOuterNode: " + xmlOuterNode);


        ConverterConfiguration converterConfiguration = new ConverterConfiguration();
        converterConfiguration.inputKafkaTopic = inputKafkaTopic;
        converterConfiguration.outputKafkaTopic = outputKafkaTopic;
        converterConfiguration.mode = Mode.modeFor(mode);
        converterConfiguration.xmlOuterNode = Optional.ofNullable(xmlOuterNode);

        converterStream = getConverterStream(converterConfiguration);
        return converterStream.buildTopology();
    }

    private ConverterStream getConverterStream(ConverterConfiguration converterConfiguration) {
        if (Mode.XML_TO_JSON.equals(converterConfiguration.mode))
            return new XmlToJsonConverterStream(converterConfiguration);
        else if (Mode.JSON_TO_XML.equals(converterConfiguration.mode))
            return new JsonToXmlConverterStream(converterConfiguration);
        return new ActiveMqConnectorToJsonConverterStream(converterConfiguration);
    }
}
