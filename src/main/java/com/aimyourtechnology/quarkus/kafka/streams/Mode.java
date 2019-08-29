package com.aimyourtechnology.quarkus.kafka.streams;

enum Mode {
    XML_TO_JSON,
    JSON_TO_XML,
    ACTIVE_MQ_CONNECTOR_TO_JSON;

    static Mode modeFor(String s) {
        if (isXmlToJson(s))
            return XML_TO_JSON;
        else if(isJsonToXml(s))
            return JSON_TO_XML;
        return ACTIVE_MQ_CONNECTOR_TO_JSON;
    }

    private static boolean isJsonToXml(String s) {
        return s.toLowerCase().startsWith("json");
    }

    private static boolean isXmlToJson(String s) {
        return s.toLowerCase().startsWith("xml");
    }
}
