package com.aimyourtechnology.quarkus.kafka.streams;

enum Mode {
    XML_TO_JSON,
    JSON_TO_XML;

    static Mode modeFor(String s) {
        if (isXmlToJson(s))
            return XML_TO_JSON;
        return JSON_TO_XML;
    }

    private static boolean isXmlToJson(String s) {
        return s.toLowerCase().startsWith("xml");
    }
}
