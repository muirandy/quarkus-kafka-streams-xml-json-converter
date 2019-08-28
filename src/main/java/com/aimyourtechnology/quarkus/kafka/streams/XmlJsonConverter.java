package com.aimyourtechnology.quarkus.kafka.streams;

import org.json.JSONObject;
import org.json.XML;

class XmlJsonConverter {

    private static final int PRETTY_PRINT_INDENT_FACTOR = 4;

    static String convertXmlToJson(String xmlString) {
        JSONObject xmlJSONObj = XML.toJSONObject(xmlString, true);
        return xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR);
    }

    static String convertJsonToXml(String jsonString) {
        JSONObject json = new JSONObject(jsonString);
        return XML.toString(json);
    }

    static String readXmlFieldFromJson(String field, String payload) {
        JSONObject json = new JSONObject(payload);
        return json.getString(field);
    }
}
