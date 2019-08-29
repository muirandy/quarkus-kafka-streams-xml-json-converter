package com.aimyourtechnology.quarkus.kafka.streams;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ModeTest {

    @Test
    public void xmlToJson() {
        assertEquals(Mode.XML_TO_JSON, Mode.modeFor("xmlToJson"));
        assertEquals(Mode.XML_TO_JSON, Mode.modeFor("XML_TO_JSON"));
    }

    @Test
    public void jsonToXml() {
        assertEquals(Mode.JSON_TO_XML, Mode.modeFor("jsonToXml"));
    }
}
