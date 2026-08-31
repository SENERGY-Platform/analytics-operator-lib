/*
 * Copyright 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.infai.ses.senergy.utils.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import net.logstash.logback.encoder.LogstashEncoder;
import org.infai.ses.senergy.utils.Baggage;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Baggage.customFields is only useful if the encoder logback.xml configures
 * actually writes those fields onto a record. This drives that encoder directly,
 * rather than asserting on the string Baggage produced, so the test fails if the
 * encoder ever stops accepting the shape.
 */
public class BaggageLoggingTest {

    @Test
    public void testBaggageEndsUpOnALogRecord() {
        // The context SLF4J already set up, rather than a fresh one: a hand-built
        // LoggerContext has no MDC adapter and the encoder asks for one. The
        // configuration it carries does not matter here, since the encoder under test
        // is configured below.
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

        LogstashEncoder encoder = new LogstashEncoder();
        encoder.setContext(context);
        encoder.setCustomFields(Baggage.customFields("analytics-operator-adder",
                Baggage.parse("smart_service_instance_id=8fbd0e8a,pipeline_id=3c1f9b42,umlaut=gr%C3%BCn")));
        encoder.start();

        // Built through a logger of the context rather than with the no-arg
        // constructor, which leaves the event without one and fails on the MDC.
        Logger logger = context.getLogger("operator");
        LoggingEvent event = new LoggingEvent(
                Logger.FQCN, logger, Level.ERROR, "kafka consume failed", null, null);

        Map<String, String> record = new Gson().fromJson(
                new String(encoder.encode(event), StandardCharsets.UTF_8),
                new TypeToken<Map<String, String>>() {
                }.getType());

        assertEquals("kafka consume failed", record.get("message"));
        assertEquals("github.com/SENERGY-Platform", record.get("organization"));
        assertEquals("analytics-operator-adder", record.get("project"));
        // The point of the whole change: given a smart service instance id, every log
        // line of its pipeline's operators can be found.
        assertEquals("8fbd0e8a", record.get("smart_service_instance_id"));
        assertEquals("3c1f9b42", record.get("pipeline_id"));
        assertEquals("grün", record.get("umlaut"));

        encoder.stop();
    }
}
