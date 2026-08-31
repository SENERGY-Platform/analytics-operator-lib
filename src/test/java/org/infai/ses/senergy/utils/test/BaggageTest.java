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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.infai.ses.senergy.utils.Baggage;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BaggageTest {

    /**
     * The exact headers the flow engine produces for these inputs, taken from its
     * own pkg/baggage.Header. Pinned rather than described, because the environment
     * variable is a contract between three languages and a drift on any side is
     * otherwise only visible in production.
     */
    private static Map<String, String> flowEngineHeaders() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("pipeline_id=3c1f9b42,smart_service_instance_id=8fbd0e8a,user_id=jonah", "three plain entries");
        headers.put("username=jonah@bitnify.net", "an email address needs no encoding");
        headers.put("comma=a%2Cb,equals=a=b,semicolon=a%3Bb,space=a%20b", "the delimiters");
        headers.put("backslash=a%5Cb,percent=50%25,quote=a%22b,umlaut=gr%C3%BCn", "escapes and utf-8");
        headers.put("empty=", "an empty value");
        return headers;
    }

    @Test
    public void testParsePlainEntries() {
        Map<String, String> entries =
                Baggage.parse("pipeline_id=3c1f9b42,smart_service_instance_id=8fbd0e8a,user_id=jonah");
        assertEquals(3, entries.size());
        assertEquals("3c1f9b42", entries.get("pipeline_id"));
        assertEquals("8fbd0e8a", entries.get("smart_service_instance_id"));
        assertEquals("jonah", entries.get("user_id"));
    }

    @Test
    public void testParseDelimitersFromTheFlowEngine() {
        Map<String, String> entries = Baggage.parse("comma=a%2Cb,equals=a=b,semicolon=a%3Bb,space=a%20b");
        assertEquals(4, entries.size());
        assertEquals("a,b", entries.get("comma"));
        // Only the first equals sign separates key from value.
        assertEquals("a=b", entries.get("equals"));
        assertEquals("a;b", entries.get("semicolon"));
        assertEquals("a b", entries.get("space"));
    }

    @Test
    public void testParseEscapesAndUtf8() {
        Map<String, String> entries =
                Baggage.parse("backslash=a%5Cb,percent=50%25,quote=a%22b,umlaut=gr%C3%BCn");
        assertEquals("a\\b", entries.get("backslash"));
        assertEquals("50%", entries.get("percent"));
        assertEquals("a\"b", entries.get("quote"));
        assertEquals("grün", entries.get("umlaut"));
    }

    @Test
    public void testParsePlusSignIsLiteral() {
        // The reason the decoder is hand-rolled: URLDecoder is a form decoder and
        // would turn this into a space.
        assertEquals("a+b", Baggage.parse("key=a+b").get("key"));
    }

    @Test
    public void testParseEmptyValue() {
        Map<String, String> entries = Baggage.parse("empty=");
        assertEquals(1, entries.size());
        assertEquals("", entries.get("empty"));
    }

    @Test
    public void testParseNoBaggage() {
        // No context sent, or an operator run outside the platform. Neither is an error.
        assertTrue(Baggage.parse(null).isEmpty());
        assertTrue(Baggage.parse("").isEmpty());
    }

    @Test
    public void testParseDropsProperties() {
        assertEquals("value", Baggage.parse("key=value;prop=1;other").get("key"));
    }

    @Test
    public void testParseToleratesWhitespace() {
        Map<String, String> entries = Baggage.parse("a=1 ,  b=2");
        assertEquals("1", entries.get("a"));
        assertEquals("2", entries.get("b"));
    }

    @Test
    public void testParseSkipsMalformedEntries() {
        Map<String, String> entries = Baggage.parse("good=1,nonsense,=novalue,");
        assertEquals(1, entries.size());
        assertEquals("1", entries.get("good"));
    }

    @Test
    public void testParseKeepsAMalformedEscapeAsWritten() {
        assertEquals("100%", Baggage.parse("key=100%").get("key"));
        assertEquals("%zz", Baggage.parse("key=%zz").get("key"));
    }

    @Test
    public void testEveryFlowEngineHeaderParses() {
        for (Map.Entry<String, String> testCase : flowEngineHeaders().entrySet()) {
            Map<String, String> entries = Baggage.parse(testCase.getKey());
            assertFalse(testCase.getValue(), entries.isEmpty());
        }
    }

    @Test
    public void testCustomFieldsCarriesTheBaggage() {
        Map<String, String> fields = parseJson(Baggage.customFields(
                "analytics-operator-adder", Baggage.parse("smart_service_instance_id=8fbd0e8a")));
        assertEquals("github.com/SENERGY-Platform", fields.get("organization"));
        assertEquals("analytics-operator-adder", fields.get("project"));
        assertEquals("8fbd0e8a", fields.get("smart_service_instance_id"));
    }

    @Test
    public void testCustomFieldsIgnoresReservedKeys() {
        // A baggage entry named like a field of the record would produce a duplicate
        // JSON key and make the real one unreadable.
        Map<String, String> fields = parseJson(Baggage.customFields("a-project",
                Baggage.parse("level=nonsense,msg=nonsense,time=nonsense,project=nonsense,keep=yes")));
        assertEquals("a-project", fields.get("project"));
        assertEquals("yes", fields.get("keep"));
        assertFalse(fields.containsKey("level"));
        assertFalse(fields.containsKey("msg"));
        assertFalse(fields.containsKey("time"));
    }

    @Test
    public void testCustomFieldsWithoutProject() {
        Map<String, String> fields = parseJson(Baggage.customFields(null, Baggage.parse(null)));
        assertEquals("unknown", fields.get("project"));
        assertEquals(2, fields.size());
    }

    private Map<String, String> parseJson(String json) {
        return new Gson().fromJson(json, new TypeToken<Map<String, String>>() {
        }.getType());
    }
}
