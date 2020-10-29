/*
 * Copyright 2020 InfAI (CC SES)
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

package org.infai.ses.senergy.operators.test;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.test.MockProcessorSupplier;
import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Stream;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.TimeProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.*;
import org.infai.ses.senergy.testing.utils.JSONHelper;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamTest {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String INPUT_TOPIC_2 = "input-topic-2";
    private final File stateDir = new File("./state/stream");
    private final LocalDateTime time = LocalDateTime.of(2020, 01, 01, 01, 01);
    private final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
    Properties props = new Properties();

    @Before
    public void setUp() {
        TimeProvider.useFixedClockAt(time);
        ConfigProvider.setConfig(new Config());
        // setup test driver
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    }

    @After
    public void deleteOutputFile() {
        if (stateDir.exists()) {
            try {
                FileUtils.deleteDirectory(stateDir);
            } catch (IOException e) {
                System.out.println("Could not delete state dir.");
            }
        }
    }

    @Test
    public void testProcessSingleStream() {
        Stream stream = new Stream();
        Config config = new Config(new JSONHelper().parseFile("stream/testProcessSingleStreamConfig.json").toString());
        JSONArray expected = new JSONHelper().parseFile("stream/testProcessSingleStreamExpected.json");
        JSONArray jsonMessages = new JSONHelper().parseFile("stream/testProcessSingleStreamMessages.json");
        stream.setOperator(new TestOperator());
        stream.processSingleStream(config.getInputTopicsConfigs().get(0));
        stream.getOutputStream().process(processorSupplier);

        final TopologyTestDriver driver = new TopologyTestDriver(stream.getBuilder().build(), props);
        final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
        inputTopic.pipeInput("A", jsonMessages.get(0).toString());
        inputTopic.pipeInput("A", jsonMessages.get(1).toString());
        inputTopic.pipeInput("B", jsonMessages.get(2).toString());
        inputTopic.pipeInput("G", jsonMessages.get(3).toString());

        int index = 0;
        assertEquals(2, processorSupplier.theCapturedProcessor().processed.size());
        for (KeyValueTimestamp<Object, Object> result : processorSupplier.theCapturedProcessor().processed) {
            JSONObject value = (JSONObject) expected.get(index++);
            value.put("time", TimeProvider.nowUTCToString());
            JSONAssert.assertEquals(value.toString(),(String) result.value(), JSONCompareMode.LENIENT);
        }
    }

    @Test
    public void testProcessSingleStreamAsTable() {
        Stream stream = new Stream();
        Config config = new Config(new JSONHelper().parseFile("stream/testProcessSingleStreamConfig.json").toString());
        JSONArray expected = new JSONHelper().parseFile("stream/testProcessSingleStreamExpected.json");
        JSONArray jsonMessages = new JSONHelper().parseFile("stream/testProcessSingleStreamMessages.json");
        stream.setOperator(new TestOperator());
        stream.processSingleStreamAsTable(config.getInputTopicsConfigs().get(0));

        stream.getOutputStream().process(processorSupplier);
        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic.pipeInput("A", jsonMessages.get(0).toString());
            inputTopic.pipeInput("A", jsonMessages.get(1).toString());
            inputTopic.pipeInput("B", jsonMessages.get(2).toString());
            inputTopic.pipeInput("G", jsonMessages.get(3).toString());
        }

        assertEquals(2, processorSupplier.theCapturedProcessor().processed.size());
        int index = 0;
        for (KeyValueTimestamp<Object, Object> result : processorSupplier.theCapturedProcessor().processed) {
            JSONObject value = (JSONObject) expected.get(index++);
            value.put("time", TimeProvider.nowUTCToString());
            JSONAssert.assertEquals(value.toString(),(String) result.value(), JSONCompareMode.LENIENT);
        }
    }


    @Test
    public void testProcessSingleStreamDeviceId() {
        Stream stream = new Stream();
        Config config = new Config(new JSONHelper().parseFile("stream/testProcessSingleStreamDeviceIdConfig.json").toString());
        JSONArray expected = new JSONHelper().parseFile("stream/testProcessSingleStreamDeviceIdExpected.json");
        stream.setOperator(new TestOperator());
        stream.processSingleStream(config.getInputTopicsConfigs().get(0));

        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic.pipeInput("A", "{\"service_id\": \"1\", \"device_id\": \"1\"}");
            inputTopic.pipeInput("B", "{\"service_id\": \"1\", \"device_id\": \"1\"}");
            inputTopic.pipeInput("C", "{\"service_id\": \"2\", \"device_id\": \"1\"}");
            inputTopic.pipeInput("D", "{\"service_id\": \"1\", \"device_id\": \"2\"}");
        }

        int index = 0;
        for (KeyValueTimestamp result : processorSupplier.theCapturedProcessor().processed) {
            JSONObject value = (JSONObject) expected.get(index++);
            value.put("time", TimeProvider.nowUTCToString());
            JSONAssert.assertEquals(value.toString(),(String) result.value(), JSONCompareMode.LENIENT);
        }
    }

    @Test
    public void testProcessTwoStreams2DeviceId() {
        Stream stream = new Stream();
        Config config = new Config(new JSONHelper().parseFile("stream/testProcessTwoStreams2DeviceIdConfig.json").toString());
        JSONObject expected = new JSONHelper().parseFile("stream/testProcessTwoStreams2DeviceIdExpected.json");
        JSONArray jsonMessages = new JSONHelper().parseFile("stream/testProcessTwoStreams2DeviceIdMessages.json");
        stream.setWindowTime(5);
        stream.setOperator(new TestOperator());
        stream.processMultipleStreams(config.getInputTopicsConfigs());

        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic2 =
                    driver.createInputTopic(INPUT_TOPIC_2, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic1.pipeInput(null, jsonMessages.get(0).toString());
            inputTopic2.pipeInput(null, jsonMessages.get(1).toString());
            inputTopic2.pipeInput(null, jsonMessages.get(2).toString());
            inputTopic1.advanceTime(Duration.ofSeconds(6));
            inputTopic2.advanceTime(Duration.ofSeconds(6));
            inputTopic1.pipeInput(null, jsonMessages.get(3).toString());
            inputTopic1.advanceTime(Duration.ofSeconds(2));
            inputTopic2.advanceTime(Duration.ofSeconds(2));
            inputTopic2.pipeInput(null, jsonMessages.get(4).toString());
        }

        expected.put("time", TimeProvider.nowUTCToString());
        JSONAssert.assertEquals(expected.toString(),(String) processorSupplier.theCapturedProcessor().processed.get(0).value(), JSONCompareMode.LENIENT);
        assertEquals(8000, processorSupplier.theCapturedProcessor().processed.get(0).timestamp());
    }

    @Test
    public void testProcessTwoStreamsAsTable2DeviceId() {
        Stream stream = new Stream();
        Config config = new Config(new JSONHelper().parseFile("stream/testProcessTwoStreams2DeviceIdConfig.json").toString());
        JSONArray expected = new JSONHelper().parseFile("stream/testProcessTwoStreamsAsTable2DeviceIdExpected.json");
        JSONArray jsonMessages = new JSONHelper().parseFile("stream/testProcessTwoStreams2DeviceIdMessages.json");
        stream.setOperator(new TestOperator());
        stream.processMultipleStreamsAsTable(config.getInputTopicsConfigs());

        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic2 =
                    driver.createInputTopic(INPUT_TOPIC_2, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic1.pipeInput(null, jsonMessages.get(0).toString());
            inputTopic2.pipeInput(null, jsonMessages.get(1).toString());
            inputTopic2.pipeInput(null, jsonMessages.get(2).toString());
            inputTopic1.advanceTime(Duration.ofSeconds(6));
            inputTopic2.advanceTime(Duration.ofSeconds(6));
            inputTopic1.pipeInput(null, jsonMessages.get(3).toString());
            inputTopic1.advanceTime(Duration.ofSeconds(2));
            inputTopic2.advanceTime(Duration.ofSeconds(2));
            inputTopic2.pipeInput(null, jsonMessages.get(4).toString());
        }
        JSONObject expected1 = (JSONObject) expected.get(1);
        JSONObject expected2 = (JSONObject) expected.get(0);
        expected1.put("time", TimeProvider.nowUTCToString());
        expected2.put("time", TimeProvider.nowUTCToString());

        int index = 0;
        long[] timestamps = {6000, 8000};
        assertEquals(2, processorSupplier.theCapturedProcessor().processed.size());
        for (KeyValueTimestamp<Object, Object> result : processorSupplier.theCapturedProcessor().processed) {
            JSONObject value = (JSONObject) expected.get(index);
            JSONAssert.assertEquals(value.toString(),(String) result.value(), JSONCompareMode.LENIENT);
            assertEquals(timestamps[index++], result.timestamp());
            assertEquals("debug", result.key());
        }
    }

    //@Test
    @Test
    public void test5Streams() {
        testProcessMultipleStreams(5);
    }

    @Test
    public void test128Streams() {
        testProcessMultipleStreams(128);
    }

    @Test
    public void test2Streams() {
        testProcessMultipleStreams(2);
    }

    @Test
    public void test5StreamsWithMultipleMessages() {
        testProcessMultipleStreamsWithMultipleMessages(10);
    }

    @Test
    public void testComplexMessage() {
        Stream stream = new Stream();
        JSONArray jsonMessages = new JSONHelper().parseFile("stream/testComplexMessageMessages.json");
        String configString = "{\"inputTopics\":[{\"name\":\"topic1\",\"filterType\":\"DeviceId\",\"filterValue\":\"1\",\"mappings\":[{\"dest\":\"value1\",\"source\":\"value.reading.OBIS_1_8_0.value\"},{\"dest\":\"timestamp1\",\"source\":\"value.reading.time\"}]},{\"name\":\"topic2\",\"filterType\":\"DeviceId\",\"filterValue\":\"2\",\"mappings\":[{\"dest\":\"value2\",\"source\":\"value.reading.OBIS_1_8_0.value\"},{\"dest\":\"timestamp2\",\"source\":\"value.reading.time\"}]},{\"name\":\"topic3\",\"filterType\":\"DeviceId\",\"filterValue\":\"3\",\"mappings\":[{\"dest\":\"value3\",\"source\":\"value.reading.OBIS_1_8_0.value\"},{\"dest\":\"timestamp3\",\"source\":\"value.reading.time\"}]}]}";
        Config config = new Config(configString);
        stream.setWindowTime(5);
        stream.setOperator(new TestOperator());
        stream.processMultipleStreams(config.getInputTopicsConfigs());

        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic("topic1", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic2 =
                    driver.createInputTopic("topic2", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic3 =
                    driver.createInputTopic("topic3", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic1.pipeInput(null, jsonMessages.get(0).toString());
            inputTopic2.pipeInput(null, jsonMessages.get(1).toString());

            inputTopic1.pipeInput(null, jsonMessages.get(2).toString());
            inputTopic2.pipeInput(null, jsonMessages.get(3).toString());
            inputTopic3.pipeInput(null,jsonMessages.get(4).toString());
        }

        JSONObject expected = new JSONHelper().parseFile("stream/testComplexMessageExpected.json");
        expected.put("time", TimeProvider.nowUTCToString());
        JSONAssert.assertEquals(expected.toString(),(String) processorSupplier.theCapturedProcessor().processed.get(0).value(), JSONCompareMode.LENIENT);
        assertEquals(0, processorSupplier.theCapturedProcessor().processed.get(0).timestamp());
    }

    private void testProcessMultipleStreams(int numStreams) {
        Stream stream = new Stream();
        String configString = "{\"inputTopics\": [";
        for (int i = 0; i <= numStreams; i++) {
            configString += "{\"Name\":\"topic" + i + "\",\"FilterType\":\"DeviceId\",\"FilterValue\":\"" + i + "\",\"Mappings\":[{\"Dest\":\"value" + i + "\",\"Source\":\"value.reading.value\"}]},";
        }
        configString = configString.substring(0, configString.length() - 1); //remove last ','
        configString += "]}";


        Config config = new Config(configString);
        TODO:stream.setWindowTime(5);
        stream.setOperator(new TestOperator());
        stream.processMultipleStreams(config.getInputTopicsConfigs());

        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.getBuilder().build(), props)) {
            List<TestInputTopic<String, String>> topics = new ArrayList<TestInputTopic<String, String>>();
            for (int i = 0; i <= numStreams; i++) {
                topics.add(driver.createInputTopic("topic" + i, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO));
                topics.get(i).pipeInput(null, "{\"device_id\": \"" + i + "\", \"value\": {\"value\":" + i + "}}");
                //topics.get(i).pipeInput(null, "{\"pipeline_id\": \"1\", \"inputs\":[{\"device_id\": \"abc\", \"value\":1}], \"analytics'\":{}}"); //To test filtering
            }
        }

        //String expected = "{\"analytics\":{\"test\":\"1\"},\"operator_id\":\"AZB\",\"inputs\":[";
        //for (int i = 0; i <= numStreams; i++) {
        //    expected += "{\"device_id\":\"" + i + "\",\"value\":" + i + "},";
        //}
        //expected = expected.substring(0, expected.length() - 1); //remove last ','
        //expected += "],\"pipeline_id\":\"1\",\"time\":\"" + TimeProvider.nowUTCToString() + "\"}";
        assertEquals(1,processorSupplier.theCapturedProcessor().processed.size());
    }

    private void testProcessMultipleStreamsWithMultipleMessages(int numStreams) {
        Map<Integer, Integer> expectedValues = new HashMap(1);
        expectedValues.put(10, 2048);
        Stream stream = new Stream();
        stream.setOperator(new TestOperator());
        String configString = "{\"inputTopics\": [";
        for (int i = 0; i <= numStreams; i++) {
            configString += "{\"Name\":\"topic" + i + "\",\"FilterType\":\"DeviceId\",\"FilterValue\":\"" + i + "\",\"Mappings\":[{\"Dest\":\"value" + i + "\",\"Source\":\"value.value\"}]},";
        }
        configString = configString.substring(0, configString.length() - 1); //remove last ','
        configString += "]}";

        Config config = new Config(configString);
        stream.setWindowTime(5);

        stream.processMultipleStreams(config.getInputTopicsConfigs());

        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.getBuilder().build(), props)) {
            List<TestInputTopic<String, String>> topics = new ArrayList<TestInputTopic<String, String>>();
            for (int i = 0; i <= numStreams; i++) {
                topics.add(driver.createInputTopic("topic" + i, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofMillis(500)));
                topics.get(i).pipeInput(null, "{\"device_id\": \"" + i + "\", \"value\": {\"value\":" + i + "}}");
                int z = i;
                if (i == 0) {
                    z = 1;
                }
                topics.get(i).pipeInput(null, "{\"device_id\": \"" + i + "\", \"value\": {\"value\":" + z + 1 + "}}");
            }
        }

        Assert.assertEquals(expectedValues.get(numStreams).intValue(), processorSupplier.theCapturedProcessor().processed.size());
    }
}