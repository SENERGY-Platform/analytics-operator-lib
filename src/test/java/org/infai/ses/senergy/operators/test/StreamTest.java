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
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Stream;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.TimeProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.*;
import org.infai.ses.senergy.testing.utils.JSONHelper;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamTest {

    private static final String INPUT_TOPIC  = "input-topic";

    private static final String INPUT_TOPIC_2  = "input-topic-2";

    private File stateDir = new File("./state/stream");

    private final LocalDateTime time = LocalDateTime.of(2020,01,01,01,01);

    Properties props = new Properties();

    @Before
    public void setUp() throws Exception {
        TimeProvider.useFixedClockAt(time);
        ConfigProvider.setConfig(new Config());
        // setup test driver
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    }

    @After
    public void deleteOutputFile() {
        if(stateDir.exists()){
            try {
                FileUtils.deleteDirectory(stateDir);
            } catch (IOException e) {
                System.out.println("Could not delete state dir.");
            }
        }
    }

    @Test
    public void testProcessSingleStream(){
        Stream stream = new Stream();
        stream.setPipelineId("AAA");
        Config config = new Config(new JSONHelper().parseFile("stream/testProcessSingleStreamConfig.json").toString());
        JSONArray expected = new JSONHelper().parseFile("stream/testProcessSingleStreamExpected.json");
        stream.setOperator(new TestOperator());
        stream.processSingleStream(config.getInputTopicsConfigs().get(0));

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.streamBuilder.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic.pipeInput("A", "{'pipeline_id': 'AAA', 'operator_id': '1'}");
            inputTopic.pipeInput("A", "{'pipeline_id': 'AAA', 'operator_id': '1'}");
            inputTopic.pipeInput("B", "{'pipeline_id': 'BBB', 'operator_id': '1'}");
            inputTopic.pipeInput("G", "{'pipeline_id': 'AAA', 'operator_id': '2'}");
        }
        int index = 0;
        int timestamp = 0;
        for (Object result:processorSupplier.theCapturedProcessor().processed){
            JSONObject value = (JSONObject)expected.get(index++);
            value.put("time", TimeProvider.nowUTCToString());
            assertEquals(new KeyValueTimestamp<>("AAA",value.toString(), timestamp),
                    result);
            timestamp += 1000;

        }
    }

    @Test
    public void testProcessSingleStreamAsTable(){
        Stream stream = new Stream();
        stream.setPipelineId("AAA");
        Config config = new Config(new JSONHelper().parseFile("stream/testProcessSingleStreamConfig.json").toString());
        JSONArray expected = new JSONHelper().parseFile("stream/testProcessSingleStreamExpected.json");
        stream.setOperator(new TestOperator());
        stream.processSingleStreamAsTable(config.getTopicConfig());

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.streamBuilder.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic.pipeInput("A", "{'pipeline_id': 'AAA', 'operator_id': '1'}");
            inputTopic.pipeInput("A", "{'pipeline_id': 'AAA', 'operator_id': '1'}");
            inputTopic.pipeInput("B", "{'pipeline_id': 'BBB', 'operator_id': '1'}");
            inputTopic.pipeInput("G", "{'pipeline_id': 'AAA', 'operator_id': '2'}");
        }

        int index = 0;
        int timestamp = 0;
        for (Object result:processorSupplier.theCapturedProcessor().processed){
            JSONObject value = (JSONObject)expected.get(index++);
            value.put("time", TimeProvider.nowUTCToString());
            assertEquals(new KeyValueTimestamp<>("AAA",value.toString(), timestamp),
                    result);
            timestamp += 1000;

        }
    }


    @Test
    public void testProcessSingleStreamDeviceId(){
        Stream stream = new Stream("AZB", "1");
        Config config = new Config(new JSONHelper().parseFile("stream/testProcessSingleStreamDeviceIdConfig.json").toString());
        JSONArray expected = new JSONHelper().parseFile("stream/testProcessSingleStreamDeviceIdExpected.json");
        stream.setOperator(new TestOperator());
        stream.processSingleStream(config.getInputTopicsConfigs().get(0));

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.streamBuilder.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic.pipeInput("A", "{'pipeline_id': '1', 'device_id': '1'}");
            inputTopic.pipeInput("B", "{'pipeline_id': '1', 'device_id': '1'}");
            inputTopic.pipeInput("C", "{'pipeline_id': '2', 'device_id': '1'}");
            inputTopic.pipeInput("D", "{'pipeline_id': '1', 'device_id': '2'}");
        }

        int index = 0;
        int timestamp = 0;
        for (Object result:processorSupplier.theCapturedProcessor().processed){
            JSONObject value = (JSONObject)expected.get(index++);
            value.put("time", TimeProvider.nowUTCToString());
            assertEquals(new KeyValueTimestamp<>("AZB",value.toString(), timestamp),
                    result);
            timestamp += 1000;

        }
    }

    @Test
    public void testProcessTwoStreams2DeviceId(){
        Stream stream = new Stream("AZB", "1");
        Config config = new Config(new JSONHelper().parseFile("stream/testProcessTwoStreams2DeviceIdConfig.json").toString());
        stream.streamBuilder.setWindowTime(5);
        stream.setOperator(new TestOperator());
        stream.processMultipleStreams(config.getTopicConfig());

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.streamBuilder.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic2 =
                    driver.createInputTopic(INPUT_TOPIC_2, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic1.pipeInput(null, "{'pipeline_id': '1', 'inputs':[{'device_id': '3', 'value':1}], 'analytics':{}}");
            inputTopic2.pipeInput(null, "{'pipeline_id': '1', 'inputs':[{'device_id': '4', 'value':1}], 'analytics':{}}");
            inputTopic2.pipeInput(null, "{'device_id': '2', 'value':3}");
            inputTopic1.advanceTime(Duration.ofSeconds(6));
            inputTopic2.advanceTime(Duration.ofSeconds(6));
            inputTopic1.pipeInput(null, "{'device_id': '1', 'value':2}");
            inputTopic1.advanceTime(Duration.ofSeconds(2));
            inputTopic2.advanceTime(Duration.ofSeconds(2));
            inputTopic2.pipeInput(null, "{'device_id': '2', 'value':2}");
        }
        JSONObject expected = new JSONHelper().parseFile("stream/testProcessTwoStreams2DeviceIdExpected.json");
        expected.put("time", TimeProvider.nowUTCToString());
        Assert.assertEquals(asList(
                new KeyValueTimestamp<>("AZB",
                        expected.toString(),
                        8000)
                ),
                processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void testProcessTwoStreamsAsTable2DeviceId(){
        Stream stream = new Stream("AZB", "1");
        Config config = new Config(new JSONHelper().parseFile("stream/testProcessTwoStreams2DeviceIdConfig.json").toString());
        JSONArray expected = new JSONHelper().parseFile("stream/testProcessTwoStreamsAsTable2DeviceIdExpected.json");
        stream.setOperator(new TestOperator());
        stream.processMultipleStreamsAsTable(config.getTopicConfig());

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.streamBuilder.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic2 =
                    driver.createInputTopic(INPUT_TOPIC_2, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic1.pipeInput("A", "{'pipeline_id': '1', 'inputs':[{'device_id': '3', 'value':1}], 'analytics':{}}");
            inputTopic2.pipeInput("A", "{'pipeline_id': '1', 'inputs':[{'device_id': '4', 'value':1}], 'analytics':{}}");
            inputTopic2.pipeInput("A", "{'device_id': '2', 'value':3}");
            inputTopic1.advanceTime(Duration.ofSeconds(6));
            inputTopic2.advanceTime(Duration.ofSeconds(6));
            inputTopic1.pipeInput("A", "{'device_id': '1', 'value':2}");
            inputTopic1.advanceTime(Duration.ofSeconds(2));
            inputTopic2.advanceTime(Duration.ofSeconds(2));
            inputTopic2.pipeInput("A", "{'device_id': '2', 'value':2}");
        }
        JSONObject expected1 = (JSONObject) expected.get(1);
        JSONObject expected2 = (JSONObject) expected.get(0);
        expected1.put("time", TimeProvider.nowUTCToString());
        expected2.put("time", TimeProvider.nowUTCToString());
        Assert.assertEquals(asList(

                new KeyValueTimestamp<>("AZB",expected1.toString(), 6000),
                new KeyValueTimestamp<>("AZB",expected2.toString(), 8000)
                ),
                processorSupplier.theCapturedProcessor().processed);
    }

    //@Test
    @Test
    public void test5Streams(){
        testProcessMultipleStreams(5);
    }

    @Test
    public void test128Streams(){
        testProcessMultipleStreams(128);
    }

    @Test
    public void test2Streams(){
        testProcessMultipleStreams(2);
    }

    @Test
    public void test5StreamsWithMultipleMessages(){
        testProcessMultipleStreamsWithMultipleMessages(10);
    }

    @Test
    public void testComplexMessage(){
        Stream stream = new Stream("AZB", "1");
        String configString = "{\"inputTopics\":[{\"name\":\"topic1\",\"filterType\":\"DeviceId\",\"filterValue\":\"1\",\"mappings\":[{\"dest\":\"value1\",\"source\":\"value.reading.OBIS_1_8_0.value\"},{\"dest\":\"timestamp1\",\"source\":\"value.reading.time\"}]},{\"name\":\"topic2\",\"filterType\":\"DeviceId\",\"filterValue\":\"2\",\"mappings\":[{\"dest\":\"value2\",\"source\":\"value.reading.OBIS_1_8_0.value\"},{\"dest\":\"timestamp2\",\"source\":\"value.reading.time\"}]},{\"name\":\"topic3\",\"filterType\":\"DeviceId\",\"filterValue\":\"3\",\"mappings\":[{\"dest\":\"value3\",\"source\":\"value.reading.OBIS_1_8_0.value\"},{\"dest\":\"timestamp3\",\"source\":\"value.reading.time\"}]}]}";
        Config config = new Config(configString);
        stream.streamBuilder.setWindowTime(5);
        stream.setOperator(new TestOperator());
        stream.processMultipleStreams(config.getTopicConfig());

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.streamBuilder.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic("topic1", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic2 =
                    driver.createInputTopic("topic2", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic3 =
                    driver.createInputTopic("topic3", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic1.pipeInput(null, "{'pipeline_id': '1', 'inputs':[{'device_id': 'abc', 'value':1}], 'analytics':{}}");
            inputTopic2.pipeInput(null, "{'pipeline_id': '1', 'inputs':[{'device_id': 'abc', 'value':1}], 'analytics':{}}");

            inputTopic1.pipeInput(null, "{'device_id':'1','service_id':'1','value':{'reading':{'OBIS_16_7':{'unit':'kW','value':0.36},'OBIS_1_8_0':{'unit':'kWh','value':226.239},'time':'2019-07-18T12:19:04.250355Z'}}}");
            inputTopic2.pipeInput(null, "{'device_id':'2','service_id':'2','value':{'reading':{'OBIS_16_7':{'unit':'kW','value':0.36},'OBIS_1_8_0':{'unit':'kWh','value':226.239},'time':'2019-07-18T12:19:04.250355Z'}}}");
            inputTopic3.pipeInput(null, "{'device_id':'3','service_id':'3','value':{'reading':{'OBIS_16_7':{'unit':'kW','value':0.36},'OBIS_1_8_0':{'unit':'kWh','value':226.239},'time':'2019-07-18T12:19:04.250355Z'}}}");
        }

        JSONObject expected = new JSONHelper().parseFile("stream/testComplexMessageExpected.json");;
        expected.put("time", TimeProvider.nowUTCToString());
        Assert.assertEquals(new KeyValueTimestamp<>("AZB",
                expected.toString(),
                0), processorSupplier.theCapturedProcessor().processed.get(0));
    }

    private void testProcessMultipleStreams(int numStreams){
        Stream stream = new Stream("AZB", "1");
        String configString = "{\"inputTopics\": [";
        for(int i = 0; i <= numStreams; i++){
            configString += "{\"Name\":\"topic"+i+"\",\"FilterType\":\"DeviceId\",\"FilterValue\":\""+i+"\",\"Mappings\":[{\"Dest\":\"value"+i+"\",\"Source\":\"value.reading.value\"}]},";
        }
        configString = configString.substring(0, configString.length()-1); //remove last ','
        configString += "]}";


        Config config = new Config(configString);
        stream.streamBuilder.setWindowTime(5);
        stream.setOperator(new TestOperator());
        stream.processMultipleStreams(config.getTopicConfig());


        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.streamBuilder.getBuilder().build(), props)) {
            List<TestInputTopic<String, String>> topics = new ArrayList<TestInputTopic<String, String>>();
            for(int i = 0; i <= numStreams; i++){
                topics.add(driver.createInputTopic("topic"+i, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO));
                topics.get(i).pipeInput(null, "{'device_id': '"+i+"', 'value':"+i+"}");
                topics.get(i).pipeInput(null, "{'pipeline_id': '1', 'inputs':[{'device_id': 'abc', 'value':1}], 'analytics':{}}"); //To test filtering
            }
        }

        String expected = "{\"analytics\":{\"test\":\"1\"},\"operator_id\":\"AZB\",\"inputs\":[";
        for(int i = 0; i <= numStreams; i++){
            expected += "{\"device_id\":\""+i+"\",\"value\":"+i+"},";
        }
        expected = expected.substring(0, expected.length()-1); //remove last ','
        expected += "],\"pipeline_id\":\"1\",\"time\":\""+ TimeProvider.nowUTCToString() +"\"}";

        Assert.assertEquals(asList(new KeyValueTimestamp<>("AZB",
                        expected,
                        0))
                , processorSupplier.theCapturedProcessor().processed);
    }

    private void testProcessMultipleStreamsWithMultipleMessages(int numStreams){
        Map<Integer, Integer> expectedValues  = new HashMap(1);
        expectedValues.put(10,2048);
        Stream stream = new Stream("AZB", "1");
        stream.setOperator(new TestOperator());
        String configString = "{\"inputTopics\": [";
        for(int i = 0; i <= numStreams; i++){
            configString += "{\"Name\":\"topic"+i+"\",\"FilterType\":\"DeviceId\",\"FilterValue\":\""+i+"\",\"Mappings\":[{\"Dest\":\"value"+i+"\",\"Source\":\"value.reading.value\"}]},";
        }
        configString = configString.substring(0, configString.length()-1); //remove last ','
        configString += "]}";

        Config config = new Config(configString);
        stream.streamBuilder.setWindowTime(5);

        stream.processMultipleStreams(config.getTopicConfig());

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(stream.streamBuilder.getBuilder().build(), props)) {
            List<TestInputTopic<String, String>> topics = new ArrayList<TestInputTopic<String, String>>();
            for(int i = 0; i <= numStreams; i++){
                topics.add(driver.createInputTopic("topic"+i, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofMillis(500)));
                topics.get(i).pipeInput(null, "{'device_id': '"+i+"', 'value':"+i+"}");
                topics.get(i).pipeInput(null, "{'device_id': '"+i+"', 'value':"+i+1+"}");
            }
        }

        System.out.println(processorSupplier.theCapturedProcessor().processed.size());

        Assert.assertEquals(expectedValues.get(numStreams).intValue(), processorSupplier.theCapturedProcessor().processed.size());
    }
}