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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.MockProcessorSupplier;
import org.infai.ses.senergy.models.DeviceMessageModel;
import org.infai.ses.senergy.models.InputMessageModel;
import org.infai.ses.senergy.models.MessageModel;
import org.infai.ses.senergy.operators.Helper;
import org.infai.ses.senergy.operators.StreamBuilder;
import org.infai.ses.senergy.serialization.JSONSerdes;
import org.infai.ses.senergy.testing.utils.JSONHelper;
import org.infai.ses.senergy.utils.TimeProvider;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

import static java.util.Arrays.asList;

public class StreamBuilderTest {

    private static final String INPUT_TOPIC  = "input-topic";

    private static final String INPUT_TOPIC_2  = "input-topic-2";

    private static final String OUTPUT_TOPIC  = "output-topic";

    private File stateDir = new File("./state/builder");

    private final LocalDateTime time = LocalDateTime.of(2020,01,01,01,01);

    Properties props = new Properties();

    @Before
    public void setUp(){
        TimeProvider.useFixedClockAt(time);
        // setup test driver
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerdes.DeviceMessage().getClass().getName());
    }

    @Test
    public void testFilterBy() {
        final String[] deviceIds = new String[]{"1"};

        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, DeviceMessageModel> source1 = builder.stream(INPUT_TOPIC);
        final KStream<String, DeviceMessageModel> filtered = StreamBuilder.filterBy(source1, deviceIds);

        KStream<String, String> out = filtered.flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, Helper.getFromObject(value)));
            return result;
        });
        out.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        out.process(processorSupplier);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic.pipeInput("A", "{\"device_id\": \"1\"}");
            inputTopic.pipeInput("B", "{\"device_id\": \"2\"}");
            inputTopic.pipeInput("D", "{\"device_id\": \"1\"}");
        }

        Assert.assertEquals(asList(
                new KeyValueTimestamp<>("A", "{\"device_id\":\"1\",\"service_id\":null,\"value\":null}", 0),
                new KeyValueTimestamp<>("D", "{\"device_id\":\"1\",\"service_id\":null,\"value\":null}", 2000)
                ),
                processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void testFilterByMultipleDevices(){
        final String[] deviceIds = new String[] {"1", "2"};

        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, DeviceMessageModel> source1 = builder.stream(INPUT_TOPIC);
        final KStream<String, DeviceMessageModel> filtered = StreamBuilder.filterBy(source1, deviceIds);
        KStream<String, String> out = filtered.flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, Helper.getFromObject(value)));
            return result;
        });
        out.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        out.process(processorSupplier);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic.pipeInput("A", "{\"device_id\": \"1\"}");
            inputTopic.pipeInput("B", "{\"device_id\": \"2\"}");
            inputTopic.pipeInput("B", "{\"device_id\": \"3\"}");
            inputTopic.pipeInput("D", "{\"device_id\": \"1\"}");
        }
        Assert.assertEquals(asList(
                new KeyValueTimestamp<>("A", "{\"device_id\":\"1\",\"service_id\":null,\"value\":null}", 0),
                new KeyValueTimestamp<>("B", "{\"device_id\":\"2\",\"service_id\":null,\"value\":null}", 1000),
                new KeyValueTimestamp<>("D", "{\"device_id\":\"1\",\"service_id\":null,\"value\":null}", 3000)
                ),
                processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void testJoinStreams() throws JSONException {
        JSONArray messages = new JSONHelper().parseFile("builder/messages.json");
        JSONArray expected = new JSONHelper().parseFile("builder/results.json");

        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, DeviceMessageModel> source1 = builder.stream(INPUT_TOPIC);
        final KStream<String, DeviceMessageModel> source2 = builder.stream(INPUT_TOPIC_2);

        List <KStream<String, InputMessageModel>> streams = new LinkedList<>();

        streams.add(source1.flatMap((key, value) -> {
            List<KeyValue<String, InputMessageModel>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, Helper.deviceToInputMessageModel(value, INPUT_TOPIC)));
            return result;
        }));
        streams.add(source2.flatMap((key, value) -> {
            List<KeyValue<String, InputMessageModel>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, Helper.deviceToInputMessageModel(value, INPUT_TOPIC_2)));
            return result;
        }));

        final KStream<String, MessageModel> merged = StreamBuilder.joinMultipleStreams(streams);
        KStream<String, String> out = merged.flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, Helper.getFromObject(value)));
            return result;
        });
        out.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        out.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            final TestInputTopic<String, String> inputTopic2 =
                    driver.createInputTopic(INPUT_TOPIC_2, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic1.pipeInput("A", messages.get(0).toString());
            inputTopic2.pipeInput("A", messages.get(1).toString());
            inputTopic2.pipeInput("A", messages.get(2).toString());
        }

        Assert.assertEquals(2, processorSupplier.theCapturedProcessor().processed.size());
        int index = 0;
        for (KeyValueTimestamp<Object, Object> result:processorSupplier.theCapturedProcessor().processed){
            JSONObject value = (JSONObject) expected.get(index++);
            JSONAssert.assertEquals(value.toString(),(String) result.value(), JSONCompareMode.LENIENT);
        }
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
}