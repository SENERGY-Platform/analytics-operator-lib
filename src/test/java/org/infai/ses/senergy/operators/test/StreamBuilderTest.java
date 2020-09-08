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

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.test.MockProcessorSupplier;
import org.infai.ses.senergy.operators.StreamBuilder;
import org.infai.ses.senergy.testing.utils.JSONHelper;
import org.infai.ses.senergy.utils.TimeProvider;
import org.json.simple.JSONArray;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Properties;
import static java.util.Arrays.asList;


public class StreamBuilderTest extends TestCase {

    private static final String INPUT_TOPIC  = "input-topic";

    private static final String INPUT_TOPIC_2  = "input-topic-2";

    private static final String OUTPUT_TOPIC  = "output-topic";

    private File stateDir = new File("./state/builder");

    private final LocalDateTime time = LocalDateTime.of(2020,01,01,01,01);

    Properties props = new Properties();

    @Override
    protected void setUp() throws Exception {
        TimeProvider.useFixedClockAt(time);
        // setup test driver
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    }

    @Test
    public void testFilterBy() {
        StreamBuilder builder = new StreamBuilder("1", "1");
        final String deviceIdPath = "device_id";
        final String[] deviceIds = new String[]{"1"};

        final KStream<String, String> source1 = builder.getBuilder().stream(INPUT_TOPIC);
        final KStream<String, String> filtered = builder.filterBy(source1, deviceIdPath, deviceIds);
        filtered.to(OUTPUT_TOPIC);

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        filtered.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic.pipeInput("A", "{'device_id': '1'}");
            inputTopic.pipeInput("B", "{'device_id': '2'}");
            inputTopic.pipeInput("D", "{'device_id': '1'}");
        }

        assertEquals(asList(
                new KeyValueTimestamp<>("A", "{'device_id': '1'}", 0),
                new KeyValueTimestamp<>("D", "{'device_id': '1'}", 2000)
                ),
                processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void testFilterByMultipleDevices(){
        StreamBuilder builder = new StreamBuilder("1", "1");
        final String deviceIdPath = "device_id";
        final String[] deviceIds = new String[] {"1", "2"};

        final KStream<String, String> source1 = builder.getBuilder().stream(INPUT_TOPIC);
        final KStream<String, String> filtered = builder.filterBy(source1, deviceIdPath, deviceIds);
        filtered.to(OUTPUT_TOPIC);

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        filtered.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic.pipeInput("A", "{'device_id': '1'}");
            inputTopic.pipeInput("B", "{'device_id': '2'}");
            inputTopic.pipeInput("D", "{'device_id': '1'}");
        }
        assertEquals(asList(
                new KeyValueTimestamp<>("A", "{'device_id': '1'}", 0),
                new KeyValueTimestamp<>("B", "{'device_id': '2'}", 1000),
                new KeyValueTimestamp<>("D", "{'device_id': '1'}", 2000)
                ),
                processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void testJoinStreams(){
        JSONArray messages = new JSONHelper().parseFile("builder/messages.json");
        JSONArray expected = new JSONHelper().parseFile("builder/results.json");

        StreamBuilder builder = new StreamBuilder("1", "1");

        final KStream<String, String> source1 = builder.getBuilder().stream(INPUT_TOPIC);
        final KStream<String, String> source2 = builder.getBuilder().stream(INPUT_TOPIC_2);

        KStream<String, String>[] streams = new KStream[2];
        streams[0] = source1;
        streams[1] = source2;

        final KStream<String, String> merged = builder.joinMultipleStreams(streams);
        merged.to(OUTPUT_TOPIC);

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.getBuilder().build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            final TestInputTopic<String, String> inputTopic2 =
                    driver.createInputTopic(INPUT_TOPIC_2, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ofSeconds(1));
            inputTopic1.pipeInput("A", messages.get(0).toString());
            inputTopic2.pipeInput("A", messages.get(1).toString());
            inputTopic2.pipeInput("A", messages.get(2).toString());
        }
        assertEquals(2, processorSupplier.theCapturedProcessor().processed.size());
        int index = 0;
        int timestamp = 0;
        for (Object result:processorSupplier.theCapturedProcessor().processed){
            Assert.assertEquals(new KeyValueTimestamp<>("A",expected.get(index++).toString(), timestamp),
                    result);
            timestamp = timestamp+ 1000;
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