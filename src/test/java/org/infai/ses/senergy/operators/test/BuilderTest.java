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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.infai.ses.senergy.operators.Builder;
import org.infai.ses.senergy.testing.utils.JSONFileReader;
import org.infai.ses.senergy.utils.TimeProvider;
import org.json.simple.JSONArray;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;

public class BuilderTest extends TestCase {

    private static final String APP_ID = "app-id";

    private KStreamTestDriver driver = new KStreamTestDriver();

    private File stateDir = new File("./state/builder");

    private final LocalDateTime time = LocalDateTime.of(2020,01,01,01,01);

    @Override
    protected void setUp() throws Exception {
        TimeProvider.useFixedClockAt(time);
    }

    @Test
    public void testFilterBy(){
        Builder builder = new Builder("1", "1");
        final String topic1 = "input-stream";
        final String deviceIdPath = "device_id";
        final String[] deviceIds = new String[] {"1"};

        final KStream<String, String> source1 = builder.getBuilder().stream(topic1);

        final KStream<String, String> filtered = builder.filterBy(source1, deviceIdPath, deviceIds);
        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        filtered.process(processorSupplier);

        driver.setUp(builder.getBuilder());
        driver.setTime(0L);

        driver.process(topic1, "A", "{'device_id': '1'}");
        driver.process(topic1, "B", "{'device_id': '2'}");
        driver.process(topic1, "D", "{'device_id': '1'}");

        Assert.assertEquals(Utils.mkList("A:{'device_id': '1'}", "D:{'device_id': '1'}"), processorSupplier.processed);
    }

    @Test
    public void testFilterByMultipleDevices(){
        Builder builder = new Builder("1", "1");
        final String topic1 = "input-stream";
        final String deviceIdPath = "device_id";
        final String[] deviceIds = new String[] {"1", "2"};

        final KStream<String, String> source1 = builder.getBuilder().stream(topic1);

        final KStream<String, String> filtered = builder.filterBy(source1, deviceIdPath, deviceIds);
        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        filtered.process(processorSupplier);

        driver.setUp(builder.getBuilder());
        driver.setTime(0L);

        driver.process(topic1, "A", "{'device_id': '1'}");
        driver.process(topic1, "B", "{'device_id': '2'}");
        driver.process(topic1, "D", "{'device_id': '1'}");

        Assert.assertEquals(Utils.mkList("A:{'device_id': '1'}", "B:{'device_id': '2'}", "D:{'device_id': '1'}"), processorSupplier.processed);
    }

    @Test
    public void testJoinStreams(){
        JSONArray messages = new JSONFileReader().parseFile("builder/messages.json");
        JSONArray results = new JSONFileReader().parseFile("builder/results.json");
        Builder builder = new Builder("1", "1");
        final String topic1 = "input-stream";
        final String topic2 = "input-stream2";

        final KStream<String, String> source1 = builder.getBuilder().stream(topic1);
        final KStream<String, String> source2 = builder.getBuilder().stream(topic2);

        final KStream<String, String> merged = builder.joinStreams(source1, source2);
        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        merged.process(processorSupplier);
        driver.setUp(builder.getBuilder(), stateDir);
        driver.setTime(0L);
        driver.process(topic1, "A", messages.get(0).toString());
        driver.process(topic2, "A", messages.get(1).toString());
        driver.process(topic2, "A", messages.get(2).toString());
        Assert.assertEquals(2, processorSupplier.processed.size());
        int index = 0;
        for (Object result:results){
            Assert.assertEquals(result.toString(),processorSupplier.processedValues.get(index++));
        }
    }

    @Test
    public void testFormatMessage(){
        Builder builder = new Builder("1", "1");
        String message = builder.formatMessage("{'device_id': '1'}");
        Assert.assertEquals("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\"}]," +
                "\"pipeline_id\":\"1\",\"time\":\""+ TimeProvider.nowUTCToString() +"\"}",message);
    }

    @Test
    public void testFormatMessage2(){
        Builder builder = new Builder("1", "2");
        String message = builder.formatMessage("{'analytics':{'test': 1},'inputs':[{'device_id': '1'}],'pipeline_id':'1'}");
        Assert.assertEquals("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"analytics\":{\"test\":1}," +
                "\"inputs\":[{\"device_id\":\"1\"}],\"pipeline_id\":\"1\"}],\"pipeline_id\":\"2\",\"time\":\""+ TimeProvider.nowUTCToString() +"\"}",message);
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
