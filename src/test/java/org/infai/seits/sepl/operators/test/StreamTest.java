/*
 * Copyright 2018 InfAI (CC SES)
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

package org.infai.seits.sepl.operators.test;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.infai.seits.sepl.operators.Builder;
import org.infai.seits.sepl.operators.Stream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class StreamTest {

    private static final String APP_ID = "app-id";

    private KStreamTestDriver driver = new KStreamTestDriver();

    private File stateDir = new File("./state");

    @Test
    public void testProcessSingleStream(){

        Stream stream = new Stream();
        stream.setPipelineId("1");
        TestOperator operator = new TestOperator();

        JSONArray topicConfig = new JSONArray("[{\"Name\":\"topic1\",\"FilterType\":\"OperatorId\",\"FilterValue\":\"1\",\"Mappings\":[{\"Dest\":\"value\",\"Source\":\"value.reading.value\"}]}]");
        stream.processSingleStream(operator, topicConfig);

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        driver.setUp(stream.builder.getBuilder());
        driver.setTime(0L);

        driver.process("topic1", "A", "{'pipeline_id': '1', 'operator_id': '1'}");
        driver.process("topic1", "B", "{'pipeline_id': '2', 'operator_id': '1'}");
        String time2 = stream.builder.time;
        driver.process("topic1", "D", "{'pipeline_id': '1', 'operator_id': '1'}");
        driver.process("topic1", "G", "{'pipeline_id': '1', 'operator_id': '2'}");

        Assert.assertEquals(Utils.mkList("A:{\"analytics\":{},\"inputs\":[{\"operator_id\":\"1\"," +
                        "\"pipeline_id\":\"1\"}],\"time\":\""+ time2 +"\"}",
                "D:{\"analytics\":{},\"inputs\":[{\"operator_id\":\"1\"," +
                        "\"pipeline_id\":\"1\"}],\"time\":\""+ stream.builder.time +"\"}"), processorSupplier.processed);
    }

    @Test
    public void testProcessSingleStreamDeviceId(){

        Stream stream = new Stream("1", "1");
        TestOperator operator = new TestOperator();

        JSONArray topicConfig = new JSONArray("[{\"Name\":\"topic1\",\"FilterType\":\"DeviceId\",\"FilterValue\":\"1\",\"Mappings\":[{\"Dest\":\"value\",\"Source\":\"value.reading.value\"}]}]");
        stream.processSingleStream(operator, topicConfig);

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        driver.setUp(stream.builder.getBuilder());
        driver.setTime(0L);

        driver.process("topic1", "A", "{'device_id': '1'}");
        String time2 = stream.builder.time;
        driver.process("topic1", "B", "{'device_id': '2'}");
        driver.process("topic1", "D", "{'device_id': '1'}");

        Assert.assertEquals(Utils.mkList("A:{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\"}]" +
                        ",\"pipeline_id\":\"1\",\"time\":\""+ time2 +"\"}",
                "D:{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\"}]" +
                        ",\"pipeline_id\":\"1\",\"time\":\""+ stream.builder.time +"\"}"), processorSupplier.processed);
    }

    @Test
    public void testProcessTwoStreams2DeviceId(){

        try{
            FileUtils.deleteDirectory(stateDir);
        } catch (IOException e){

        }
        Stream stream = new Stream("1", "1");
        TestOperator operator = new TestOperator();
        JSONArray topicConfig = new JSONArray("[" +
                "{\"Name\":\"topic1\",\"FilterType\":\"DeviceId\",\"FilterValue\":\"1\",\"Mappings\":[{\"Dest\":\"value1\",\"Source\":\"value.reading.value\"}]}," +
                "{\"Name\":\"topic2\",\"FilterType\":\"DeviceId\",\"FilterValue\":\"2\",\"Mappings\":[{\"Dest\":\"value2\",\"Source\":\"value.reading.value\"}]}" +
                "]");
        stream.builder.setWindowTime(5);

        stream.processTwoStreams(operator, topicConfig);


        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        driver.setUp(stream.builder.getBuilder(), new File( "./state" ));

        driver.setTime(0L);
        driver.process("topic1", null, "{'pipeline_id': '1', 'inputs':[{'device_id': '3', 'value':1}], 'analytics':{}}");
        driver.process("topic2", null, "{'pipeline_id': '1', 'inputs':[{'device_id': '4', 'value':1}], 'analytics':{}}");
        driver.process("topic2", null, "{'device_id': '2', 'value':3}");
        driver.setTime(5001L);
        driver.process("topic1", null, "{'device_id': '1', 'value':2}");
        driver.setTime(7001L);
        driver.process("topic2", null, "{'device_id': '2', 'value':2}");

        Assert.assertEquals(Utils.mkList("A:{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"value\":2},{\"device_id\":\"2\",\"value\":2}]" +
                        ",\"pipeline_id\":\"1\",\"time\":\""+ stream.builder.time +"\"}"),
                processorSupplier.processed);

    }
}
