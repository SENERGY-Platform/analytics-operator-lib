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

package org.infai.seits.sepl.operators.test;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.infai.seits.sepl.operators.Config;
import org.infai.seits.sepl.operators.Stream;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class StreamTest {

    private static final String APP_ID = "app-id";

    private KStreamTestDriver driver = new KStreamTestDriver();

    private File stateDir = new File("./state/stream");

    @Test
    public void testProcessSingleStream(){

        Stream stream = new Stream();
        stream.setPipelineId("1");
        TestOperator operator = new TestOperator();
        JSONFileReader reader = new JSONFileReader();
        Config config = new Config(reader.parseFile("stream/processSingleStreamConfig.json").toString());
        stream.processSingleStream(operator, config.getTopicConfig());

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        driver.setUp(stream.builder.getBuilder());
        driver.setTime(0L);

        driver.process("topic1", "A", "{'pipeline_id': '1', 'operator_id': '1'}");
        driver.process("topic1", "B", "{'pipeline_id': '2', 'operator_id': '1'}");
        String time2 = stream.builder.time;
        driver.process("topic1", "D", "{'pipeline_id': '1', 'operator_id': '1'}");
        driver.process("topic1", "G", "{'pipeline_id': '1', 'operator_id': '2'}");

        Assert.assertEquals(Utils.mkList("A:{\"analytics\":{\"test\":\"1\"},\"inputs\":[{\"operator_id\":\"1\"," +
                        "\"pipeline_id\":\"1\"}],\"time\":\""+ time2 +"\"}",
                "D:{\"analytics\":{\"test\":\"1\"},\"inputs\":[{\"operator_id\":\"1\"," +
                        "\"pipeline_id\":\"1\"}],\"time\":\""+ stream.builder.time +"\"}"), processorSupplier.processed);
    }

    @Test
    public void testProcessSingleStreamDeviceId(){

        Stream stream = new Stream("1", "1");
        TestOperator operator = new TestOperator();

        Config config = new Config("{\"inputTopics\": [{\"Name\":\"topic1\",\"FilterType\":\"DeviceId\",\"FilterValue\":\"1\",\"Mappings\":[{\"Dest\":\"value\",\"Source\":\"value.reading.value\"}]}]}");
        stream.processSingleStream(operator, config.getTopicConfig());

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        driver.setUp(stream.builder.getBuilder());
        driver.setTime(0L);

        driver.process("topic1", "A", "{'device_id': '1'}");
        String time2 = stream.builder.time;
        driver.process("topic1", "B", "{'device_id': '2'}");
        driver.process("topic1", "D", "{'device_id': '1'}");

        Assert.assertEquals(Utils.mkList("A:{\"analytics\":{\"test\":\"1\"},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\"}]" +
                        ",\"pipeline_id\":\"1\",\"time\":\""+ time2 +"\"}",
                "D:{\"analytics\":{\"test\":\"1\"},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\"}]" +
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
        Config config = new Config("{\"inputTopics\": [" +
                "{\"Name\":\"topic1\",\"FilterType\":\"DeviceId\",\"FilterValue\":\"1\",\"Mappings\":[{\"Dest\":\"value1\",\"Source\":\"value.reading.value\"}]}," +
                "{\"Name\":\"topic2\",\"FilterType\":\"DeviceId\",\"FilterValue\":\"2\",\"Mappings\":[{\"Dest\":\"value2\",\"Source\":\"value.reading.value\"}]}" +
                "]}");
        stream.builder.setWindowTime(5);

        stream.processMultipleStreams(operator, config.getTopicConfig());


        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        driver.setUp(stream.builder.getBuilder(), stateDir);

        driver.setTime(0L);
        driver.process("topic1", null, "{'pipeline_id': '1', 'inputs':[{'device_id': '3', 'value':1}], 'analytics':{}}");
        driver.process("topic2", null, "{'pipeline_id': '1', 'inputs':[{'device_id': '4', 'value':1}], 'analytics':{}}");
        driver.process("topic2", null, "{'device_id': '2', 'value':3}");
        driver.setTime(5001L);
        driver.process("topic1", null, "{'device_id': '1', 'value':2}");
        driver.setTime(7001L);
        driver.process("topic2", null, "{'device_id': '2', 'value':2}");

        Assert.assertEquals(Utils.mkList("A:{\"analytics\":{\"test\":\"1\"},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"value\":2},{\"device_id\":\"2\",\"value\":2}]" +
                        ",\"pipeline_id\":\"1\",\"time\":\""+ stream.builder.time +"\"}"),
                processorSupplier.processed);

    }

    private void testProcessMultipleStreams(int numStreams){
        try{
            FileUtils.deleteDirectory(stateDir);
        } catch (IOException e){
            System.err.println("Could not delete stateDir: " + e.getMessage());
        }
        Stream stream = new Stream("1", "1");
        TestOperator operator = new TestOperator();

        String configString = "{\"inputTopics\": [";
        for(int i = 1; i <= numStreams; i++){
            configString += "{\"Name\":\"topic"+i+"\",\"FilterType\":\"DeviceId\",\"FilterValue\":\""+i+"\",\"Mappings\":[{\"Dest\":\"value"+i+"\",\"Source\":\"value.reading.value\"}]},";
        }
        configString = configString.substring(0, configString.length()-1); //remove last ','
        configString += "]}";


        Config config = new Config(configString);
        stream.builder.setWindowTime(5);

        stream.processMultipleStreams(operator, config.getTopicConfig());


        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        driver.setUp(stream.builder.getBuilder(), stateDir);

        driver.setTime(0L);

        for(int i = 1; i <= numStreams; i++){
            driver.process("topic"+i, null, "{'device_id': '"+i+"', 'value':"+i+"}");
            driver.process("topic"+i, null, "{'pipeline_id': '1', 'inputs':[{'device_id': 'abc', 'value':1}], 'analytics':{}}"); //To test filtering
        }

        String expected = "A:{\"analytics\":{\"test\":\"1\"},\"operator_id\":\"1\",\"inputs\":[";
        for(int i = 1; i <= numStreams; i++){
            expected += "{\"device_id\":\""+i+"\",\"value\":"+i+"},";
        }
        expected = expected.substring(0, expected.length()-1); //remove last ','
        expected += "],\"pipeline_id\":\"1\",\"time\":\""+ stream.builder.time +"\"}";

        driver.close();

        Assert.assertEquals(Utils.mkList(expected), processorSupplier.processed);

    }

    //@Test
    public void test5Streams(){
        testProcessMultipleStreams(5);
    }

    @Test
    public void test128Streams(){
        testProcessMultipleStreams(128);
    }

    @Test
    public void test2Streams(){
        stateDir = new File("./state/stream/2");
        testProcessMultipleStreams(2);
    }

    @Test
    public void testComplexMessage(){
        try{
            FileUtils.deleteDirectory(stateDir);
        } catch (IOException e){
            System.err.println("Could not delete stateDir: " + e.getMessage());
        }
        Stream stream = new Stream("1", "1");
        TestOperator operator = new TestOperator();

        String configString = "{\"inputTopics\":[{\"name\":\"topic1\",\"filterType\":\"DeviceId\",\"filterValue\":\"1\",\"mappings\":[{\"dest\":\"value1\",\"source\":\"value.reading.OBIS_1_8_0.value\"},{\"dest\":\"timestamp1\",\"source\":\"value.reading.time\"}]},{\"name\":\"topic2\",\"filterType\":\"DeviceId\",\"filterValue\":\"2\",\"mappings\":[{\"dest\":\"value2\",\"source\":\"value.reading.OBIS_1_8_0.value\"},{\"dest\":\"timestamp2\",\"source\":\"value.reading.time\"}]},{\"name\":\"topic3\",\"filterType\":\"DeviceId\",\"filterValue\":\"3\",\"mappings\":[{\"dest\":\"value3\",\"source\":\"value.reading.OBIS_1_8_0.value\"},{\"dest\":\"timestamp3\",\"source\":\"value.reading.time\"}]}]}";


        Config config = new Config(configString);
        stream.builder.setWindowTime(5);

        stream.processMultipleStreams(operator, config.getTopicConfig());


        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        stream.getOutputStream().process(processorSupplier);

        driver.setUp(stream.builder.getBuilder(), stateDir);

        driver.setTime(0L);
        driver.process("topic1", null, "{'pipeline_id': '1', 'inputs':[{'device_id': 'abc', 'value':1}], 'analytics':{}}"); //To test filtering
        driver.process("topic2", null, "{'pipeline_id': '1', 'inputs':[{'device_id': 'abc', 'value':1}], 'analytics':{}}"); //To test filtering

        driver.process("topic1", null, "{'device_id':'1','service_id':'1','value':{'reading':{'OBIS_16_7':{'unit':'kW','value':0.36},'OBIS_1_8_0':{'unit':'kWh','value':226.239},'time':'2019-07-18T12:19:04.250355Z'}}}");
        driver.process("topic2", null, "{'device_id':'2','service_id':'2','value':{'reading':{'OBIS_16_7':{'unit':'kW','value':0.36},'OBIS_1_8_0':{'unit':'kWh','value':226.239},'time':'2019-07-18T12:19:04.250355Z'}}}");
        driver.process("topic3", null, "{'device_id':'3','service_id':'3','value':{'reading':{'OBIS_16_7':{'unit':'kW','value':0.36},'OBIS_1_8_0':{'unit':'kWh','value':226.239},'time':'2019-07-18T12:19:04.250355Z'}}}");

        String expected = "A:{\"analytics\":{\"test\":\"1\"},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"service_id\":\"1\",\"value\":{\"reading\":{\"OBIS_16_7\":{\"unit\":\"kW\",\"value\":0.36},\"time\":\"2019-07-18T12:19:04.250355Z\",\"OBIS_1_8_0\":{\"unit\":\"kWh\",\"value\":226.239}}}},{\"device_id\":\"2\",\"service_id\":\"2\",\"value\":{\"reading\":{\"OBIS_16_7\":{\"unit\":\"kW\",\"value\":0.36},\"time\":\"2019-07-18T12:19:04.250355Z\",\"OBIS_1_8_0\":{\"unit\":\"kWh\",\"value\":226.239}}}},{\"device_id\":\"3\",\"service_id\":\"3\",\"value\":{\"reading\":{\"OBIS_16_7\":{\"unit\":\"kW\",\"value\":0.36},\"time\":\"2019-07-18T12:19:04.250355Z\",\"OBIS_1_8_0\":{\"unit\":\"kWh\",\"value\":226.239}}}}],\"pipeline_id\":\"1\",\"time\":\""+stream.builder.time+"\"}";

        Assert.assertEquals(expected, processorSupplier.processed.get(0));
    }
}
