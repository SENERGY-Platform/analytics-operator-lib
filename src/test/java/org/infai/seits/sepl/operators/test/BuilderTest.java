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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.infai.seits.sepl.operators.Builder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class BuilderTest {

    private static final String APP_ID = "app-id";

    private KStreamTestDriver driver = new KStreamTestDriver();

    @Test
    public void testFilterBy(){
        Builder builder = new Builder("1", "1");
        final String topic1 = "input-stream";
        final String deviceIdPath = "device_id";
        final String deviceId = "1";

        final KStream<String, String> source1 = builder.getBuilder().stream(topic1);

        final KStream<String, String> filtered = builder.filterBy(source1, deviceIdPath, deviceId);
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
    public void testJoinStreams(){

        Builder builder = new Builder("1", "1");
        final String topic1 = "input-stream";
        final String topic2 = "input-stream2";

        final KStream<String, String> source1 = builder.getBuilder().stream(topic1);
        final KStream<String, String> source2 = builder.getBuilder().stream(topic2);

        final KStream<String, String> merged = builder.joinStreams(source1, source2);
        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        merged.process(processorSupplier);

        driver.setUp(builder.getBuilder(), new File( "./state" ));
        driver.setTime(0L);
        driver.process(topic1, "A", "{'device_id': '1', 'value':1}");
        driver.process(topic2, "A", "{'device_id': '2', 'value':1}");
        String time2 = builder.time;
        driver.process(topic2, "A", "{'device_id': '2', 'value':3}");


        Assert.assertEquals(Utils.mkList(
                "A:{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"value\":1},{\"device_id\":\"2\",\"value\":1}]," +
                        "\"pipeline_id\":\"1\",\"time\":\""+ time2 +"\"}",
        "A:{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"value\":1},{\"device_id\":\"2\",\"value\":3}]," +
                "\"pipeline_id\":\"1\",\"time\":\""+ builder.time +"\"}"),
                processorSupplier.processed);
    }

    @Test
    public void testFormatMessage(){
        Builder builder = new Builder("1", "1");
        String message = builder.formatMessage("{'device_id': '1'}");
        Assert.assertEquals("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\"}]," +
                "\"pipeline_id\":\"1\",\"time\":\""+ builder.time +"\"}",message);
    }

    @Test
    public void testFormatMessage2(){
        Builder builder = new Builder("1", "2");
        String message = builder.formatMessage("{'analytics':{'test': 1},'inputs':[{'device_id': '1'}],'pipeline_id':'1'}");
        Assert.assertEquals("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"analytics\":{\"test\":1}," +
                "\"inputs\":[{\"device_id\":\"1\"}],\"pipeline_id\":\"1\"}],\"pipeline_id\":\"2\",\"time\":\""+ builder.time +"\"}",message);
    }

}
