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

import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Message;
import org.infai.ses.senergy.testing.utils.JSONFileReader;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.json.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class MessageTest {

    @Test
    public void testGetMessageEntityId() {
        Config config = new Config(new JSONFileReader().parseFile("message/testGetMessageEntityIdConfig.json").toString());
        ConfigProvider.setConfig(config);
        JSONObject jsonMessage = new JSONFileReader().parseFile("message/testGetMessageEntityIdMessage.json");
        Message message = new Message(jsonMessage.toString());
        Assert.assertEquals("134534", message.getMessageEntityId());
    }

    @Test
    public void testInputValue(){
        Message message = new Message("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"val\":\"2\"},{\"device_id\":\"2\",\"value\":1}],\"pipeline_id\":\"1\"}");
        ConfigProvider.setConfig(new Config("{ \"inputTopics\":[\n" +
                "  {\n" +
                "    \"Name\": \"analytics-diff\",\n" +
                "    \"FilterType\": \"DeviceId\",\n" +
                "    \"FilterValue\": \"1\",\n" +
                "    \"Mappings\": [\n" +
                "      {\n" +
                "        \"Dest\": \"value\",\n" +
                "        \"Source\": \"val\"\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "]}\n"));
        message.addInput("value");
        Double value = Double.valueOf(message.getInput("value").getValue());
        Assert.assertEquals(Double.valueOf(2.0), value);
    }

    @Test
    public void testOutputValue(){
        Message message = new Message("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"val\":\"2\"},{\"device_id\":\"2\",\"value\":1}],\"pipeline_id\":\"1\"}");
        message.output("test", Double.valueOf(2));
        Assert.assertEquals("{\"analytics\":{\"test\":2.0},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"val\":\"2\"},{\"device_id\":\"2\",\"value\":1}],\"pipeline_id\":\"1\"}", message.getMessageString());
    }

    @Test
    public void testArrayValue(){
        ConfigProvider.setConfig(new Config("{ \"inputTopics\":[\n" +
                "  {\n" +
                "    \"Name\": \"analytics-diff\",\n" +
                "    \"FilterType\": \"DeviceId\",\n" +
                "    \"FilterValue\": \"1\",\n" +
                "    \"Mappings\": [\n" +
                "      {\n" +
                "        \"Dest\": \"value\",\n" +
                "        \"Source\": \"val\"\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "]}\n"));
        Message message = new Message("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"val\":[1, 2, 3]},{\"device_id\":\"2\",\"value\":1}],\"pipeline_id\":\"1\"}");
        message.addInput("value");
        JSONArray expected = new JSONArray().put(1).put(2).put(3);
        Assert.assertEquals(expected.toString(), message.getInput("value").getJSONArray().toString());
    }
}
