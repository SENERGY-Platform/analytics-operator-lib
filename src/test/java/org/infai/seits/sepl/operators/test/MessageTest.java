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

import org.infai.seits.sepl.operators.Message;
import org.junit.Assert;
import org.junit.Test;

public class MessageTest {

    @Test
    public void testInputValue(){
        Message message = new Message("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"val\":\"2\"},{\"device_id\":\"2\",\"value\":1}],\"pipeline_id\":\"1\"}");
        message.setConfig("[\n" +
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
                "]\n");
        message.addInput("value");
        Double value = new Double(message.getInput("value").getValue());
        Assert.assertEquals(new Double(2.0), value);
    }

    @Test
    public void testOutputValue(){
        Message message = new Message("{\"analytics\":{},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"val\":\"2\"},{\"device_id\":\"2\",\"value\":1}],\"pipeline_id\":\"1\"}");
        message.output("test", new Double(2));
        Assert.assertEquals("{\"analytics\":{\"test\":2.0},\"operator_id\":\"1\",\"inputs\":[{\"device_id\":\"1\",\"val\":\"2\"},{\"device_id\":\"2\",\"value\":1}],\"pipeline_id\":\"1\"}", message.getMessageString());
    }
}
