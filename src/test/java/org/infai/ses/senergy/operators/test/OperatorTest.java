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
import org.infai.ses.senergy.operators.Builder;
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Message;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.junit.Assert;
import org.junit.Test;
import org.infai.ses.senergy.testing.utils.JSONHelper;

import java.util.HashMap;
import java.util.Map;

public class OperatorTest extends TestCase {

    static TestOperator testOperator;
    protected JSONArray messages = new JSONHelper().parseFile("operator/messages.json");
    static String configString = new JSONHelper().parseFile("operator/config-1.json").toString();


    @Override
    protected void setUp() throws Exception {
    }

    @Test
    public void testTwoFilterValues(){
        ConfigProvider.setConfig(new Config(configString));
        Builder builder = new Builder("1", "1");
        Message message = new Message();
        testOperator = new TestOperator();
        testOperator.configMessage(message);
        Map <String, String> map = new HashMap<>();
        map.put("test", "1");
        for(Object msg : messages){
            message.setMessage(builder.formatMessage(msg.toString()));
            testOperator.run(message);
            Assert.assertEquals(new JSONObject(map), JSONHelper.<JSONObject>getValue("analytics", message.getMessageString()));
            Assert.assertEquals(msg, JSONHelper.<JSONArray>getValue("inputs", message.getMessageString()).get(0));
        }

    }

    @Test
    public void testTwoFilterValuesWithMessagesWithTwoValues(){
        ConfigProvider.setConfig(new Config(new JSONHelper().parseFile("operator/config-2.json").toString()));
        Builder builder = new Builder("1", "1");
        Message message = new Message();
        testOperator = new TestOperator();
        testOperator.configMessage(message);
        Map <String, String> map = new HashMap<>();
        map.put("val", "5.5");
        map.put("val2", "3.5");
        messages = new JSONHelper().parseFile("operator/messages-2.json");
        for(Object msg : messages){
            message.setMessage(builder.formatMessage(msg.toString()));
            testOperator.run(message);
            Assert.assertEquals(new JSONObject(map), JSONHelper.<JSONObject>getValue("analytics", message.getMessageString()));
            Assert.assertEquals(msg, JSONHelper.<JSONArray>getValue("inputs", message.getMessageString()).get(0));
        }

    }

    @Test
    public void testReadingValuesFromTwoInputs() {
        ConfigProvider.setConfig(new Config(new JSONHelper().parseFile("operator/config-3.json").toString()));
        Message message = new Message();
        testOperator = new TestOperator();
        Map <String, String> map = new HashMap<>();
        map.put("val", "1.0");
        map.put("val2", "2.0");
        String msgInputs = new JSONHelper().parseFile("operator/messages-3.json").toString();
        message.setMessage(msgInputs);
        testOperator.configMessage(message);
        testOperator.run(message);
        Assert.assertEquals(new JSONObject(map), JSONHelper.<JSONObject>getValue("analytics", message.getMessageString()));
    }
}
