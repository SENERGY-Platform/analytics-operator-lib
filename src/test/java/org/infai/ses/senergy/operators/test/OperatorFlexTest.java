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
import org.infai.ses.senergy.testing.utils.JSONFileReader;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class OperatorFlexTest extends TestCase {

    static TestFlexOperator testOperator;
    protected JSONArray messages = new JSONFileReader().parseFile("operatorFlex/messages-1.json");
    static String configString = new JSONFileReader().parseFile("operatorFlex/config-1.json").toString();


    @Override
    protected void setUp() throws Exception {
    }

    @Test
    public void testTwoFilterValues(){
        ConfigProvider.setConfig(new Config(configString));
        Builder builder = new Builder("1", "1");
        Message message = new Message();
        testOperator = new TestFlexOperator();
        testOperator.configMessage(message);
        Map <String, Double> map = new HashMap<>();
        map.put("test", 11.0);
        for(Object msg : messages){
            message.setMessage(builder.formatMessage(msg.toString()));
            testOperator.run(message);
            Assert.assertEquals(new JSONObject(map), message.<JSONObject>getValue("analytics"));
        }

    }
}
