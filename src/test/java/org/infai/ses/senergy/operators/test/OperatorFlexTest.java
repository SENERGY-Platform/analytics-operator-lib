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

import org.infai.ses.senergy.models.DeviceMessageModel;
import org.infai.ses.senergy.models.MessageModel;
import org.infai.ses.senergy.operators.Helper;
import org.infai.ses.senergy.operators.StreamBuilder;
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Message;
import org.infai.ses.senergy.testing.utils.JSONHelper;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class OperatorFlexTest {

    static TestFlexOperator testOperator;
    protected JSONArray messages = new JSONHelper().parseFile("operatorFlex/messages-1.json");
    static String configString = new JSONHelper().parseFile("operatorFlex/config-1.json").toString();


    @Before
    public void setUp() throws Exception {
    }


    @Test
    public void testTwoFilterValues(){
        Config config = new Config(configString);
        ConfigProvider.setConfig(config);
        Message message = new Message();
        MessageModel model =  new MessageModel();
        testOperator = new TestFlexOperator();
        testOperator.configMessage(message);
        int index = 0;
        for(Object msg : messages){
            String topicName = config.getInputTopicsConfigs().get(index++).getName();
            DeviceMessageModel deviceMessageModel = JSONHelper.getObjectFromJSONString(msg.toString(), DeviceMessageModel.class);
            assert deviceMessageModel != null;
            model.putMessage(topicName, Helper.deviceToInputMessageModel(deviceMessageModel, topicName));
        }
        message.setMessage(model);
        testOperator.run(message);
        Assert.assertEquals( 21.0, message.getMessage().getOutputMessage().getAnalytics().get("test"));
    }
}
