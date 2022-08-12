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
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class OperatorFlexTest {

    static TestFlexOperator testOperator;

    @Test
    public void testOneFilterValue(){
        JSONArray messages = new JSONHelper().parseFile("operatorFlex/messages-1.json");
        String configString = new JSONHelper().parseFile("operatorFlex/config-1.json").toString();
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

    @Test
    public void testTwoFlexValues(){
        JSONArray messages = new JSONHelper().parseFile("operatorFlex/messages-3.json");
        String configString = new JSONHelper().parseFile("operatorFlex/config-2.json").toString();
        Config config = new Config(configString);
        ConfigProvider.setConfig(config);
        Message message = new Message();
        MessageModel model =  new MessageModel();
        testOperator = new TestFlexOperator();
        testOperator.configMessage(message);
        int index = 0;
        for(Object msg : messages){
            String topicName = config.getInputTopicsConfigs().get(index % 2).getName();
            DeviceMessageModel deviceMessageModel = JSONHelper.getObjectFromJSONString(msg.toString(), DeviceMessageModel.class);
            assert deviceMessageModel != null;
            model.putMessage(topicName, Helper.deviceToInputMessageModel(deviceMessageModel, topicName));
            if (index % 2 != 0){
                message.setMessage(model);
                testOperator.run(message);
            }
            index++;
        }

        Assert.assertEquals( 4.0, message.getMessage().getOutputMessage().getAnalytics().get("test"));
        Assert.assertEquals( 6.0, message.getMessage().getOutputMessage().getAnalytics().get("test2"));
    }

    @Test
    public void testTwoFlexInputsOneMessageDifferentValues(){
        JSONArray messages = new JSONHelper().parseFile("operatorFlex/messages-4.json");
        String configString = new JSONHelper().parseFile("operatorFlex/config-4.json").toString();
        Config config = new Config(configString);
        ConfigProvider.setConfig(config);
        Message message = new Message();
        MessageModel model =  new MessageModel();
        testOperator = new TestFlexOperator();
        testOperator.configMessage(message);
        for(Object msg : messages){
            String topicName = config.getInputTopicsConfigs().get(0).getName();
            DeviceMessageModel deviceMessageModel = JSONHelper.getObjectFromJSONString(msg.toString(), DeviceMessageModel.class);
            assert deviceMessageModel != null;
            model.putMessage(topicName, Helper.deviceToInputMessageModel(deviceMessageModel, topicName));
            message.setMessage(model);
            testOperator.run(message);
        }

        Assert.assertEquals( 10.0, message.getMessage().getOutputMessage().getAnalytics().get("test"));
        Assert.assertEquals( 10.0, message.getMessage().getOutputMessage().getAnalytics().get("test2"));
    }

    @Test
    public void testCurrentFilterValuesWithUnchangingValues(){
        testCurrentFilterValues("operatorFlex/messages-2.json");
    }

    @Test
    public void testCurrentFilterValuesWithChangingValues(){
        testCurrentFilterValues("operatorFlex/messages-3.json");
    }

    private void testCurrentFilterValues(String messageFile) {
        JSONArray messages = new JSONHelper().parseFile(messageFile);
        String configString = new JSONHelper().parseFile("operatorFlex/config-3.json").toString();
        Config config = new Config(configString);
        ConfigProvider.setConfig(config);
        testOperator = new TestFlexOperator();
        int index = 0;
        Message message = new Message();
        MessageModel model = new MessageModel();
        testOperator.configMessage(message);
        for(Object msg : messages){
            String topicName = config.getInputTopicsConfigs().get(index % 2).getName();
            DeviceMessageModel deviceMessageModel = JSONHelper.getObjectFromJSONString(msg.toString(), DeviceMessageModel.class);
            assert deviceMessageModel != null;
            model.putMessage(topicName, Helper.deviceToInputMessageModel(deviceMessageModel, topicName));
            message.setMessage(model);
            testOperator.run(message);
            Assert.assertEquals("" + (index % 2), message.getMessage().getOutputMessage().getAnalytics().get("test"));
            index++;
        }
    }
}
