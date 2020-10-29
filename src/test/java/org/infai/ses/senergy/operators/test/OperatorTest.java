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

import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.models.DeviceMessageModel;
import org.infai.ses.senergy.models.MessageModel;
import org.infai.ses.senergy.operators.Helper;
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Message;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.json.simple.JSONArray;
import org.junit.Assert;
import org.junit.Test;
import org.infai.ses.senergy.testing.utils.JSONHelper;

public class OperatorTest {

    private TestOperator testOperator;
    private JSONArray messages = new JSONHelper().parseFile("operator/messages.json");
    private final String configString = new JSONHelper().parseFile("operator/config-1.json").toString();

    @Test
    public void testTwoFilterValues(){
        Config config = new Config(configString);
        String topicName = config.getInputTopicsConfigs().get(0).getName();
        ConfigProvider.setConfig(config);
        Message message = new Message();
        MessageModel model =  new MessageModel();
        testOperator = new TestOperator();
        testOperator.configMessage(message);
        for(Object msg : messages){
            DeviceMessageModel deviceMessageModel = JSONHelper.getObjectFromJSONString(msg.toString(), DeviceMessageModel.class);
            assert deviceMessageModel != null;
            model.putMessage(topicName, Helper.deviceToInputMessageModel(deviceMessageModel, topicName));
            message.setMessage(model);
            testOperator.run(message);
            Assert.assertEquals( "1", message.getMessage().getOutputMessage().getAnalytics().get("test"));
        }

    }

    @Test
    public void testTwoFilterValuesWithMessagesWithTwoValues(){
        Config config = new Config(new JSONHelper().parseFile("operator/config-2.json").toString());
        messages = new JSONHelper().parseFile("operator/messages-2.json");
        String topicName = config.getInputTopicsConfigs().get(0).getName();
        ConfigProvider.setConfig(config);
        Message message = new Message();
        MessageModel model =  new MessageModel();
        testOperator = new TestOperator();
        testOperator.configMessage(message);
        for(Object msg : messages){
            DeviceMessageModel deviceMessageModel = JSONHelper.getObjectFromJSONString(msg.toString(), DeviceMessageModel.class);
            assert deviceMessageModel != null;
            model.putMessage(topicName, Helper.deviceToInputMessageModel(deviceMessageModel, topicName));
            message.setMessage(model);
            testOperator.run(message);
            Assert.assertEquals( "5.5", message.getMessage().getOutputMessage().getAnalytics().get("val"));
            Assert.assertEquals( "3.5", message.getMessage().getOutputMessage().getAnalytics().get("val2"));
        }

    }

    @Test
    public void testReadingValuesFromTwoInputs() {
        Config config = new Config(new JSONHelper().parseFile("operator/config-3.json").toString());
        JSONArray messages = new JSONHelper().parseFile("operator/messages-3.json");
        String topicName = config.getInputTopicsConfigs().get(0).getName();
        ConfigProvider.setConfig(config);
        Message message = new Message();
        MessageModel model =  new MessageModel();
        testOperator = new TestOperator();
        testOperator.configMessage(message);
        String [] expected = new String[]{"1.0", "2.0"};
        int index = 0;
        for (Object msg: messages) {
            AnalyticsMessageModel analyticsMessageModel = JSONHelper.getObjectFromJSONString(msg.toString(), AnalyticsMessageModel.class);
            assert analyticsMessageModel != null;
            model.putMessage(topicName, Helper.analyticsToInputMessageModel(analyticsMessageModel, topicName));
            message.setMessage(model);
            testOperator.run(message);
            Assert.assertEquals( expected[index++], message.getMessage().getOutputMessage().getAnalytics().get("val"));
        }
    }
}
