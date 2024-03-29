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

import org.infai.ses.senergy.exceptions.NoValueException;
import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.models.DeviceMessageModel;
import org.infai.ses.senergy.models.MessageModel;
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Helper;
import org.infai.ses.senergy.operators.Message;
import org.infai.ses.senergy.testing.utils.JSONHelper;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.TimeProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;

public class MessageTest {

    private final LocalDateTime time = LocalDateTime.of(2020, 01, 01, 01, 01);

    @Before
    public void setUp() {
        TimeProvider.useFixedClockAt(time);
    }

    @Test
    public void testInputValue() throws NoValueException {
        ConfigProvider.setConfig(new Config(new JSONHelper().parseFile("message/testInputValueConfig.json").toString()));
        AnalyticsMessageModel inputMessage = JSONHelper.getObjectFromJSONPath("message/testInputValueMessage.json", AnalyticsMessageModel.class);
        MessageModel messageModel = new MessageModel();
        messageModel.putMessage("debug", Helper.analyticsToInputMessageModel(inputMessage, "test"));
        Message message = new Message();
        message.addInput("value");
        message.setMessage(messageModel);
        Double value = message.getInput("value").getValue();
        Assert.assertEquals(Double.valueOf(2.0), value);
    }

    @Test
    public void testInputValueDevice() throws NoValueException {
        ConfigProvider.setConfig(new Config(new JSONHelper().parseFile("message/testDeviceInputValueConfig.json").toString()));
        DeviceMessageModel inputMessage = JSONHelper.getObjectFromJSONPath("message/testDeviceInputValueMessage.json", DeviceMessageModel.class);
        MessageModel messageModel = new MessageModel();
        messageModel.putMessage("debug", Helper.deviceToInputMessageModel(inputMessage, "test"));
        Message message = new Message();
        message.addInput("value");
        message.setMessage(messageModel);
        Double value = message.getInput("value").getValue();
        Assert.assertEquals(Double.valueOf(2.0), value);
    }

    @Test
    public void testInputValueDeep() throws NoValueException {
        ConfigProvider.setConfig(new Config(new JSONHelper().parseFile("message/testGetMessageEntityIdConfig.json").toString()));
        DeviceMessageModel inputMessage = JSONHelper.getObjectFromJSONPath("message/testGetMessageEntityIdMessage.json", DeviceMessageModel.class);
        MessageModel messageModel = new MessageModel();
        messageModel.putMessage("debug", Helper.deviceToInputMessageModel(inputMessage, "test"));
        Message message = new Message();
        message.addInput("value");
        message.setMessage(messageModel);
        Double value = message.getInput("value").getValue();
        Assert.assertEquals(Double.valueOf(2.0), value);
    }

    @Test
    public void testTwoMappingsOneInput() throws NoValueException {
        ConfigProvider.setConfig(new Config(new JSONHelper().parseFile("message/twoMappingsOneInput/config.json").toString()));
        List<DeviceMessageModel> inputMessages = JSONHelper.getObjectArrayFromJSONPath("message/twoMappingsOneInput/messages.json", DeviceMessageModel.class);
        MessageModel messageModel = new MessageModel();
        Message message = new Message();
        message.addFlexInput("value");

        messageModel.putMessage("debug", Helper.deviceToInputMessageModel(inputMessages.get(0), "debug"));
        messageModel.putMessage("debug-2", Helper.deviceToInputMessageModel(inputMessages.get(1), "debug-2"));
        message.setMessage(messageModel);
        Double value = message.getFlexInput("value").getValue();
        Assert.assertEquals(Double.valueOf(2.0), value);
        messageModel.putMessage("debug", Helper.deviceToInputMessageModel(inputMessages.get(2), "debug"));
        message.setMessage(messageModel);
        value = message.getFlexInput("value").getValue();
        Assert.assertEquals(Double.valueOf(3.0), value);
    }

    @Test
    public void testOutputValue(){
        Message message = new Message();
        message.output("test", Double.valueOf(2));
        Assert.assertEquals("{\"pipeline_id\":\"debug\",\"operator_id\":\"debug\",\"analytics\":{\"test\":2.0},\"time\":\""+TimeProvider.nowUTCToString()+"\"}", Helper.getFromObject(message.getMessage().getOutputMessage()));
    }

    @Test
    public void testInputFilterId(){
        ConfigProvider.setConfig(new Config(new JSONHelper().parseFile("message/testGetMessageEntityIdConfig.json").toString()));
        DeviceMessageModel inputMessage = JSONHelper.getObjectFromJSONPath("message/testGetMessageEntityIdMessage.json", DeviceMessageModel.class);
        MessageModel messageModel = new MessageModel();
        messageModel.putMessage("debug", Helper.deviceToInputMessageModel(inputMessage, "test"));
        Message message = new Message();
        message.addInput("value");
        message.setMessage(messageModel);
        Assert.assertEquals("134534", message.getInput("value").getFilterId());
    }

    @Test
    public void testFullInput() throws NoValueException {
        ConfigProvider.setConfig(new Config(new JSONHelper().parseFile("message/testFullInputConfig.json").toString()));
        DeviceMessageModel inputMessage = JSONHelper.getObjectFromJSONPath("message/testFullInputMessage.json", DeviceMessageModel.class);
        MessageModel messageModel = new MessageModel();
        messageModel.putMessage("test", Helper.deviceToInputMessageModel(inputMessage, "test"));
        Message message = new Message();
        message.addInput("value");
        message.setMessage(messageModel);
        Object value = message.getInput("value").getValue(Object.class);
        Assert.assertEquals(inputMessage.getValue(), value);
    }
}
