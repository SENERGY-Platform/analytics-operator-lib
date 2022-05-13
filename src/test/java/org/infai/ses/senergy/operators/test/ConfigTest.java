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

import org.infai.ses.senergy.models.InputTopicModel;
import org.infai.ses.senergy.operators.Config;
import org.junit.Assert;
import org.junit.Test;
import org.infai.ses.senergy.testing.utils.JSONHelper;

import java.util.List;

import static org.assertj.core.api.Assertions.*;


public class ConfigTest {

    Config config = new Config(new JSONHelper().parseFile("config/config.json").toString());

    @Test
    public void testGetTopicConfig(){
        List<InputTopicModel> conf =  config.getInputTopicsConfigs();
        InputTopicModel expected = JSONHelper.getObjectFromJSONString("{\"mappings\":[{\"source\":\"value.temperature.level\",\"dest\":\"value\"}],\"filterValue\":\"filterValue\",\"name\":\"test\",\"filterType\":\"DeviceId\"}", InputTopicModel.class);
        assertThat(expected.getFilterType()).isEqualTo(conf.get(0).getFilterType());
        assertThat(expected.getFilterValue()).isEqualTo(conf.get(0).getFilterValue());
    }

    @Test
    public void testTopicOfInput(){
        InputTopicModel conf =  config.getInputTopicByInputName("value");
        Assert.assertEquals("test", conf.getName());
    }

    @Test
    public void testTopicOfInputSmall(){
        InputTopicModel conf =  config.getInputTopicByInputName("value");
        Assert.assertEquals("test", conf.getName());
    }

    @Test
    public void testGetTopicName(){
        Assert.assertEquals("test",config.getInputTopicsConfigs().get(0).getName());
    }

    @Test
    public void testGetTopicNameSmall(){
        Assert.assertEquals("test",config.getInputTopicsConfigs().get(0).getName());
    }

    @Test
    public void testGetConfigValue(){
        Assert.assertEquals("test1",config.getConfigValue("test", "test1"));
    }

    @Test
    public void testGetUserId(){ Assert.assertEquals("not set", config.getUserId());}
}
