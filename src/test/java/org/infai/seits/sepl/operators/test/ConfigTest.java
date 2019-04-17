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

import org.infai.seits.sepl.operators.Config;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ConfigTest {

    @Test
    public void testGetTopicConfig(){
        Config config = new Config("{\"inputTopics\":[{\"Name\":\"analytics-diff\",\"FilterType\":\"OperatorId\"," +
                "\"FilterValue\":\"6\",\"Mappings\":[{\"Dest\":\"value\",\"Source\":\"diff\"}]}]}");
        JSONArray array =  config.getTopicConfig();
        Assert.assertEquals("[{\"mappings\":[{\"source\":\"diff\",\"dest\":\"value\"}],\"name\":\"analytics-diff\",\"filtervalue\":\"6\",\"filtertype\":\"operatorid\"}]",
                array.toString());
    }

    @Test
    public void testTopicOfInput(){
        Config config = new Config("{\"inputTopics\":[{\"Name\":\"analytics-diff\",\"FilterType\":\"OperatorId\"," +
                "\"FilterValue\":\"6\",\"Mappings\":[{\"Dest\":\"value\",\"Source\":\"diff\"}]}]}");
        Map<String, Object> conf =  config.inputTopic("value");
        Assert.assertEquals("analytics-diff", conf.get("name"));
    }

    @Test
    public void testTopicOfInputSmall(){
        Config config = new Config("{\"inputTopics\":[{\"name\":\"analytics-diff\",\"filterType\":\"OperatorId\"," +
                "\"filterValue\":\"6\",\"mappings\":[{\"dest\":\"value\",\"source\":\"diff\"}]}]}");
        Map<String, Object> conf =  config.inputTopic("value");
        Assert.assertEquals("analytics-diff", conf.get("name"));
    }

    @Test
    public void testGetTopicName(){
        Config config = new Config("{\"inputTopics\":[{\"Name\":\"analytics-diff\",\"FilterType\":\"OperatorId\"," +
                "\"FilterValue\":\"6\",\"Mappings\":[{\"Dest\":\"value\",\"Source\":\"diff\"}]}]}");
        Assert.assertEquals("analytics-diff",config.getTopicName(0));
    }

    @Test
    public void testGetTopicNameSmall(){
        Config config = new Config("{\"inputTopics\":[{\"name\":\"analytics-diff\",\"FilterType\":\"OperatorId\"," +
                "\"FilterValue\":\"6\",\"Mappings\":[{\"Dest\":\"value\",\"Source\":\"diff\"}]}]}");
        Assert.assertEquals("analytics-diff",config.getTopicName(0));
    }

    @Test
    public void testGetConfigValue(){
        Config config = new Config("{\"config\": {\"test\": \"testValue\"},\"inputTopics\":[{\"Name\":\"analytics-diff\",\"FilterType\":\"OperatorId\"," +
                "\"FilterValue\":\"6\",\"Mappings\":[{\"Dest\":\"value\",\"Source\":\"diff\"}]}]}");
        Assert.assertEquals("testvalue",config.getConfigValue("test", "test1"));
    }
}
