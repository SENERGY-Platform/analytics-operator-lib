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

package org.infai.seits.sepl.operators;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {

    private String configString = Helper.getEnv("CONFIG", "[]");


    public Config(){}

    public Config (String configString){
        this.configString = configString;
    }

    public JSONArray getTopicConfig(){
        String array = JsonPath.read(configString, "$.inputTopics[*]");
        return new JSONArray(array);
    }

    public Integer topicCount(){
        return getTopicConfig().length();
    }

    public String getConfigValue (String value, String defaultValue) {
        try {
            return JsonPath.read(configString, "$.config."+value);
        } catch (PathNotFoundException e) {
            return defaultValue;
        }
    }

    public String getTopicName(Integer index){
        return JsonPath.read(this.configString, "$.inputTopics["+ index+"].Name");
    }

    public Map<String, Object> inputTopic(String inputName){
        Map<String, Object> topic = new HashMap<String, Object>();
        List<Map<String, Object>> topics = JsonPath.read(this.configString,"$.inputTopics.*");
        for(Map<String, Object> t : topics){
            List<Map<String, Object>> mappings = (List<Map<String, Object>>) t.get("Mappings");
            for (Map<String, Object> m : mappings){
                if (m.get("Dest").equals(inputName)){
                    t.put("Source", m.get("Source"));
                    topic = t;
                }
            }
        }
        return  topic;
    }
}
