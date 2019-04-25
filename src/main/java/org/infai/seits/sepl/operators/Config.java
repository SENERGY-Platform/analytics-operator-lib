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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {

    private String configString = Helper.getEnv("CONFIG", "[]");


    public Config(){
        streamlineConfigString();
    }

    public Config (String configString){
        this.configString = configString;
        streamlineConfigString();
    }

    public JSONArray getTopicConfig(){
        net.minidev.json.JSONArray array = JsonPath.read(configString, "$."+Values.INPUT_TOPICS+"[*]");
        return new JSONArray(array.toString());
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
        try {
            return JsonPath.read(this.configString, "$."+Values.INPUT_TOPICS+"["+ index+"]."+Values.TOPIC_NAME_KEY);
        } catch (PathNotFoundException e) {
            System.out.println(e.getMessage());
            return "";
        }
    }

    public Map<String, Object> inputTopic(String inputName){
        Map<String, Object> topic = new HashMap<String, Object>();
        List<Map<String, Object>> topics = JsonPath.read(this.configString,"$."+Values.INPUT_TOPICS+".*");
        for(Map<String, Object> t : topics){
            List<Map<String, Object>> mappings;
                mappings = (List<Map<String, Object>>) t.get(Values.MAPPINGS_KEY);
            for (Map<String, Object> m : mappings){
                if (m.get(Values.MAPPING_DEST_KEY).equals(inputName)){
                    t.put(Values.MAPPING_SOURCE_KEY, m.get(Values.MAPPING_SOURCE_KEY));
                    topic = t;
                }
            }
        }
        return  topic;
    }

    private void streamlineConfigString(){
        this.configString = this.configString.replaceAll("(?i)\""+Values.TOPIC_NAME_KEY+"\"", '"'+Values.TOPIC_NAME_KEY+'"');
        this.configString = this.configString.replaceAll("(?i)\""+Values.MAPPINGS_KEY+"\"", '"'+Values.MAPPINGS_KEY+'"');
        this.configString = this.configString.replaceAll("(?i)\""+Values.MAPPING_DEST_KEY+"\"", '"'+Values.MAPPING_DEST_KEY+'"');
        this.configString = this.configString.replaceAll("(?i)\""+Values.MAPPING_SOURCE_KEY+"\"", '"'+Values.MAPPING_SOURCE_KEY+'"');
        this.configString = this.configString.replaceAll("(?i)\""+Values.FILTER_TYPE_KEY+"\"", '"'+Values.FILTER_TYPE_KEY+'"');
        this.configString = this.configString.replaceAll("(?i)\""+Values.FILTER_VALUE_KEY+"\"", '"'+Values.FILTER_VALUE_KEY+'"');
        this.configString = this.configString.replaceAll("(?i)\""+Values.FILTER_TYPE_OPERATOR_KEY+"\"", '"'+Values.FILTER_TYPE_OPERATOR_KEY+'"');
        this.configString = this.configString.replaceAll("(?i)\""+Values.FILTER_TYPE_DEVICE_KEY+"\"", '"'+Values.FILTER_TYPE_DEVICE_KEY+'"');
        this.configString = this.configString.replaceAll("(?i)\""+Values.INPUT_TOPICS+"\"", '"'+Values.INPUT_TOPICS+'"');
    }
}


