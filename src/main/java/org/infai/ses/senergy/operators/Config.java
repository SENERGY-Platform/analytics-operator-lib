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

package org.infai.ses.senergy.operators;

import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.infai.ses.senergy.models.ConfigModel;
import org.infai.ses.senergy.models.InputTopicModel;
import org.infai.ses.senergy.models.MappingModel;
import org.json.JSONArray;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Config {

    private String configString = Helper.getEnv("CONFIG", "{}");
    private ConfigModel configModel;
    private static final Logger log = Logger.getLogger(Config.class.getName());

    public Config(){
        streamlineConfigString();
        this.configModel = new Gson().fromJson(this.configString, ConfigModel.class);
    }

    public Config (String configString){
        this.configString = configString;
        streamlineConfigString();
        this.configModel = new Gson().fromJson(this.configString, ConfigModel.class);
    }

    public String getConfigString() {
        return configString;
    }

    /**
     * @Deprecated (uses wrong JSON lib)
     *
     * @return JSONArray
     */
    @Deprecated
    public JSONArray getTopicConfig(){
        net.minidev.json.JSONArray array = JsonPath.read(configString, "$."+Values.INPUT_TOPICS+"[*]");
        return new JSONArray(array.toString());
    }

    /**
     * Returns a list of inputtopic models.
     *
     * @return list of inputtopic models
     */
    public List<InputTopicModel> getInputTopicsConfigs() {
        return this.configModel.getInputTopics();
    }

    public InputTopicModel getInputTopicByName(String name) {
        for (InputTopicModel topic : this.configModel.getInputTopics()){
            if (topic.getName().equals(name)){
                return topic;
            }
        }
        return null;
    }

    public InputTopicModel getInputTopicByDestination(String destination) {
        for (InputTopicModel topic : this.configModel.getInputTopics()){
            for (MappingModel mappingModel : topic.getMappings()){
                if (mappingModel.getDest().equals(destination)){
                    return topic;
                }
            }
        }
        return null;
    }

    public JSONArray getTopicConfigById(Integer index){
        try {
            net.minidev.json.JSONArray array = JsonPath.read(this.configString, "$.."+Values.INPUT_TOPICS+"["+ index+"]");
            return new JSONArray(array.toString());
        } catch (PathNotFoundException e) {
            log.log(Level.SEVERE, e.getMessage());
            return new JSONArray();
        }
    }

    public Integer topicCount(){
        return getTopicConfig().length();
    }

    public String getConfigValue (String value, String defaultValue) {
        String rvalue = "";
        try {
            rvalue = JsonPath.read(configString, "$.config."+value);
        } catch (PathNotFoundException e) {
            return defaultValue;
        }
        if (rvalue.length() == 0) {
            return defaultValue;
        }
        return rvalue;
    }

    public String getTopicName(Integer index){
        try {
            return JsonPath.read(this.configString, "$."+Values.INPUT_TOPICS+"["+ index+"]."+Values.TOPIC_NAME_KEY);
        } catch (PathNotFoundException e) {
            log.log(Level.SEVERE, e.getMessage());
            return "";
        }
    }

    /**
     * Returns the the input topic configuration which corresponds to the dest name given.
     *
     * @param inputName
     * @return
     */
    public Map<String, Object> getInputTopicByInputName(String inputName){
        Map<String, Object> topic = new HashMap<>();
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
        final String PRE = "(?i)\"";
        this.configString = this.configString.replaceAll(PRE + Values.TOPIC_NAME_KEY+"\"", '"'+Values.TOPIC_NAME_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.MAPPINGS_KEY+"\"", '"'+Values.MAPPINGS_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.MAPPING_DEST_KEY+"\"", '"'+Values.MAPPING_DEST_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.MAPPING_SOURCE_KEY+"\"", '"'+Values.MAPPING_SOURCE_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.FILTER_TYPE_KEY+"\"", '"'+Values.FILTER_TYPE_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.FILTER_VALUE_KEY+"\"", '"'+Values.FILTER_VALUE_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.FILTER_TYPE_OPERATOR_KEY+"\"", '"'+Values.FILTER_TYPE_OPERATOR_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.FILTER_TYPE_DEVICE_KEY+"\"", '"'+Values.FILTER_TYPE_DEVICE_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.INPUT_TOPICS+"\"", '"'+Values.INPUT_TOPICS+'"');
    }
}


