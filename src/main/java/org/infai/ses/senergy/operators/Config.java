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
import org.infai.ses.senergy.models.ConfigModel;
import org.infai.ses.senergy.models.InputTopicModel;
import org.infai.ses.senergy.models.MappingModel;
import java.util.*;

public class Config {

    private String configString = Helper.getEnv("CONFIG", "{}");
    private ConfigModel configModel;

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

    /**
     * Get the input topic from a given destination.
     *
     * @param destination String
     * @return InputTopicModel
     */
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

    /**
     * Get the input topics from a given destination.
     *
     * @param destination String
     * @return InputTopicModel
     */
    public List<InputTopicModel> getInputTopicsByDestination(String destination) {
        List<InputTopicModel> topics = new LinkedList<>();
        for (InputTopicModel topic : this.configModel.getInputTopics()){
            for (MappingModel mappingModel : topic.getMappings()){
                if (mappingModel.getDest().equals(destination)){
                    List<MappingModel> mappings = new LinkedList<>();
                    InputTopicModel newTopicModel = new InputTopicModel(topic);
                    mappings.add(mappingModel);
                    newTopicModel.setMappings(mappings);
                    topics.add(newTopicModel);
                }
            }
        }
        return topics;
    }

    public Integer topicCount(){
        return this.getInputTopicsConfigs().size();
    }

    /**
     * Returns a config value.
     *
     * @param value String
     * @param defaultValue String
     * @return String
     */
    public String getConfigValue (String value, String defaultValue) {
        if (this.configModel.getOperatorConfig().get(value) != null && !this.configModel.getOperatorConfig().get(value).equals("")){
            return this.configModel.getOperatorConfig().get(value);
        } else {
            return defaultValue;
        }
    }


    /**
     * Returns the the input topic configuration which corresponds to the dest name given.
     *
     * @param inputName String
     * @return InputTopicModel
     */
    public InputTopicModel getInputTopicByInputName(String inputName){
        return this.getInputTopicByDestination(inputName);
    }

    private void streamlineConfigString(){
        final String PRE = "(?i)\"";
        this.configString = this.configString.replaceAll(PRE + Values.TOPIC_NAME_KEY+"\"", '"'+Values.TOPIC_NAME_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.MAPPINGS_KEY+"\"", '"'+Values.MAPPINGS_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.MAPPING_DEST_KEY+"\"", '"'+Values.MAPPING_DEST_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.MAPPING_SOURCE_KEY+"\"", '"'+Values.MAPPING_SOURCE_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.FILTER_TYPE_KEY+"\"", '"'+Values.FILTER_TYPE_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.FILTER_VALUE_KEY+"\"", '"'+Values.FILTER_VALUE_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.FILTER_VALUE_2_KEY+"\"", '"'+Values.FILTER_VALUE_2_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.FILTER_TYPE_OPERATOR_KEY+"\"", '"'+Values.FILTER_TYPE_OPERATOR_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.FILTER_TYPE_DEVICE_KEY+"\"", '"'+Values.FILTER_TYPE_DEVICE_KEY+'"');
        this.configString = this.configString.replaceAll(PRE +Values.INPUT_TOPICS+"\"", '"'+Values.INPUT_TOPICS+'"');
    }
}


