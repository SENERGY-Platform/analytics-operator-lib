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

import com.jayway.jsonpath.JsonPath;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.infai.ses.senergy.utils.MessageProvider;

import java.util.List;
import java.util.Map;

public class Input {

    private String messageString = MessageProvider.getMessage().getMessageString();
    private final Config config = ConfigProvider.getConfig();
    private final Map<String, Object> topicConfig;

    public Input(String name) {
        this.topicConfig = this.config.getInputTopicByInputName(name);
    }

    public Input setMessage(String message) {
        this.messageString = message;
        return this;
    }

    public Double getValue() {
        try {
            return Double.valueOf(this.getVal());
        } catch (NullPointerException e){
            return null;
        }
    }

    public String getString(){
        try {
            return new String(this.getVal());
        } catch (NullPointerException e){
            return "";
        }
    }

    public org.json.JSONArray getJSONArray(){
        try {
            return new org.json.JSONArray(this.getVal());
        } catch (NullPointerException e){
            return new org.json.JSONArray();
        }
    }

    private String getVal(){
        String filterType = (String) this.topicConfig.get(Values.FILTER_TYPE_KEY);
        String filterValueString = (String) this.topicConfig.get(Values.FILTER_VALUE_KEY);
        String[] filterValues = filterValueString.split(",");
        String value = null;
        List<String> operatorIds = JsonPath.read(this.messageString, "$.inputs[*].operator_id");
        List<String> deviceIds = JsonPath.read(this.messageString, "$.inputs[*].device_id");

        for (String filterValue : filterValues) {
            if (filterType.equals(Values.FILTER_TYPE_OPERATOR_KEY) && operatorIds.contains(filterValue)) {
                List<Object> helper = JsonPath.read(this.messageString, "$.inputs[?(@.operator_id == '" + filterValue + "')].analytics." + this.topicConfig.get(Values.MAPPING_SOURCE_KEY));
                value = convertToString(helper.get(0));
            } else
            if (filterType.equals(Values.FILTER_TYPE_DEVICE_KEY) && deviceIds.contains(filterValue)) {
                List<Object> helper = JsonPath.read(this.messageString, "$.inputs[?(@.device_id == '" + filterValue + "')]." + this.topicConfig.get(Values.MAPPING_SOURCE_KEY));
                value = convertToString(helper.get(0));
            }
        }
        return value;
    }

    private String convertToString(Object ret) {
        if (ret instanceof Double) {
            return ret.toString();
        } else if (ret instanceof Integer) {
            return String.valueOf(ret);
        } else if (ret instanceof net.minidev.json.JSONArray){
            return ret.toString();
        } else {
            try {
                return (String) ret;
            } catch (ClassCastException e){
                System.err.println("Error converting input value: " + e.getMessage());
                return null;
            }
        }
    }
}
