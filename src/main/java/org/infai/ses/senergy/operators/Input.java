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

import java.util.List;
import java.util.Map;

public class Input {

    private String name = "";
    private String messageString = "";
    private Map<String, Object> config;

    public Input(String name, String messageString, Map<String, Object> config) {
        this.messageString = messageString;
        this.name = name;
        this.config = config;
    }

    public Input setMessage(String message) {
        this.messageString = message;
        return this;
    }

    public Double getValue() {
        try {
            return Double.valueOf(this.getVal());
        } catch (NullPointerException e){
            return Double.valueOf(0);
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
        String filterType = (String) this.config.get(Values.FILTER_TYPE_KEY);
        String filterValueString = (String) this.config.get(Values.FILTER_VALUE_KEY);
        String[] filterValues = filterValueString.split(",");
        String value = null;
        for ( String filterValue: filterValues) {
            if (filterType.equals(Values.FILTER_TYPE_OPERATOR_KEY) && filterValue.equals (JsonPath.read(this.messageString, "$.inputs[0].operator_id"))) {
                List<Object> helper = JsonPath.read(this.messageString, "$.inputs[?(@.operator_id == '" + filterValue + "')].analytics." + this.config.get(Values.MAPPING_SOURCE_KEY));
                value = convertToString(helper.get(0));
            } else if (filterType.equals(Values.FILTER_TYPE_DEVICE_KEY) && filterValue.equals (JsonPath.read(this.messageString, "$.inputs[0].device_id"))) {
                List<Object> helper = JsonPath.read(this.messageString, "$.inputs[?(@.device_id == '" + filterValue + "')]." + this.config.get(Values.MAPPING_SOURCE_KEY));
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
            return ((net.minidev.json.JSONArray) ret).toString();
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
