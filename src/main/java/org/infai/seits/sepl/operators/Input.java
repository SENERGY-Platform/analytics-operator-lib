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
            return new Double(this.getVal());
        } catch (NullPointerException e){
            return new Double(0);
        }
    }

    public String getString(){
        try {
            return new String(this.getVal());
        } catch (NullPointerException e){
            return "";
        }
    }

    private String getVal(){
        String filterType = (String) this.config.get("FilterType");
        String filterValue = (String) this.config.get("FilterValue");
        String value = null;
        if (filterType.equals("OperatorId")) {
            List<Object> helper = JsonPath.read(this.messageString, "$.inputs[?(@.operator_id == '" + filterValue + "')].analytics." + this.config.get("Source"));
            value = convertToString(helper.get(0));
        } else if (filterType.equals("DeviceId")) {
            List<Object> helper = JsonPath.read(this.messageString, "$.inputs[?(@.device_id == '" + filterValue + "')]." + this.config.get("Source"));
            value = convertToString(helper.get(0));
        }
        return value;
    }

    private String convertToString(Object ret) {
        if (ret instanceof Double) {
            return ret.toString();
        } else if (ret instanceof Integer) {
            return String.valueOf(ret);
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
