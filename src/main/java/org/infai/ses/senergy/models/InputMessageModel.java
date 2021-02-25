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

package org.infai.ses.senergy.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class InputMessageModel {

    public enum FilterType
    {
        OPERATOR_ID, DEVICE_ID, IMPORT_ID
    }

    @JsonProperty("filterIdFirst")
    private String filterIdFirst;
    @JsonProperty("filterIdSecond")
    private String filterIdSecond;
    @JsonProperty("filterType")
    private FilterType filterType;
    @JsonProperty("value")
    private Map<String, Object> value;
    @JsonProperty("topic")
    private String topic;

    @JsonProperty("filterIdFirst")
    public String getFilterIdFirst() {
        return filterIdFirst;
    }

    @JsonProperty("filterIdFirst")
    public void setFilterIdFirst(String filterIdFirst) {
        this.filterIdFirst = filterIdFirst;
    }

    @JsonProperty("filterIdSecond")
    public String getFilterIdSecond() {
        return filterIdSecond;
    }

    @JsonProperty("filterIdSecond")
    public void setFilterIdSecond(String filterIdSecond) {
        this.filterIdSecond = filterIdSecond;
    }

    @JsonProperty("filterType")
    public FilterType getFilterType() {
        return filterType;
    }

    @JsonProperty("filterType")
    public void setFilterType(FilterType filterType) {
        this.filterType = filterType;
    }

    @JsonProperty("value")
    public Map<String, Object> getValue() {
        return value;
    }

    @JsonProperty("value")
    public void setValue(Map<String, Object> value) {
        this.value = value;
    }

    @JsonProperty("topic")
    public String getTopic() {
        return topic;
    }

    @JsonProperty("topic")
    public void setTopic(String topic) {
        this.topic = topic;
    }
}
