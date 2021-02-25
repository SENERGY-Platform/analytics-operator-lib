/*
 * Copyright 2021 InfAI (CC SES)
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

public class ImportMessageModel {

    @JsonProperty("import_id")
    private String importId;

    @JsonProperty("value")
    private Map<String, Object> value;

    @JsonProperty("time")
    private String time;

    @JsonProperty("import_id")
    public String getImportId() {
        return importId;
    }

    @JsonProperty("value")
    public Map<String, Object> getValue() {
        if (value != null && time != null)
            value.put("time", time);
        return value;
    }

    @JsonProperty("time")
    public String getTime() {
        return time;
    }
}
