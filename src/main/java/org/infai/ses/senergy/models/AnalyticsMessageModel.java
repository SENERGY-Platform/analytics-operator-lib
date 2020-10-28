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
import org.infai.ses.senergy.utils.TimeProvider;

import java.util.Map;

public class AnalyticsMessageModel {

    @JsonProperty("pipeline_id")
    private String pipelineId;

    @JsonProperty("operator_id")
    private String operatorId;

    @JsonProperty("analytics")
    private Map<String, Object> analytics;

    @JsonProperty("time")
    private String time = TimeProvider.nowUTCToString();

    @JsonProperty("pipeline_id")
    public String getPipelineId() {
        return pipelineId;
    }

    @JsonProperty("pipeline_id")
    public void setPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
    }

    @JsonProperty("operator_id")
    public String getOperatorId() {
        return operatorId;
    }

    @JsonProperty("operator_id")
    public void setOperatorId(String operatorId) {
        this.operatorId = operatorId;
    }

    @JsonProperty("analytics")
    public Map<String, Object> getAnalytics() {
        return analytics;
    }

    @JsonProperty("analytics")
    public void setAnalytics(Map<String, Object> analytics) {
        this.analytics = analytics;
    }

}
