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

public class DeviceMessageModel {

    @JsonProperty("device_id")
    private String deviceId;
    @JsonProperty("service_id")
    private String serviceId;
    @JsonProperty("value")
    private Map<String, Object> value;

    @JsonProperty("device_id")
    public String getDeviceId() {
        return deviceId;
    }

    @JsonProperty("device_id")
    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    @JsonProperty("service_id")
    public String getOperatorId() {
        return serviceId;
    }

    @JsonProperty("service_id")
    public void setOperatorId(String serviceId) {
        this.serviceId = serviceId;
    }

    @JsonProperty("value")
    public Map<String, Object> getValue() {
        return value;
    }

    @JsonProperty("value")
    public void setValue(Map<String, Object> value) {
        this.value = value;
    }
}
