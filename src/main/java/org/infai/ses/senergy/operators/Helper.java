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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.models.DeviceMessageModel;
import org.infai.ses.senergy.models.ImportMessageModel;
import org.infai.ses.senergy.models.InputMessageModel;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Helper {

    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = Logger.getLogger(Helper.class.getName());

    private Helper() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Returns the value of an env var or ,if the env var is not set, the default value.
     *
     * @param envName      the name of the env var
     * @param defaultValue the default return value
     * @return the value of the env var or the default value
     */
    public static String getEnv(String envName, String defaultValue) {
        if (defaultValue.equals("")) {
            defaultValue = null;
        }
        return System.getenv(envName) != null ? System.getenv(envName) : defaultValue;
    }

    public static Integer getEnv(String envName, Integer defaultValue) {
        return System.getenv(envName) != null ? Integer.parseInt(System.getenv(envName)) : defaultValue;
    }

    public static <T> String getFromObject(T object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            log.log(Level.SEVERE, e.getMessage());
            return null;
        }
    }

    public static InputMessageModel analyticsToInputMessageModel(AnalyticsMessageModel input, String topicName) {
        InputMessageModel message = new InputMessageModel();
        message.setTopic(topicName);
        message.setFilterType(InputMessageModel.FilterType.OPERATOR_ID);
        message.setValue(input.getAnalytics());
        message.setFilterIdFirst(input.getOperatorId());
        message.setFilterIdSecond(input.getPipelineId());
        return message;
    }

    public static InputMessageModel deviceToInputMessageModel(DeviceMessageModel input, String topicName) {
        InputMessageModel message = new InputMessageModel();
        message.setTopic(topicName);
        message.setFilterType(InputMessageModel.FilterType.DEVICE_ID);
        message.setValue(input.getValue());
        message.setFilterIdFirst(input.getDeviceId());
        message.setFilterIdSecond(input.getServiceId());
        return message;
    }

    public static InputMessageModel importToInputMessageModel(ImportMessageModel input, String topicName) {
        InputMessageModel message = new InputMessageModel();
        message.setTopic(topicName);
        message.setFilterType(InputMessageModel.FilterType.IMPORT_ID);
        message.setValue(input.getValue());
        message.setFilterIdFirst(input.getImportId());
        return message;
    }
}
