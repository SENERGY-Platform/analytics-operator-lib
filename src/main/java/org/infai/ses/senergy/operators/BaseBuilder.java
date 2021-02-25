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

import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.models.DeviceMessageModel;
import org.infai.ses.senergy.models.ImportMessageModel;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BaseBuilder {

    private static final Logger log = Logger.getLogger(BaseBuilder.class.getName());

    private BaseBuilder() {
        throw new IllegalStateException("Utility class");
    }

    protected static <T>boolean filterId(String[] filterValues, String[]filterValues2, T message) {
        if (filterValues.length > 0) {
            if (message instanceof AnalyticsMessageModel){
                try {
                    return (Arrays.asList(filterValues2).contains(((AnalyticsMessageModel) message).getPipelineId()) &&
                            Arrays.asList(filterValues).contains(((AnalyticsMessageModel) message).getOperatorId())) ||
                            Arrays.asList(filterValues).contains(((AnalyticsMessageModel) message).getOperatorId() +":"+
                                    ((AnalyticsMessageModel) message).getPipelineId());
                } catch (NullPointerException e) {
                    log.log(Level.SEVERE, "No Filter ID was set to be filtered");
                }
            } else if (message instanceof DeviceMessageModel){
                try {
                    return Arrays.asList(filterValues).contains(((DeviceMessageModel) message).getDeviceId());
                } catch (NullPointerException e) {
                    log.log(Level.SEVERE, "No Filter ID was set to be filtered");
                }
            } else if (message instanceof ImportMessageModel){
                try {
                    return Arrays.asList(filterValues).contains(((ImportMessageModel) message).getImportId());
                } catch (NullPointerException e) {
                    log.log(Level.SEVERE, "No Filter ID was set to be filtered");
                }
            } else {
                log.log(Level.SEVERE, "Wrong Message model");
                return false;
            }
            return false;
        }
        return true;
    }
}
