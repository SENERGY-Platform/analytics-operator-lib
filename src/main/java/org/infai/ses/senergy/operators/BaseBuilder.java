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

import org.apache.kafka.streams.StreamsBuilder;
import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.models.DeviceMessageModel;
import org.infai.ses.senergy.utils.TimeProvider;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BaseBuilder {

    protected StreamsBuilder builder = new StreamsBuilder();
    private static final Logger log = Logger.getLogger(BaseBuilder.class.getName());

    private JSONObject createMessageWrapper(){
        return new JSONObject().
                put("pipeline_id", Values.PIPELINE_ID).
                put("time", TimeProvider.nowUTCToString()).
                put("operator_id", Values.OPERATOR_ID).
                put("analytics", new JSONObject());
    }

    public String formatMessage (String value) {
        List<String> values = Collections.singletonList(value);
        return createMessage(values).toString();
    }

    public JSONObject createMessage(List<String> values){
        JSONObject ob = createMessageWrapper();
        JSONArray inputs = new JSONArray();
        values.forEach(v -> {
            inputs.put(new JSONObject(v));
        });
        ob.put("inputs", inputs);
        return ob;
    }

    public StreamsBuilder getBuilder() {
        return this.builder;
    }

    protected String joinLastStreams(String leftValue, String rightValue) {
        List<String> values = new LinkedList<>();

        if(leftValue.startsWith("[")) {
            JSONArray array = new JSONArray(leftValue);
            for (int j=0; j<array.length(); j++) {
                values.add(array.getJSONObject(j).toString());
            }
        }else{
            values.add(leftValue);
        }
        values.add(rightValue);
        return this.createMessage(values).toString();
    }

    protected static <T>boolean filterId(String[] filterValues, T message) {
        if (filterValues.length > 0) {
            if (message instanceof AnalyticsMessageModel){
                try {
                    return Values.PIPELINE_ID.equals(((AnalyticsMessageModel) message).getPipelineId()) &&
                            Arrays.asList(filterValues).contains(((AnalyticsMessageModel) message).getOperatorId());
                } catch (NullPointerException e) {
                    log.log(Level.SEVERE, "No Filter ID was set to be filtered");
                }
            } else if (message instanceof DeviceMessageModel){
                try {
                    return Arrays.asList(filterValues).contains(((DeviceMessageModel) message).getDeviceId());
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
