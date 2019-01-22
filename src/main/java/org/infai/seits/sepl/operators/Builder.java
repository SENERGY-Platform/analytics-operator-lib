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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.infai.seits.sepl.operators.Helper.checkPathExists;

public class Builder {

    private StreamsBuilder builder = new StreamsBuilder();
    private Integer seconds = 5;
    private String pipelineId;
    private String operatorId;

    public String time;

    public Builder (String operatorId, String pipelineId){
        this.operatorId = operatorId;
        this.pipelineId = pipelineId;
    }

    /**
     * Filter by device id.
     *
     * @param inputStream
     * @param valuePath
     * @param filterValue
     * @return KStream filterData
     */
    public KStream<String, String> filterBy(KStream<String, String> inputStream, String valuePath, String filterValue) {

        KStream<String, String> filterData = inputStream.filter((key, json) -> {
            if (valuePath != null) {
                if (checkPathExists(json, "$." + valuePath)) {
                    String value = JsonPath.parse(json).read("$." + valuePath);
                    //if the ids do not match, filter the element
                    try {
                        return filterValue.equals(value);
                    } catch (NullPointerException e) {
                        System.out.println("No Device ID was set to be filtered");
                    }
                }
                //if the path does not exist, the element is filtered
                return false;
            }
            // if no path is given, everything is processed
            return true;
        });
        return filterData;
    }

    public KStream<String, String> joinStreams(KStream<String, String> stream1, KStream<String, String> stream2){
        return joinStreams(stream1, stream2, seconds);
    }

    public KStream<String, String> joinStreams(KStream<String, String> stream1, KStream<String, String> stream2, Integer seconds) {
        KStream<String, String> joinedStream = stream1.join(stream2, (leftValue, rightValue) -> {
            List <String> values = Arrays.asList(leftValue,rightValue);
            return formatMessage(values).toString();
            },JoinWindows.of(TimeUnit.SECONDS.toMillis(seconds)), Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

        return joinedStream;
    }

    public StreamsBuilder getBuilder() {
        return builder;
    }

    private void setTime(){
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        this.time =timestamp.toInstant().toString();
    }

    private JSONObject createMessageWrapper(){
        setTime();
        return new JSONObject().
                put("pipeline_id", pipelineId).
                put("time", this.time).
                put("operator_id", operatorId).
                put("analytics", new JSONObject());
    }

    public String formatMessage (String value) {
        List <String> values = Arrays.asList(value);
        return formatMessage(values).toString();
    }

    public JSONObject formatMessage(List<String> values){
        JSONObject ob = createMessageWrapper();
        JSONArray inputs = new JSONArray();
        values.forEach((v) -> inputs.put(new JSONObject(v)));
        ob.put("inputs", inputs);
        return ob;
    }

    public void setWindowTime(Integer seconds){
        this.seconds = seconds;
    }

}
