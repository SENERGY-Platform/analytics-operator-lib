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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class StreamBuilder extends BaseBuilder {

    private StreamsBuilder builder = new StreamsBuilder();
    private Integer seconds = Values.WINDOW_TIME;

    public StreamBuilder(String operatorId, String pipelineId) {
        super(operatorId, pipelineId);
    }

    /**
     * Filter by device id.
     *
     * @param inputStream
     * @param valuePath
     * @param filterValues
     * @return KStream filterData
     */
    public KStream<String, String> filterBy(KStream<String, String> inputStream, String valuePath, String [] filterValues) {

        KStream<String, String> filterData = inputStream.filter((key, json) -> {
            if (valuePath != null) {
                if (Helper.checkPathExists(json, "$." + valuePath)) {
                    String value = JsonPath.parse(json).read("$." + valuePath);
                    //if the ids do not match, filter the element
                    try {
                        return Arrays.asList(filterValues).contains(value);
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

    public KStream<String, String> joinMultipleStreams(KStream[] streams) {
        return joinMultipleStreams(streams, seconds);
    }

    public KStream<String, String> joinMultipleStreams(KStream[] streams, int seconds) {
        KStream<String, String> joinedStream = streams[0];
        for(int i = 1; i < streams.length; i++) {
            if(i == streams.length - 1) {
                joinedStream = joinedStream.join(streams[i], (leftValue, rightValue) -> {
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
                    return this.formatMessage(values).toString();
                        }, JoinWindows.of(Duration.ofSeconds(seconds)), StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                );
            }
            else {
                joinedStream = joinedStream.join(streams[i], (leftValue, rightValue) -> {
                    if (!leftValue.startsWith("[")){
                        leftValue = "[" + leftValue + "]";
                    }

                    return new JSONArray(leftValue).put(new JSONObject(rightValue)).toString();
                    },
                    JoinWindows.of(Duration.ofSeconds(seconds)), StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                );
            }
        }

        return joinedStream;
    }

    public StreamsBuilder getBuilder() {
        return this.builder;
    }

    public void setWindowTime(Integer seconds){
        this.seconds = seconds;
    }

}