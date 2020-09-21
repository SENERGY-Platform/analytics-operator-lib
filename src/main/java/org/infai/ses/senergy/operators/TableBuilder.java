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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class TableBuilder extends BaseBuilder {

    public TableBuilder(String operatorId, String pipelineId) {
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
    public KTable<String, String> filterBy(KTable<String, String> inputStream, String valuePath, String [] filterValues) {
        //TODO: Tombstones herausfiltern
        KTable<String, String> filterData = inputStream.filter((key, json) -> {
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
                //return false;
            }
            // if no path is given, everything is processed
            return true;
        });
        KStream<String, String> filterDataStream = filterData.toStream().filter((key, value) -> {
            if (value == null) {
                return false;
            }
            return true;
        });
        return filterDataStream.toTable();
    }

    public KTable<String, String> joinMultipleStreams(KTable[] streams) {
        KTable<String, String> joinedStream = streams[0];
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
                            values.add((String) rightValue);
                            return this.formatMessage(values).toString();
                        }
                );
            }
            else {
                joinedStream = joinedStream.join(streams[i], (leftValue, rightValue) -> {
                            if (!leftValue.startsWith("[")){
                                leftValue = "[" + leftValue + "]";
                            }

                            return new JSONArray(leftValue).put(new JSONObject(rightValue)).toString();
                        }
                );
            }
        }

        return joinedStream;
    }
}
