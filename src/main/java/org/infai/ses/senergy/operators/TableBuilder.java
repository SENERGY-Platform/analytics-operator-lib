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

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.json.JSONArray;
import org.json.JSONObject;

public class TableBuilder extends BaseBuilder {

    public TableBuilder(String operatorId, String pipelineId) {
        super(operatorId, pipelineId);
    }

    /**
     * Filter by device id.
     *
     * @param inputStream KTable
     * @param valuePath String
     * @param filterValues String []
     * @return KStream filterData
     */
    public KTable<String, String> filterBy(KTable<String, String> inputStream, String valuePath, String [] filterValues) {
        KTable<String, String> filterData = inputStream.filter((key, json) -> Helper.filterId(valuePath, filterValues, json));
        KStream<String, String> filterDataStream = filterData.toStream().filter((key, value) -> value != null);
        return filterDataStream.toTable();
    }

    public KTable<String, String> joinMultipleStreams(KTable[] streams) {
        KTable<String, String> joinedStream = streams[0];
        for(int i = 1; i < streams.length; i++) {
            if(i == streams.length - 1) {
                joinedStream = joinedStream.join(streams[i], this::joinStreams);
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
