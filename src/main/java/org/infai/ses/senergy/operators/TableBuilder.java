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

import org.apache.kafka.streams.kstream.KTable;
import org.infai.ses.senergy.models.InputMessageModel;
import org.infai.ses.senergy.models.MessageModel;
import java.util.List;

public class TableBuilder {

    private TableBuilder() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Filter by filter values.
     *
     * @param inputStream KTable<String, T>
     * @param filterValues String []
     * @return KStream filterData
     */
    public static <T>KTable<String, T> filterBy(KTable<String, T> inputStream, String [] filterValues, String [] filterValues2) {
        KTable<String, T> filterData = inputStream.filter((key, value) -> BaseBuilder.filterId(filterValues, filterValues2, value));
        filterData = filterNullValues(filterData);
        return filterData;
    }

    public static <T>KTable<String, T> filterNullValues (KTable<String, T> inputStream){
        return inputStream.toStream().filter((key, value) -> value != null).toTable();
    }

    /**
     * Join a list of Ktables.
     *
     * @param streams List<KTable<String, InputMessageModel>>
     * @return KTable<String, MessageModel>
     */
    public static KTable<String, MessageModel> joinMultipleStreams(List<KTable<String, InputMessageModel>> streams) {
        MessageModel message = new MessageModel();
        KTable<String, MessageModel> joinedStream = null;
        int i = 0;
        for (KTable<String, InputMessageModel> stream  : streams) {
            if (i == 0){
                joinedStream = stream.mapValues(value -> {
                    message.putMessage(value.getTopic(), value);
                    return message;
                });
            } else {
                if ("outer".equals(Values.JOIN_STRATEGY)) {
                    joinedStream = joinedStream.outerJoin(stream, (leftValue, rightValue) -> {
                                if (rightValue != null) {
                                    message.putMessage(rightValue.getTopic(), rightValue);
                                }
                                return message;
                            }
                    );
                } else {
                    joinedStream = joinedStream.join(stream, (leftValue, rightValue) -> {
                                if (rightValue != null) {
                                    message.putMessage(rightValue.getTopic(), rightValue);
                                }
                                return message;
                            }
                    );
                }
            }
            i++;
        }
        return joinedStream;
    }
}
