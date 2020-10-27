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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.infai.ses.senergy.models.MessageModel;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StreamBuilder extends BaseBuilder {

    private Integer seconds = Values.WINDOW_TIME;

    public <T>KStream<String, T> filterBy(KStream<String, T> inputStream, String[] filterValues) {
        return inputStream.filter((key, value) -> {
            Boolean test = filterId(filterValues, value);
            return test;
        });
    }

    public KStream<String, MessageModel> joinMultipleStreams(Map<String, KStream<String, Object>> streams) {
        return joinMultipleStreams(streams, seconds);
    }

    public KStream<String, MessageModel> joinMultipleStreams(Map<String, KStream<String, Object>> streams, int seconds) {
        MessageModel message = new MessageModel();
        KStream<String, MessageModel> joinedStream = null;
        int i = 0;
        for (Map.Entry<String,KStream<String, Object>> stream  : streams.entrySet()) {
            if (i == 0){
                joinedStream = stream.getValue().flatMap((key, value) -> {
                    message.putMessage(stream.getKey(), value);
                    List<KeyValue<String, MessageModel>> result = new LinkedList<>();
                    result.add(KeyValue.pair(key, message));
                    return result;
                });
            } else {
                joinedStream = joinedStream.join(stream.getValue(), (leftValue, rightValue) ->
                        {
                            message.putMessage(stream.getKey(), rightValue);
                            return message;
                        }, JoinWindows.of(Duration.ofSeconds(seconds)));
            }
            i++;
        }
        return joinedStream;
    }

    public void setWindowTime(Integer seconds) {
        this.seconds = seconds;
    }
}
