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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.infai.ses.senergy.models.InputMessageModel;
import org.infai.ses.senergy.models.MessageModel;
import org.infai.ses.senergy.serialization.JSONSerdes;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

public class StreamBuilder{

    private StreamBuilder() {
        throw new IllegalStateException("Utility class");
    }

    private static Integer seconds = Values.WINDOW_TIME;

    public static  <T>KStream<String, T> filterBy(KStream<String, T> inputStream, String[] filterValues) {
        return inputStream.filter((key, value) -> BaseBuilder.filterId(filterValues, value));
    }

    public static KStream<String, MessageModel> joinMultipleStreams(List <KStream<String, InputMessageModel>> streams) {
        return joinMultipleStreams(streams, seconds);
    }

    public static KStream<String, MessageModel> joinMultipleStreams(List <KStream<String, InputMessageModel>> streams, int seconds) {
        MessageModel message = new MessageModel();
        KStream<String, MessageModel> joinedStream = null;
        int i = 0;
        for (KStream<String, InputMessageModel> stream  : streams) {
            if (i == 0){
                joinedStream = stream.flatMap((key, value) -> {
                    message.putMessage(value.getTopic(), value);
                    List<KeyValue<String, MessageModel>> result = new LinkedList<>();
                    result.add(KeyValue.pair(key, message));
                    return result;
                });
            } else {
                joinedStream = joinedStream.join(stream, (leftValue, rightValue) -> {
                    message.putMessage(rightValue.getTopic(), rightValue);
                    return message;
                    }, JoinWindows.of(Duration.ofSeconds(seconds)),
                        StreamJoined.with(Serdes.String(), JSONSerdes.Message(), JSONSerdes.InputMessage())
                );
            }
            i++;
        }
        return joinedStream;
    }
}
